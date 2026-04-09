"""
UAIMC Ambient Knowledge Layer — "The Curator" v1.0 (KnowledgeSentinel)
=====================================================================
Predictive context enrichment for AI agents.

Automatically injects relevant UAIMC knowledge into agent context
without agents explicitly querying. Uses a 4-tier pipeline:

  Tier 0: Keyword extraction from agent's recent activity (uaimc_anno)
  Tier 1: GPU-AM tensor match (CUDA, free)
  Tier 2: FTS5 cross-reference enrichment (free)
  Tier 3: Haiku fast-ranking for low-confidence results (paid, ~$0.001/call)

Tiers 0-2 are local and free. Tier 3 is budget-capped and rare (~5% of calls).

v1.0 Features:
  - 4-tier ambient pipeline with quality filtering
  - Keyword cache (1hr TTL) for fast repeat queries
  - Haiku Tier 3 escalation with budget enforcement
  - WebSocket ambient push on file-write triggers
  - Multi-agent cross-pollination (inject other agents' relevant work)
  - Dashboard metrics (cost, tier distribution, cache hit rate, per-agent stats)
  - MCP tools: uaimc_ambient, uaimc_ambient_metrics, uaimc_cross_pollinate

PRIVATE -- Not for publication
COPILOT_VSCODE (Team Brain) | March 17, 2026 (v1.0: March 18, 2026)
"""

import json
import hashlib
import logging
import math
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import uaimc_anno

logger = logging.getLogger("uaimc.ambient")

# ── Try to load ContextCompressor (optional, zero-dep) ──────────────────────
_compressor = None
try:
    import sys
    _cc_path = Path(r"C:\Users\logan\OneDrive\Documents\AutoProjects\ContextCompressor")
    if _cc_path.exists():
        sys.path.insert(0, str(_cc_path))
        from contextcompressor import ContextCompressor
        _compressor = ContextCompressor()
        logger.info("ContextCompressor loaded — ambient results will be compressed")
    else:
        logger.info("ContextCompressor not found — ambient results served uncompressed")
except Exception as e:
    logger.warning(f"ContextCompressor import failed (non-fatal): {e}")


# ── Result Quality Filters (Beta) ───────────────────────────────────────────
# Patterns that indicate garbage/non-useful content in ambient results

_BASE64_RE = re.compile(r"(?:[A-Za-z0-9+/]{4}){10,}={0,2}")  # Base64 blocks (4-char groups, optional padding)
_BINARY_RE = re.compile(r"[\x00-\x08\x0e-\x1f]")  # Non-printable control chars
_NOTEBOOK_CELL_RE = re.compile(
    r"(cell_type|execution_count|outputs|metadata)\s*[\":]", re.IGNORECASE
)
_JSON_BLOB_RE = re.compile(r'^\s*\{\s*"[^"]+"\s*:')  # Starts with {"key": — raw JSON
_MIN_USEFUL_LENGTH = 20  # Results shorter than this are likely noise

# B3-Step7: Enhanced patterns — license headers, import blocks, config fragments
_LICENSE_RE = re.compile(
    r"(Licensed under the|Apache License|MIT License|BSD License|GNU General Public|"
    r"Permission is hereby granted|SPDX-License-Identifier|Copyright \(c\)\s*\d{4}|"
    r"All rights reserved\.?|THIS SOFTWARE IS PROVIDED)",
    re.IGNORECASE,
)
_IMPORT_BLOCK_RE = re.compile(
    r"^(\s*(import\s+\w+|from\s+\w+\s+import|#include\s*[<\"]|using\s+\w+|require\s*[\(\'])\s*\n?){3,}",
    re.MULTILINE,
)
_CONFIG_FRAGMENT_RE = re.compile(
    r"^\s*[\w.-]+\s*[:=]\s*[\w\"'{}\[\]]+\s*$",
    re.MULTILINE,
)

# Quality filter: if >50% of summary chars are non-alphanumeric, likely garbage
_GARBAGE_RATIO_THRESHOLD = 0.50
_MIN_INFORMATION_DENSITY = 0.30  # B3-Step7: unique words / total words threshold


def _is_garbage_content(text: str) -> bool:
    """Return True if text is garbage (base64, binary, notebook cells, too short,
    license headers, import blocks, config fragments, low information density)."""
    if not text or len(text.strip()) < _MIN_USEFUL_LENGTH:
        return True
    if _BASE64_RE.search(text):
        return True
    if _BINARY_RE.search(text):
        return True
    # Check for raw JSON blobs (starts with {"key": — structured data, not prose)
    if _JSON_BLOB_RE.match(text):
        return True
    # Check for raw notebook JSON fragments
    if _NOTEBOOK_CELL_RE.search(text) and text.count("{") > 1:
        return True
    # B3-Step7: License headers (Apache 2.0, MIT, BSD, etc.)
    if _LICENSE_RE.search(text):
        return True
    # B3-Step7: Import-only blocks (3+ consecutive import lines with little else)
    stripped = text.strip()
    if _IMPORT_BLOCK_RE.search(stripped):
        import_lines = sum(1 for line in stripped.splitlines()
                          if re.match(r"\s*(import |from \w+ import |#include |using |require)", line))
        total_lines = max(len(stripped.splitlines()), 1)
        if import_lines / total_lines > 0.6:
            return True
    # B3-Step7: Config-only fragments (majority of lines are key=value or key: value)
    lines = stripped.splitlines()
    if len(lines) >= 3:
        config_lines = sum(1 for line in lines if _CONFIG_FRAGMENT_RE.match(line))
        if config_lines / len(lines) > 0.7:
            return True
    # B3-Step7: Information density — too many repeated words = boilerplate
    words = stripped.lower().split()
    if len(words) >= 8:
        unique = len(set(words))
        density = unique / len(words)
        if density < _MIN_INFORMATION_DENSITY:
            return True
    # Ratio check: too many non-alphanumeric chars
    alnum = sum(1 for c in text if c.isalnum() or c.isspace())
    if len(text) > 0 and alnum / len(text) < (1 - _GARBAGE_RATIO_THRESHOLD):
        return True
    return False


# ── Keyword Cache (Beta — 1hr TTL) ──────────────────────────────────────────

_keyword_cache: dict[str, tuple[list[str], float]] = {}  # cache_key -> (keywords, timestamp)
_KEYWORD_CACHE_TTL = 3600.0  # 1 hour


def _cache_key(agent: str, topic: str = "") -> str:
    """Build cache key: agent alone, or agent:topic_hash for topic-aware caching."""
    if topic and topic.strip():
        topic_hash = hashlib.md5(topic.strip().encode()).hexdigest()[:8]
        return f"{agent}:{topic_hash}"
    return agent


def _get_cached_keywords(agent: str, topic: str = "") -> Optional[list[str]]:
    """Return cached keywords for agent+topic if still fresh, else None."""
    key = _cache_key(agent, topic)
    entry = _keyword_cache.get(key)
    if entry and (time.time() - entry[1]) < _KEYWORD_CACHE_TTL:
        logger.debug(f"Ambient keyword cache HIT for {key}")
        return entry[0]
    return None


def _set_cached_keywords(agent: str, keywords: list[str], topic: str = ""):
    """Cache keywords for agent+topic with current timestamp."""
    key = _cache_key(agent, topic)
    _keyword_cache[key] = (keywords, time.time())


def invalidate_keyword_cache(agent: str = ""):
    """Invalidate keyword cache for an agent (or all agents if empty).

    Clears all topic variants for the agent (agent, agent:hash1, agent:hash2, etc).
    """
    if agent:
        keys_to_remove = [k for k in _keyword_cache if k == agent or k.startswith(f"{agent}:")]
        for k in keys_to_remove:
            del _keyword_cache[k]
    else:
        _keyword_cache.clear()


# ── Haiku Tier 3 Client (Beta) ──────────────────────────────────────────────

_haiku_available = False
_anthropic_client = None
_haiku_semaphore = threading.Semaphore(3)  # Max 3 concurrent Haiku API calls

try:
    import anthropic
    _anthropic_client = anthropic.Anthropic()
    _haiku_available = True
    logger.info("Anthropic SDK loaded — Tier 3 Haiku escalation available")
except ImportError:
    logger.info("Anthropic SDK not installed — Tier 3 disabled (Tiers 0-2 only)")
except Exception as e:
    logger.warning(f"Anthropic client init failed (non-fatal): {e}")


# ── Budget Tracking (Beta) ──────────────────────────────────────────────────

_budget_tracker: dict[str, float] = {"daily_spent": 0.0, "monthly_spent": 0.0}
_budget_day: str = ""
_budget_month: str = ""


# ── Configuration ────────────────────────────────────────────────────────────

@dataclass
class AmbientConfig:
    """Ambient knowledge layer settings."""
    enabled: bool = True
    default_token_budget: int = 500       # Max chars for ambient section
    max_token_budget: int = 1500
    confidence_threshold: float = 0.3     # Min relevance score to inject
    max_results: int = 5                  # Max ambient items to inject
    recent_activity_lookback: int = 5     # How many recent activities to analyze
    compress_results: bool = True         # Use ContextCompressor if available
    feedback_loop_prevention: bool = True # Tag ambient output to prevent loops
    # Beta additions
    escalation_threshold: float = 0.6    # Below this, escalate to Haiku Tier 3
    haiku_model: str = "claude-3-haiku-20240307"  # Haiku model for fast ranking
    haiku_max_candidates: int = 20       # Max results to send to Haiku
    haiku_daily_cap: int = 100           # Max Haiku calls per day
    haiku_monthly_budget_usd: float = 5.0  # Monthly Haiku spend cap
    haiku_cost_per_call: float = 0.001   # Estimated cost per Haiku call
    cache_keywords: bool = True          # Cache extracted keywords per agent
    quality_filter: bool = True          # Filter garbage results (base64, binary, etc.)
    topic_relevance_boost: float = 1.3   # B2-Step4: boost factor for topic keyword coverage


def load_ambient_config(config: dict) -> AmbientConfig:
    """Load ambient config from the main config dict."""
    ac = config.get("ambient", {})
    return AmbientConfig(
        enabled=ac.get("enabled", True),
        default_token_budget=ac.get("default_token_budget", 500),
        max_token_budget=ac.get("max_token_budget", 1500),
        confidence_threshold=ac.get("confidence_threshold", 0.3),
        max_results=ac.get("max_results", 5),
        recent_activity_lookback=ac.get("recent_activity_lookback", 5),
        compress_results=ac.get("compress_results", True),
        feedback_loop_prevention=ac.get("feedback_loop_prevention", True),
        escalation_threshold=ac.get("escalation_threshold", 0.6),
        haiku_model=ac.get("haiku_model", "claude-3-haiku-20240307"),
        haiku_max_candidates=ac.get("haiku_max_candidates", 20),
        haiku_daily_cap=ac.get("haiku_daily_cap", 100),
        haiku_monthly_budget_usd=ac.get("haiku_monthly_budget_usd", 5.0),
        haiku_cost_per_call=ac.get("haiku_cost_per_call", 0.001),
        cache_keywords=ac.get("cache_keywords", True),
        quality_filter=ac.get("quality_filter", True),
        topic_relevance_boost=ac.get("topic_relevance_boost", 1.3),
    )


# ── Schema ───────────────────────────────────────────────────────────────────

AMBIENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS ambient_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    agent             TEXT NOT NULL,
    trigger_type      TEXT NOT NULL,
    keywords_extracted TEXT,
    tier_reached      INTEGER,
    results_count     INTEGER,
    top_confidence    REAL,
    tokens_injected   INTEGER,
    compressed        INTEGER DEFAULT 0,
    compression_ratio REAL DEFAULT 1.0,
    latency_ms        REAL,
    created_at        TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_ambient_agent ON ambient_log(agent, created_at);
"""


def ensure_schema(db: sqlite3.Connection):
    """Create ambient_log table if it doesn't exist."""
    db.executescript(AMBIENT_SCHEMA)
    db.commit()


# ── Core Pipeline ────────────────────────────────────────────────────────────

def extract_agent_keywords(db: sqlite3.Connection, agent: str,
                           lookback: int = 5) -> list[str]:
    """Tier 0: Extract keywords from an agent's recent activity.

    FAST path — uses only indexed agent_activity table (no JOINs).
    Extracts keywords from recent activity details via uaimc_anno.

    Returns deduplicated keyword list sorted by relevance.
    """
    keywords: dict[str, float] = {}
    agent_lower = agent.lower().strip()
    agent_upper = agent.upper().strip()

    t0 = time.perf_counter()

    # Pull recent activity details (indexed, fast — no JOINs)
    try:
        rows = db.execute(
            "SELECT details FROM agent_activity WHERE agent_name = ? "
            "ORDER BY timestamp DESC LIMIT ?",
            (agent_upper, lookback)
        ).fetchall()
        if not rows:
            rows = db.execute(
                "SELECT details FROM agent_activity WHERE agent_name = ? "
                "ORDER BY timestamp DESC LIMIT ?",
                (agent_lower, lookback)
            ).fetchall()
        for row in rows:
            details = row[0] if row[0] else ""
            # Extract meaningful tokens from activity details
            annos = uaimc_anno.annotate(details, source="activity")
            for a in annos:
                if a.token not in keywords or a.weight > keywords[a.token]:
                    keywords[a.token] = a.weight
    except Exception as e:
        logger.debug(f"Agent activity extraction: {e}")

    # Filter out the agent's own name and overly generic terms
    keywords.pop(agent_lower, None)
    keywords.pop(agent_upper, None)

    elapsed = (time.perf_counter() - t0) * 1000
    logger.debug(f"Ambient Tier 0 keywords: {len(keywords)} in {elapsed:.0f}ms")

    # Sort by weight descending, return top N
    sorted_kw = sorted(keywords.items(), key=lambda x: x[1], reverse=True)
    return [kw for kw, _ in sorted_kw[:15]]  # Cap at 15 keywords


def gpu_am_match(memory, keywords: list[str], limit: int = 10) -> list[dict]:
    """Tier 1+2: GPU-AM tensor match for extracted keywords.

    Uses the existing GPU acceleration layer to find relevant summaries.
    Sends only top 5 keywords to keep query fast.
    Returns raw query results sorted by relevance score.
    """
    if not keywords:
        return []
    # Cap keywords to top 5 for speed (highest weight already sorted)
    top_keywords = keywords[:5]
    t0 = time.perf_counter()
    results = memory.query(top_keywords, limit=limit)
    elapsed = (time.perf_counter() - t0) * 1000
    logger.debug(f"Ambient Tier 1+2 GPU-AM: {len(results)} results in {elapsed:.0f}ms")
    return results


def compress_ambient_text(text: str, query_keywords: str,
                          max_chars: int) -> tuple[str, float]:
    """Compress ambient text using ContextCompressor if available.

    Returns (compressed_text, compression_ratio).
    If compressor unavailable, returns input truncated to max_chars.
    """
    if _compressor and len(text) > max_chars:
        try:
            compressed, result = _compressor.compress_text(
                text,
                query=query_keywords,
                method="relevant"
            )
            # Ensure we're within budget
            if len(compressed) > max_chars:
                compressed = compressed[:max_chars]
            ratio = result.compression_ratio
            logger.debug(
                f"Compressed ambient: {result.original_size} → {result.compressed_size} "
                f"({ratio:.1%}), saved ~{result.estimated_token_savings} tokens"
            )
            return compressed, ratio
        except Exception as e:
            logger.debug(f"ContextCompressor failed (non-fatal): {e}")

    # Fallback: simple truncation
    if len(text) > max_chars:
        return text[:max_chars], max_chars / len(text) if len(text) > 0 else 1.0
    return text, 1.0


def build_ambient_section(results: list[dict], agent: str,
                          keywords: list[str], config: AmbientConfig) -> tuple[str, float]:
    """Build the ambient knowledge markdown section from query results.

    Filters by confidence threshold and quality, compresses if enabled,
    respects token budget.
    """
    if not results:
        return "", 1.0

    # Filter by confidence threshold
    filtered = [
        r for r in results
        if r.get("relevance_score", 0) >= config.confidence_threshold
    ]

    if not filtered:
        return "", 1.0

    # Take top N results (quality filter already applied upstream in enrich_context)
    filtered = filtered[:config.max_results]

    # Build raw section
    lines = []
    for r in filtered:
        source = r.get("source", "unknown").upper()
        author = r.get("author", "")
        author_str = f" [{author}]" if author else ""
        date = r.get("created_at", "")[:10]
        summary = r.get("summary", "")

        # Truncate individual summaries to keep things tight
        if len(summary) > 300:
            summary = summary[:297] + "..."

        lines.append(f"- [{source}]{author_str} ({date}) {summary}")

    raw_text = "\n".join(lines)
    query_str = " ".join(keywords[:5])

    # Compress if enabled and compressor available
    if config.compress_results and _compressor:
        compressed, ratio = compress_ambient_text(
            raw_text, query_str, config.default_token_budget
        )
        return compressed, ratio
    else:
        # Simple truncation to budget
        if len(raw_text) > config.default_token_budget:
            raw_text = raw_text[:config.default_token_budget]
        return raw_text, 1.0


def log_ambient(db: sqlite3.Connection, agent: str, trigger: str,
                keywords: list[str], tier: int, results_count: int,
                top_confidence: float, tokens_injected: int,
                compressed: bool, compression_ratio: float,
                latency_ms: float):
    """Log ambient injection for tracking and cost analysis."""
    try:
        db.execute(
            "INSERT INTO ambient_log (agent, trigger_type, keywords_extracted, "
            "tier_reached, results_count, top_confidence, tokens_injected, "
            "compressed, compression_ratio, latency_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                agent, trigger, json.dumps(keywords[:10]),
                tier, results_count, round(top_confidence, 4),
                tokens_injected, 1 if compressed else 0,
                round(compression_ratio, 4), round(latency_ms, 2)
            )
        )
        db.commit()
    except Exception as e:
        logger.debug(f"ambient log write failed: {e}")


# ── Main Entry Point ────────────────────────────────────────────────────────

def _check_budget(config: AmbientConfig) -> bool:
    """Check if we're within Haiku budget limits. Returns True if OK to spend."""
    global _budget_tracker, _budget_day, _budget_month

    today = time.strftime("%Y-%m-%d")
    month = time.strftime("%Y-%m")

    # Reset daily counter if new day
    if _budget_day != today:
        _budget_tracker["daily_spent"] = 0.0
        _budget_day = today

    # Reset monthly counter if new month
    if _budget_month != month:
        _budget_tracker["monthly_spent"] = 0.0
        _budget_month = month

    # Check daily call cap
    daily_calls = _budget_tracker["daily_spent"] / max(config.haiku_cost_per_call, 0.0001)
    if daily_calls >= config.haiku_daily_cap:
        logger.debug("Ambient Haiku: daily call cap reached")
        return False

    # Check monthly budget
    if _budget_tracker["monthly_spent"] >= config.haiku_monthly_budget_usd:
        logger.debug("Ambient Haiku: monthly budget exhausted")
        return False

    return True


def _record_spend(config: AmbientConfig):
    """Record a Haiku API call spend."""
    _budget_tracker["daily_spent"] += config.haiku_cost_per_call
    _budget_tracker["monthly_spent"] += config.haiku_cost_per_call


def haiku_rank_results(results: list[dict], keywords: list[str],
                       agent: str, config: AmbientConfig) -> list[dict]:
    """Tier 3: Use Claude Haiku to semantically rank ambient candidates.

    Only called when local confidence is below escalation_threshold.
    Budget-capped: checks daily/monthly limits before calling.

    Returns re-ordered results with Haiku confidence scores.
    """
    if not _haiku_available or not _anthropic_client:
        return results

    if not _check_budget(config):
        return results

    if not _haiku_semaphore.acquire(timeout=2):
        logger.debug("Haiku ranking skipped — semaphore busy")
        return results

    try:
        return _haiku_rank_results_inner(results, keywords, agent, config)
    finally:
        _haiku_semaphore.release()


def _haiku_rank_results_inner(results, keywords, agent, config):
    """Inner Haiku ranking — called under semaphore."""
    # Build ranking prompt
    intent = " ".join(keywords[:7])
    candidates = []
    for i, r in enumerate(results[:config.haiku_max_candidates]):
        summary = r.get("summary", "")[:200]
        source = r.get("source", "unknown")
        candidates.append(f"{i+1}. [{source}] {summary}")

    if not candidates:
        return results

    candidates_text = "\n".join(candidates)
    prompt = (
        f"Agent {agent} is working on: {intent}\n\n"
        f"Rank these {len(candidates)} knowledge items by relevance to the agent's work. "
        f"Return ONLY a JSON array of objects with 'index' (1-based) and 'score' (0.0-1.0). "
        f"Example: [{{'index': 3, 'score': 0.95}}, {{'index': 1, 'score': 0.7}}]\n\n"
        f"Items:\n{candidates_text}"
    )

    try:
        t0 = time.perf_counter()
        response = _anthropic_client.messages.create(
            model=config.haiku_model,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
            timeout=15.0,  # Hard cap — ambient calls must be fast
        )
        elapsed = (time.perf_counter() - t0) * 1000
        _record_spend(config)
        logger.info(f"Ambient Tier 3 Haiku: ranked {len(candidates)} items in {elapsed:.0f}ms")

        # Parse response
        text = response.content[0].text.strip()
        # Extract JSON array from response (Haiku may wrap in markdown)
        json_match = re.search(r"\[.*\]", text, re.DOTALL)
        if not json_match:
            logger.debug("Haiku response did not contain valid JSON array")
            return results

        rankings = json.loads(json_match.group())

        # Re-order results by Haiku score
        scored = {}
        for rank in rankings:
            idx = rank.get("index", 0) - 1  # Convert to 0-based
            score = rank.get("score", 0.0)
            if 0 <= idx < len(results):
                scored[idx] = score

        # Sort by Haiku score descending, inject score into result
        ranked = []
        for idx in sorted(scored, key=scored.get, reverse=True):
            result = dict(results[idx])
            result["haiku_score"] = scored[idx]
            result["relevance_score"] = max(
                result.get("relevance_score", 0),
                scored[idx]
            )
            ranked.append(result)

        # Add any unranked results at the end
        ranked_indices = set(scored.keys())
        for i, r in enumerate(results):
            if i not in ranked_indices:
                ranked.append(r)

        return ranked

    except json.JSONDecodeError:
        logger.debug("Haiku response JSON parse failed — using local ranking")
        return results
    except Exception as e:
        logger.warning(f"Haiku ranking failed (non-fatal): {e}")
        return results


def detect_contradictions(results: list[dict], keywords: list[str],
                          agent: str, config: AmbientConfig) -> list[dict]:
    """Detect contradictory claims among ambient results using Haiku.

    Called when multiple results exist. Identifies conflicting information
    so the agent can make informed decisions rather than blindly trusting
    contradictory knowledge.

    Returns list of contradiction dicts: [{"items": [1,3], "explanation": "..."}]
    """
    if not _haiku_available or not _anthropic_client:
        return []

    if len(results) < 2:
        return []

    if not _check_budget(config):
        return []

    if not _haiku_semaphore.acquire(timeout=2):
        logger.debug("Contradiction detection skipped — semaphore busy")
        return []

    try:
        return _detect_contradictions_inner(results, keywords, agent, config)
    finally:
        _haiku_semaphore.release()


def _detect_contradictions_inner(results, keywords, agent, config):
    """Inner contradiction detection — called under semaphore."""
    intent = " ".join(keywords[:7])
    candidates = []
    for i, r in enumerate(results[:config.haiku_max_candidates]):
        summary = r.get("summary", "")[:200]
        source = r.get("source", "unknown")
        candidates.append(f"{i+1}. [{source}] {summary}")

    if len(candidates) < 2:
        return []

    candidates_text = "\n".join(candidates)
    prompt = (
        f"Agent {agent} is working on: {intent}\n\n"
        f"Review these {len(candidates)} knowledge items for CONTRADICTIONS. "
        f"Two items contradict if they make directly opposing claims about the same topic. "
        f"Return ONLY a JSON array. If no contradictions, return []. "
        f"If found, return objects with 'items' (1-based indices) and 'explanation' (brief). "
        f"Example: [{{\"items\": [2, 5], \"explanation\": \"Item 2 says X is deprecated, item 5 recommends using X\"}}]\n\n"
        f"Items:\n{candidates_text}"
    )

    try:
        t0 = time.perf_counter()
        response = _anthropic_client.messages.create(
            model=config.haiku_model,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
            timeout=15.0,
        )
        elapsed = (time.perf_counter() - t0) * 1000
        _record_spend(config)
        logger.info(f"Ambient contradiction check: {len(candidates)} items in {elapsed:.0f}ms")

        text = response.content[0].text.strip()
        json_match = re.search(r"\[.*\]", text, re.DOTALL)
        if not json_match:
            return []

        contradictions = json.loads(json_match.group())
        if not isinstance(contradictions, list):
            return []

        valid = []
        for c in contradictions:
            if not isinstance(c, dict):
                continue
            items = c.get("items", [])
            explanation = c.get("explanation", "")
            if (isinstance(items, list) and len(items) >= 2
                    and explanation
                    and all(isinstance(i, int) and 1 <= i <= len(results) for i in items)):
                valid.append({"items": items, "explanation": str(explanation)[:300]})

        if valid:
            logger.info(f"Ambient[{agent}]: {len(valid)} contradiction(s) detected")

        return valid

    except json.JSONDecodeError:
        logger.debug("Contradiction detection JSON parse failed")
        return []
    except Exception as e:
        logger.warning(f"Contradiction detection failed (non-fatal): {e}")
        return []


def enrich_context(memory, agent: str, config: AmbientConfig,
                   trigger: str = "context_request", topic: str = "") -> str:
    """Main ambient pipeline: extract → match → [escalate] → compress → format.

    Called from the /context endpoint. Returns a formatted markdown section
    to append to the agent's context, or empty string if nothing relevant.

    Beta "The Advisor" — Tier 0-2 free + Tier 3 Haiku escalation (budget-capped).

    Pipeline:
      Tier 0: Keyword extraction (free, <10ms, cached 1hr)
      Tier 1+2: GPU-AM + FTS5 match (free, <50ms)
      Quality Filter: Remove garbage (base64, binary, notebook cells)
      Tier 3: Haiku fast-ranking (if top_confidence < escalation_threshold)
      Compress: ContextCompressor (82% compression)
      Format: Markdown section for injection

    Args:
        memory: UnifiedMemory instance
        agent: Agent name requesting context
        config: AmbientConfig with thresholds and budgets
        trigger: What triggered this enrichment

    Returns:
        Formatted string to append to context (may be empty).
    """
    if not config.enabled or not agent:
        return ""

    t0 = time.perf_counter()

    # === Tier 0: Keyword Extraction (free, <10ms, cached 1hr) ===
    keywords = None
    # B2-Step4: Always extract topic keywords separately for topic-aware scoring
    topic_kw: list[str] = []
    if topic and topic.strip():
        _topic_annos = uaimc_anno.annotate(topic.strip(), source="topic")
        topic_kw = [a.token for a in sorted(_topic_annos, key=lambda x: -x.weight)[:7]]

    if config.cache_keywords:
        keywords = _get_cached_keywords(agent, topic)

    if not keywords:
        activity_keywords = extract_agent_keywords(
            memory.db, agent, lookback=config.recent_activity_lookback
        )

        # B-002: Blend topic keywords with activity keywords
        if topic_kw:
            # Deduplicated merge: topic keywords first, then activity keywords
            seen = set()
            keywords = []
            for kw in topic_kw + (activity_keywords or []):
                if kw.lower() not in seen:
                    seen.add(kw.lower())
                    keywords.append(kw)
            keywords = keywords[:15]  # Same cap as activity-only path
        else:
            keywords = activity_keywords

        if keywords and config.cache_keywords:
            _set_cached_keywords(agent, keywords, topic)

    if not keywords:
        logger.debug(f"Ambient[{agent}]: no keywords extracted, skipping")
        return ""

    tier_reached = 0
    logger.debug(f"Ambient[{agent}]: Tier 0 extracted {len(keywords)} keywords: {keywords[:5]}")

    # === Tier 1+2: GPU-AM Match + FTS5 Cross-Reference (free, <50ms) ===
    results = gpu_am_match(memory, keywords, limit=config.max_results * 4)
    tier_reached = 2 if results else 0

    # === B2-Step4: Topic-aware scoring — boost results matching topic keywords ===
    if topic_kw and results:
        topic_set = {kw.lower() for kw in topic_kw}
        for r in results:
            summary_lower = r.get("summary", "").lower()
            matched_topic = sum(1 for tkw in topic_set if tkw in summary_lower)
            coverage = matched_topic / len(topic_set)
            boost = 1.0 + coverage * config.topic_relevance_boost
            r["relevance_score"] = r.get("relevance_score", 0) * boost
            r["topic_coverage"] = round(coverage, 3)
            r["topic_boost"] = round(boost, 3)

    if not results:
        logger.debug(f"Ambient[{agent}]: no matches found")
        elapsed = (time.perf_counter() - t0) * 1000
        log_ambient(memory.db, agent, trigger, keywords, tier_reached,
                    0, 0.0, 0, False, 1.0, elapsed)
        return ""

    # === Quality Filter (Beta): remove garbage before confidence check ===
    if config.quality_filter:
        before = len(results)
        results = [r for r in results if not _is_garbage_content(r.get("summary", ""))]
        filtered_out = before - len(results)
        if filtered_out:
            logger.debug(f"Ambient[{agent}]: quality filter removed {filtered_out}/{before} results")

    if not results:
        elapsed = (time.perf_counter() - t0) * 1000
        log_ambient(memory.db, agent, trigger, keywords, tier_reached,
                    0, 0.0, 0, False, 1.0, elapsed)
        return ""

    # === Tier 3: Haiku Escalation (if local confidence is low) ===
    top_confidence = max(r.get("relevance_score", 0) for r in results)

    if top_confidence < config.escalation_threshold and _haiku_available:
        logger.debug(
            f"Ambient[{agent}]: top_conf={top_confidence:.2f} < "
            f"threshold={config.escalation_threshold}, escalating to Haiku"
        )
        results = haiku_rank_results(results, keywords, agent, config)
        tier_reached = 3
        # Recalculate top confidence after Haiku re-ranking
        top_confidence = max(r.get("relevance_score", 0) for r in results)

    # === RC: Contradiction Detection (Haiku escalation) ===
    contradictions = detect_contradictions(results, keywords, agent, config)

    # === Build & Compress Section ===
    section_text, compression_ratio = build_ambient_section(
        results, agent, keywords, config
    )

    if not section_text:
        elapsed = (time.perf_counter() - t0) * 1000
        log_ambient(memory.db, agent, trigger, keywords, tier_reached,
                    0, 0.0, 0, False, 1.0, elapsed)
        return ""

    was_compressed = compression_ratio < 0.95

    # === Format Final Section ===
    kw_display = ", ".join(keywords[:5])
    tier_label = f"Tier {tier_reached}"
    header = f"\n--- AMBIENT KNOWLEDGE (The Curator) [{len(results)} signals | {tier_label} | {kw_display}] ---"

    # === RC: Cross-Pollination ===
    xpoll_items = cross_pollinate(memory.db, agent, config)
    xpoll_section = ""
    if xpoll_items:
        xpoll_lines = []
        for xp in xpoll_items:
            from_agent = xp.get("from_agent", "?")
            summary = xp.get("summary", "")[:200]
            xpoll_lines.append(f"- [FROM {from_agent}] {summary}")
        xpoll_section = "\n[Cross-Team Intel]\n" + "\n".join(xpoll_lines)

    # === RC: Contradiction Alerts ===
    contradiction_section = ""
    if contradictions:
        clines = []
        for c in contradictions:
            items_str = ", ".join(f"#{i}" for i in c["items"])
            clines.append(f"- CONFLICT ({items_str}): {c['explanation']}")
        contradiction_section = "\n[Contradiction Alerts]\n" + "\n".join(clines)

    footer = "--- END AMBIENT ---\n"
    ambient_block = f"{header}\n{section_text}{xpoll_section}{contradiction_section}\n{footer}"

    elapsed = (time.perf_counter() - t0) * 1000

    # === Log ===
    log_ambient(
        memory.db, agent, trigger, keywords, tier_reached,
        len(results), top_confidence, len(ambient_block),
        was_compressed, compression_ratio, elapsed
    )

    logger.info(
        f"Ambient[{agent}]: {len(results)} results, "
        f"tier={tier_reached}, top_conf={top_confidence:.2f}, "
        f"{len(ambient_block)} chars, "
        f"{'compressed ' + f'{compression_ratio:.0%}' if was_compressed else 'raw'}, "
        f"{elapsed:.0f}ms"
    )

    return ambient_block


# ── Standalone Query (Beta) ─────────────────────────────────────────────────

def query_ambient(memory, agent: str, intent: str, config: AmbientConfig,
                  max_results: int = 5) -> dict:
    """Standalone ambient query — used by /ambient endpoint and MCP tool.

    Unlike enrich_context (which hooks into /context), this returns
    structured JSON results for direct use.

    Args:
        memory: UnifiedMemory instance
        agent: Agent name
        intent: What the agent is working on (used as keywords)
        config: AmbientConfig
        max_results: Max results to return

    Returns:
        Dict with 'results', 'keywords', 'tier', 'latency_ms', etc.
    """
    if not config.enabled:
        return {"results": [], "error": "ambient disabled"}

    t0 = time.perf_counter()

    # Use intent as keywords (parse via annotation engine)
    annos = uaimc_anno.annotate(intent, source="ambient_query")
    keywords = sorted(
        {a.token: a.weight for a in annos}.items(),
        key=lambda x: x[1], reverse=True
    )
    keywords = [kw for kw, _ in keywords[:15]]

    if not keywords:
        return {"results": [], "keywords": [], "tier": 0, "count": 0,
                "top_confidence": 0, "latency_ms": round((time.perf_counter() - t0) * 1000, 2)}

    # Tier 1+2: GPU-AM + FTS5
    results = gpu_am_match(memory, keywords, limit=max_results * 4)
    tier = 2 if results else 0

    # Sanitize scores — GPU-AM can produce inf/nan which aren't JSON-serialisable
    for r in results:
        for k in ("relevance_score", "haiku_score", "confidence"):
            v = r.get(k)
            if isinstance(v, float) and (math.isinf(v) or math.isnan(v)):
                r[k] = 0.0

    # Quality filter
    if config.quality_filter:
        results = [r for r in results if not _is_garbage_content(r.get("summary", ""))]

    # Tier 3: Haiku escalation if needed
    top_conf = max((r.get("relevance_score", 0) for r in results), default=0)
    if top_conf < config.escalation_threshold and _haiku_available and results:
        results = haiku_rank_results(results, keywords, agent, config)
        tier = 3
        top_conf = max((r.get("relevance_score", 0) for r in results), default=0)

    # Contradiction detection
    contradictions = detect_contradictions(results, keywords, agent, config)

    # Trim to requested limit
    results = results[:max_results]

    elapsed = (time.perf_counter() - t0) * 1000

    # Format results
    items = []
    for r in results:
        items.append({
            "summary": r.get("summary", "")[:500],
            "source": r.get("source", "unknown"),
            "author": r.get("author", ""),
            "confidence": round(r.get("relevance_score", 0), 4),
            "haiku_score": round(r.get("haiku_score", 0), 4) if "haiku_score" in r else None,
            "created_at": r.get("created_at", ""),
        })

    log_ambient(memory.db, agent, "standalone_query", keywords, tier,
                len(items), top_conf, 0, False, 1.0, elapsed)

    return {
        "agent": agent,
        "intent": intent,
        "keywords": keywords[:10],
        "results": items,
        "contradictions": contradictions,
        "count": len(items),
        "tier_reached": tier,
        "top_confidence": round(top_conf, 4),
        "latency_ms": round(elapsed, 2),
    }


# ── RC: WebSocket Ambient Push ──────────────────────────────────────────────

def build_ambient_push_payload(filepath: str, author: str,
                               memory, config: AmbientConfig) -> Optional[dict]:
    """Build an ambient push payload triggered by a file write.

    Called from the file watcher callback. Extracts keywords from the
    new/modified file content, runs the ambient pipeline, and returns
    a WebSocket-ready payload for connected agents.

    Returns None if nothing relevant to push.
    """
    if not config.enabled:
        return None

    t0 = time.perf_counter()

    # Extract keywords from the file path + author context
    context_text = f"{filepath} {author}"
    annos = uaimc_anno.annotate(context_text, source="file_write")
    keywords = sorted(
        {a.token: a.weight for a in annos}.items(),
        key=lambda x: x[1], reverse=True
    )
    keywords = [kw for kw, _ in keywords[:10]]

    if not keywords:
        return None

    # Run GPU-AM match to find related knowledge
    results = gpu_am_match(memory, keywords, limit=config.max_results * 2)

    # Quality filter
    if config.quality_filter:
        results = [r for r in results if not _is_garbage_content(r.get("summary", ""))]

    if not results:
        return None

    # Take top N
    results = results[:config.max_results]
    top_conf = max((r.get("relevance_score", 0) for r in results), default=0)

    elapsed = (time.perf_counter() - t0) * 1000

    items = []
    for r in results:
        items.append({
            "summary": r.get("summary", "")[:300],
            "source": r.get("source", "unknown"),
            "author": r.get("author", ""),
            "confidence": round(r.get("relevance_score", 0), 4),
        })

    log_ambient(memory.db, author or "file_watcher", "file_write_push",
                keywords, 2, len(items), top_conf, 0, False, 1.0, elapsed)

    return {
        "event": "ambient_push",
        "trigger": "file_write",
        "filepath": filepath,
        "author": author,
        "keywords": keywords[:5],
        "results": items,
        "count": len(items),
        "top_confidence": round(top_conf, 4),
        "latency_ms": round(elapsed, 2),
    }


# ── RC: Multi-Agent Cross-Pollination ───────────────────────────────────────

def cross_pollinate(db: sqlite3.Connection, agent: str,
                    config: AmbientConfig) -> list[dict]:
    """Find relevant work from OTHER agents that this agent should know about.

    Looks at what other agents have been querying/working on recently,
    then finds summaries matching those topics via annotations_fts.
    Returns items tagged with which agent was interested in each topic.

    Returns a list of cross-pollination items (max 3).
    """
    if not config.enabled:
        return []

    agent_upper = agent.upper().strip()

    # Find other agents' recent activity with their intents
    try:
        other_activity = db.execute(
            "SELECT agent_name, details FROM agent_activity "
            "WHERE agent_name != ? AND agent_name != ? "
            "AND timestamp > datetime('now', '-24 hours') "
            "AND details IS NOT NULL AND length(details) > 5 "
            "ORDER BY timestamp DESC LIMIT 20",
            (agent_upper, agent.lower().strip())
        ).fetchall()
    except Exception as e:
        logger.debug(f"Cross-pollinate agent query failed: {e}")
        return []

    if not other_activity:
        return []

    # Extract keywords from other agents' activity details
    # details format: "intent=DRGFC compression" or "topic=builds"
    other_keywords: dict[str, list[str]] = {}  # agent -> keywords
    for row in other_activity:
        agent_name = row[0]
        details = row[1] or ""
        # Parse "intent=..." or "topic=..." from details
        for part in details.split(","):
            part = part.strip()
            if "=" in part:
                val = part.split("=", 1)[1].strip()
                if val and len(val) > 2:
                    other_keywords.setdefault(agent_name, []).append(val)

    if not other_keywords:
        return []

    # Collect unique search terms from other agents (deduplicated)
    seen_terms: set[str] = set()
    search_pairs: list[tuple[str, str]] = []  # (agent_name, search_term)
    for ag, kws in other_keywords.items():
        for kw in kws:
            kw_lower = kw.lower().strip()
            if kw_lower not in seen_terms:
                seen_terms.add(kw_lower)
                search_pairs.append((ag, kw))

    items = []
    seen_ids: set[int] = set()

    for from_agent, term in search_pairs[:5]:
        if len(items) >= 3:
            break

        # Tokenize the term for FTS5 OR query
        tokens = [t.strip() for t in term.split() if len(t.strip()) > 2]
        if not tokens:
            continue
        fts_query = " OR ".join(tokens)

        try:
            rows = db.execute(
                "SELECT DISTINCT s.id, s.content, s.source, s.created_at, "
                "       SUM(a.weight) as total_weight "
                "FROM annotations_fts f "
                "JOIN annotations a ON a.id = f.rowid "
                "JOIN summaries s ON s.id = a.summary_id "
                "WHERE annotations_fts MATCH ? "
                "GROUP BY s.id "
                "ORDER BY total_weight DESC "
                "LIMIT 2",
                (fts_query,)
            ).fetchall()
        except Exception as e:
            logger.debug(f"Cross-pollinate FTS5 search for '{fts_query}' failed: {e}")
            continue

        for row in rows:
            sid = row[0]
            if sid in seen_ids:
                continue
            seen_ids.add(sid)

            summary = row[1] or ""
            if _is_garbage_content(summary):
                continue

            items.append({
                "summary": summary[:300],
                "source": row[2] or "unknown",
                "from_agent": from_agent,
                "topic": term,
                "created_at": (row[3] or "")[:10],
                "type": "cross_pollination",
            })
            if len(items) >= 3:
                break

    if items:
        logger.info(
            f"Ambient cross-pollinate[{agent}]: {len(items)} items from "
            f"{', '.join(set(i['from_agent'] for i in items))}"
        )

    return items


# ── RC: Dashboard Metrics ────────────────────────────────────────────────────

def get_ambient_metrics(db: sqlite3.Connection) -> dict:
    """Aggregate ambient usage metrics for the dashboard.

    Returns daily cost, injections per agent, tier distribution,
    cache hit rate, and latency percentiles.
    """
    metrics: dict = {
        "daily_cost_usd": 0.0,
        "monthly_cost_usd": 0.0,
        "total_injections_24h": 0,
        "tier_distribution": {},
        "per_agent": {},
        "latency_p50_ms": 0,
        "latency_p95_ms": 0,
        "cache_hit_rate": 0.0,
        "haiku_calls_today": 0,
    }

    try:
        # Daily stats
        daily = db.execute(
            "SELECT COUNT(*), AVG(latency_ms), "
            "SUM(CASE WHEN tier_reached = 3 THEN 1 ELSE 0 END) "
            "FROM ambient_log "
            "WHERE created_at > datetime('now', '-24 hours')"
        ).fetchone()

        if daily and daily[0]:
            metrics["total_injections_24h"] = daily[0]
            metrics["latency_p50_ms"] = round(daily[1] or 0, 1)
            haiku_calls = daily[2] or 0
            metrics["haiku_calls_today"] = haiku_calls
            metrics["daily_cost_usd"] = round(haiku_calls * 0.001, 4)

        # Monthly cost
        monthly = db.execute(
            "SELECT SUM(CASE WHEN tier_reached = 3 THEN 1 ELSE 0 END) "
            "FROM ambient_log "
            "WHERE created_at > datetime('now', '-30 days')"
        ).fetchone()
        if monthly and monthly[0]:
            metrics["monthly_cost_usd"] = round(monthly[0] * 0.001, 4)

        # Tier distribution
        tiers = db.execute(
            "SELECT tier_reached, COUNT(*) "
            "FROM ambient_log "
            "WHERE created_at > datetime('now', '-24 hours') "
            "GROUP BY tier_reached ORDER BY tier_reached"
        ).fetchall()
        metrics["tier_distribution"] = {
            f"tier_{row[0]}": row[1] for row in tiers
        }

        # Per-agent stats
        agents = db.execute(
            "SELECT agent, COUNT(*), AVG(latency_ms), AVG(top_confidence) "
            "FROM ambient_log "
            "WHERE created_at > datetime('now', '-24 hours') "
            "GROUP BY agent ORDER BY COUNT(*) DESC LIMIT 20"
        ).fetchall()
        for row in agents:
            metrics["per_agent"][row[0]] = {
                "injections": row[1],
                "avg_latency_ms": round(row[2] or 0, 1),
                "avg_confidence": round(row[3] or 0, 4),
            }

        # Latency percentiles (approximate via sorted values)
        latencies = db.execute(
            "SELECT latency_ms FROM ambient_log "
            "WHERE created_at > datetime('now', '-24 hours') "
            "AND latency_ms IS NOT NULL "
            "ORDER BY latency_ms"
        ).fetchall()
        if latencies:
            vals = [r[0] for r in latencies]
            n = len(vals)
            metrics["latency_p50_ms"] = round(vals[n // 2], 1)
            metrics["latency_p95_ms"] = round(vals[min(int(n * 0.95), n - 1)], 1)

        # Cache hit rate estimate (keyword cache hits vs total calls)
        # Use the in-memory cache stats
        total_agents_cached = len(_keyword_cache)
        metrics["cache_hit_rate"] = round(
            total_agents_cached / max(len(metrics["per_agent"]), 1), 2
        )
        metrics["keyword_caches_active"] = total_agents_cached

        # Budget status
        metrics["budget_daily_spent"] = round(_budget_tracker.get("daily_spent", 0), 4)
        metrics["budget_monthly_spent"] = round(_budget_tracker.get("monthly_spent", 0), 4)

    except Exception as e:
        logger.warning(f"Ambient metrics query failed: {e}")
        metrics["error"] = str(e)

    return metrics
