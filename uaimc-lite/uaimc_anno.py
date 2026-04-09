"""
UAIMC Annotation Engine v1.0
=============================
Merged from:
  - Server UMA anno_engine.py (fnv1a_hash, Annotation, annotate, make_summary, apply_tfidf)
  - BCH auto_tag.py (PROJECT_TERMS, detect_intent, extract_mentions, agent patterns)

Extracts meaningful annotations from any text for the Universal AI Memory Core.
Designed for multi-agent context: Team Brain agents, BCH messages, session logs.

PRIVATE -- Not for publication
COPILOT_VSCODE (Team Brain) | March 14, 2026
"""

import math
import json
import re
import sqlite3
import functools
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# ── Minimal config read (avoids circular import with uaimc_service) ──────────
_ANNO_CONFIG_PATH = Path(__file__).parent / "config" / "config.json"

def _load_scoring_config() -> dict:
    try:
        if _ANNO_CONFIG_PATH.exists():
            with open(_ANNO_CONFIG_PATH, "r") as f:
                return json.load(f).get("scoring", {})
    except (json.JSONDecodeError, OSError):
        pass
    return {}

SCORING = _load_scoring_config()

# ── FNV-1a Hash (fast, minimal collisions) ───────────────────────────────────
_FNV_OFFSET = 0xCBF29CE484222325
_FNV_PRIME = 0x100000001B3
_FNV_MASK = 0xFFFFFFFFFFFFFFFF


def fnv1a_hash(s: str) -> int:
    """64-bit FNV-1a hash of a lowercase string. Returns signed int for SQLite."""
    h = _FNV_OFFSET
    for byte in s.encode("utf-8"):
        h ^= byte
        h = (h * _FNV_PRIME) & _FNV_MASK
    if h >= 0x8000000000000000:
        h -= 0x10000000000000000
    return h


def simhash(tokens: list[str], hashbits: int = 64) -> int:
    """Compute SimHash fingerprint from annotation tokens.

    Similar token sets produce similar fingerprints (small Hamming distance).
    Uses fnv1a_hash per token for consistency with the annotation ecosystem.
    Returns signed int for SQLite compatibility.
    """
    if not tokens:
        return 0
    v = [0] * hashbits
    for token in tokens:
        h = fnv1a_hash(token) & _FNV_MASK  # unsigned for bit ops
        for i in range(hashbits):
            if h & (1 << i):
                v[i] += 1
            else:
                v[i] -= 1
    fingerprint = 0
    for i in range(hashbits):
        if v[i] >= 0:
            fingerprint |= (1 << i)
    # Convert to signed for SQLite
    if fingerprint >= 0x8000000000000000:
        fingerprint -= 0x10000000000000000
    return fingerprint


def hamming_distance(a: int, b: int, bits: int = 64) -> int:
    """Count differing bits between two SimHash fingerprints."""
    x = (a ^ b) & ((1 << bits) - 1)
    return bin(x).count("1")


# ── Configuration ────────────────────────────────────────────────────────────
MAX_ANNOTATIONS_PER_EVENT = 25  # Increased for emotional dimensions
MIN_KEYWORD_LENGTH = 3
MAX_KEYWORD_LENGTH = 40

# ── Stopwords ────────────────────────────────────────────────────────────────
STOPWORDS = frozenset({
    "the", "and", "for", "are", "but", "not", "you", "all", "can", "had",
    "her", "was", "one", "our", "out", "has", "have", "been", "from", "this",
    "that", "with", "they", "will", "each", "make", "like", "long", "look",
    "many", "some", "than", "them", "then", "into", "just", "over", "such",
    "take", "also", "back", "been", "come", "could", "does", "even", "find",
    "here", "know", "more", "most", "much", "must", "need", "next", "only",
    "other", "said", "same", "should", "show", "since", "still", "sure",
    "tell", "their", "there", "these", "thing", "think", "those", "time",
    "upon", "very", "want", "well", "were", "what", "when", "which", "while",
    "who", "whom", "will", "with", "would", "your", "about", "above",
    "after", "again", "along", "being", "below", "between", "both",
    "during", "every", "given", "going", "great", "however", "itself",
    "might", "never", "often", "perhaps", "quite", "rather", "really",
    "right", "seems", "shall", "something", "through", "under", "until",
    "using", "where", "without", "because", "before", "between",
    "data", "none", "null", "true", "false", "error", "info", "type",
    "value", "result", "results", "status", "unknown", "undefined",
    "default", "output", "input",
})

# ── Project Terms (from BCH auto_tag — always captured) ──────────────────────
PROJECT_TERMS = frozenset({
    # HMSS ecosystem
    "qegg", "drgfc", "lwis", "spts", "hmss", "quad", "2s1c",
    # Team Brain agents
    "beacon", "clio", "nexus", "atlas", "opus", "grok", "laia", "gemini",
    "forge", "iris", "porter", "bolt", "sonnet", "aegis", "copilot",
    "axiom", "codex", "visor",
    # Infrastructure
    "synapse", "mcp", "bch", "uaimc", "ramdisk",
    # Technical
    "api", "websocket", "database", "backup", "sync", "migration",
    "python", "rust", "tauri", "electron", "fastapi", "sqlite",
    "pytorch", "cuda", "vram",
    # Actions
    "task", "bug", "fix", "feature", "review", "deploy", "test",
    # BCH-specific
    "channel", "message", "webhook", "sidecar", "tray",
})

# ── Agent Names (for author detection and boosting) ──────────────────────────
AGENT_NAMES = frozenset({
    "clio", "forge", "atlas", "iris", "porter", "bolt", "sonnet",
    "nexus", "opus", "copilot", "aegis", "grok", "gemini", "laia",
    "axiom", "codex", "visor", "logan",
})

# ── Intent Patterns (from BCH auto_tag) ─────────────────────────────────────
INTENT_PATTERNS = {
    "question": [r"\?$", r"^(what|how|why|when|where|who|which|can|could|would|is|are|do|does)"],
    "request": [r"^(please|can you|could you|would you|i need|i want)"],
    "bug_report": [r"(bug|error|issue|problem|broken|not working|fails|crash)"],
    "feature": [r"(feature|add|implement|create|build|make|new)"],
    "task": [r"(task|todo|action|priority|deadline)"],
    "update": [r"(update|status|progress|report|summary)"],
}


@dataclass
class Annotation:
    """A single extracted annotation token with metadata."""
    token: str
    token_hash: int
    weight: float
    source: str       # 'keyword', 'hashtag', 'project_term', 'agent', 'mention', 'intent'
    category: str     # 'term', 'tag', 'agent', 'entity', 'intent'


# ── OPT-018: Annotation LRU cache keyed on text hash ────────────────────────
_anno_cache: dict[int, list] = {}
_ANNO_CACHE_MAX = 256


def annotate(text: str, source: str = "unknown", author: str = "",
             metadata: dict | None = None) -> list[Annotation]:
    """Extract meaningful annotations from text.

    Args:
        text:     Any text (message, session log, conversation)
        source:   Origin system ('bch', 'session', 'user', 'webhook')
        author:   Author name (agent or human)
        metadata: Optional context dict (channel, tags, etc.)

    Returns:
        List of Annotation objects, sorted by weight descending, limited.
    """
    if not text or not isinstance(text, str):
        return []

    cache_key = fnv1a_hash(text.strip().lower() + source + author)
    if cache_key in _anno_cache:
        return _anno_cache[cache_key]

    result = _annotate_impl(text, source, author, metadata)

    if len(_anno_cache) >= _ANNO_CACHE_MAX:
        # Evict oldest ~quarter of the cache
        keys = list(_anno_cache.keys())
        for k in keys[:_ANNO_CACHE_MAX // 4]:
            del _anno_cache[k]
    _anno_cache[cache_key] = result
    return result


def _annotate_impl(text: str, source: str, author: str,
                   metadata: dict | None) -> list[Annotation]:
    """Core annotation extraction (called by cached annotate())."""

    annotations: dict[str, Annotation] = {}
    text_lower = text.lower()

    # 1. Extract explicit hashtags
    for match in re.finditer(r"#(\w+)", text):
        tag = match.group(1).lower()
        if len(tag) >= MIN_KEYWORD_LENGTH and tag not in STOPWORDS:
            annotations[tag] = Annotation(
                token=tag, token_hash=fnv1a_hash(tag),
                weight=2.0, source="hashtag", category="tag",
            )

    # 2. Extract @mentions
    for match in re.finditer(r"(?:^|[\s,.:;!?])@(\w+)", text, re.MULTILINE):
        mention = match.group(1).lower()
        if mention in AGENT_NAMES:
            annotations[mention] = Annotation(
                token=mention, token_hash=fnv1a_hash(mention),
                weight=2.5, source="mention", category="agent",
            )
        elif mention not in STOPWORDS and len(mention) >= MIN_KEYWORD_LENGTH:
            annotations[mention] = Annotation(
                token=mention, token_hash=fnv1a_hash(mention),
                weight=1.5, source="mention", category="entity",
            )

    # 3. Extract project terms
    for term in PROJECT_TERMS:
        if term in text_lower and term not in annotations:
            annotations[term] = Annotation(
                token=term, token_hash=fnv1a_hash(term),
                weight=2.0, source="project_term", category="term",
            )

    # 4. Detect agent names in text
    for agent in AGENT_NAMES:
        if agent in text_lower and agent not in annotations:
            annotations[agent] = Annotation(
                token=agent, token_hash=fnv1a_hash(agent),
                weight=2.5, source="agent", category="agent",
            )

    # 5. Author as annotation
    if author:
        author_lower = author.lower().strip()
        if author_lower and author_lower not in annotations:
            annotations[author_lower] = Annotation(
                token=author_lower, token_hash=fnv1a_hash(author_lower),
                weight=1.5, source="author", category="agent" if author_lower in AGENT_NAMES else "entity",
            )

    # 6. Detect intent
    for intent, patterns in INTENT_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text_lower):
                if intent not in annotations:
                    annotations[intent] = Annotation(
                        token=intent, token_hash=fnv1a_hash(intent),
                        weight=1.0, source="intent", category="intent",
                    )
                break

    # 7. General keyword extraction
    words = re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", text)
    word_freq: dict[str, int] = {}
    for w in words:
        w_lower = w.lower()
        if (MIN_KEYWORD_LENGTH <= len(w_lower) <= MAX_KEYWORD_LENGTH
                and w_lower not in STOPWORDS
                and w_lower not in annotations):
            word_freq[w_lower] = word_freq.get(w_lower, 0) + 1

    for word, freq in sorted(word_freq.items(), key=lambda x: x[1], reverse=True):
        if len(annotations) >= MAX_ANNOTATIONS_PER_EVENT:
            break
        annotations[word] = Annotation(
            token=word, token_hash=fnv1a_hash(word),
            weight=1.0 + (0.2 * min(freq - 1, 5)),
            source="keyword", category="term",
        )

    # 8. Metadata annotations
    if metadata:
        for key in ("channel", "tags", "source"):
            val = metadata.get(key)
            if val and isinstance(val, str):
                token = val.lower().strip().replace(" ", "_")
                if (token and token not in STOPWORDS
                        and len(token) >= MIN_KEYWORD_LENGTH
                        and token not in annotations):
                    annotations[token] = Annotation(
                        token=token, token_hash=fnv1a_hash(token),
                        weight=1.5, source=f"meta_{key}", category="entity",
                    )

    # 9. Emotional texture annotations (via EmotionalTextureAnalyzer)
    # Emotional annotations get priority — they participate in the final weight-sorted truncation
    try:
        import uaimc_tools
        emotional = uaimc_tools.analyze_emotional_texture(text)
        for emo in emotional:
            token = emo["token"]
            if token not in annotations:
                annotations[token] = Annotation(
                    token=token, token_hash=fnv1a_hash(token),
                    weight=emo["weight"], source=emo["source"],
                    category=emo.get("category", "emotion"),
                )
    except Exception:
        pass  # ETA not available — graceful degradation

    result = sorted(annotations.values(), key=lambda a: a.weight, reverse=True)
    return result[:MAX_ANNOTATIONS_PER_EVENT]


def apply_tfidf(annotations: list[Annotation], db_path: str | None = None,
                db_conn=None) -> list[Annotation]:
    """Apply TF-IDF weighting: boost rare tokens, dampen common ones.

    Accepts an optional open db_conn to avoid opening a new connection per call.
    """
    if not annotations or (not db_path and not db_conn):
        return annotations
    try:
        owns_conn = db_conn is None
        db = db_conn if db_conn else sqlite3.connect(db_path)
        total = db.execute("SELECT COUNT(*) FROM summaries").fetchone()[0]
        if total < 10:
            if owns_conn:
                db.close()
            return annotations
        for anno in annotations:
            doc_freq = db.execute(
                "SELECT COUNT(DISTINCT summary_id) FROM annotations WHERE token = ?",
                (anno.token,)
            ).fetchone()[0]
            idf = math.log(total / (1 + doc_freq)) + 1.0
            anno.weight = round(anno.weight * min(idf, 5.0), 3)
        if owns_conn:
            db.close()
    except Exception:
        pass
    return annotations


def make_summary(text: str, max_length: int = 300) -> str:
    """Create a brief summary from text with sentence awareness."""
    if not text:
        return ""
    if len(text) <= max_length:
        return text.strip()
    truncated = text[:max_length]
    for end_char in (".", "!", "?"):
        last_pos = truncated.rfind(end_char)
        if last_pos > max_length // 2:
            return truncated[:last_pos + 1].strip()
    last_space = truncated.rfind(" ")
    if last_space > max_length // 2:
        return truncated[:last_space].strip() + "..."
    return truncated.strip() + "..."


# ── Phase B4: Aspect Fingerprinting ──────────────────────────────────────────

def compute_aspects(annotations: list[Annotation], source: str, author: str,
                    metadata: dict | None = None) -> dict[str, int]:
    """Compute the 5-aspect fingerprint (Document DNA) for a document.

    Args:
        annotations: Already-computed annotation list from annotate()
        source: Source type string (session_log, bch_message, etc.)
        author: Agent or human author name
        metadata: Optional metadata dict

    Returns:
        Dict with keys: sem_hash, source_hash, agent_hash, intent_hash, project_hash
    """
    # Aspect 1: Semantic — SimHash of top-K annotation tokens by weight
    sorted_annos = sorted(annotations, key=lambda a: a.weight, reverse=True)
    _top_k = int(SCORING.get("simhash_top_k_tokens", 15)) if SCORING else 15
    top_tokens = [a.token for a in sorted_annos[:_top_k]]
    sem_hash = simhash(top_tokens) if top_tokens else 0

    # Aspect 2: Source type
    source_hash = fnv1a_hash(source.lower().strip()) if source else 0

    # Aspect 3: Agent/Author
    agent = (author or "unknown").lower().strip()
    agent_hash = fnv1a_hash(agent)

    # Aspect 4: Intent — first intent annotation found, or "none"
    intent = "none"
    for a in annotations:
        if a.category == "intent":
            intent = a.token
            break
    intent_hash = fnv1a_hash(intent)

    # Aspect 5: Project — first project_term annotation found, or "general"
    project = "general"
    for a in annotations:
        if a.source == "project_term":
            project = a.token
            break
    project_hash = fnv1a_hash(project)

    return {
        "sem_hash": sem_hash,
        "source_hash": source_hash,
        "agent_hash": agent_hash,
        "intent_hash": intent_hash,
        "project_hash": project_hash,
    }
