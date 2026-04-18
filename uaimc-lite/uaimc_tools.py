"""
UAIMC Tools Integration Layer v1.0
====================================
Integrates AutoProject tools into UAIMC's memory pipeline:

Tier 1 (Core Integration):
  - EmotionalTextureAnalyzer → Emotional dimension annotations
  - HashGuard → Backup integrity verification
  - SQLSchemaDiff → Schema drift detection between live/backup DBs

Tier 2 (Available Utilities):
  - RegexLab → Debug annotation patterns
  - JSONQuery → Query annotation data

v1.0 Production Polish:
  - ContextDecayMeter → Auto-summarize trigger based on decay
  - TokenTracker → Per-agent API usage accounting
  - KnowledgeSync → Cross-agent knowledge graph bridge
  - MCPBridge → MCP protocol discovery for UAIMC

PRIVATE -- Not for publication
COPILOT_VSCODE (Team Brain) | March 14, 2026
"""

import json
import logging
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("uaimc.tools")

# ── Tool Paths ────────────────────────────────────────────────────────────────
# Primary: bundled tools/ directory (works on Railway + local)
_BUNDLED_TOOLS = Path(__file__).parent / "tools"
if str(_BUNDLED_TOOLS) not in sys.path:
    sys.path.insert(0, str(_BUNDLED_TOOLS))

# Fallback: local AutoProjects (Windows dev only)
AUTOPROJECT_PATH = Path(r"C:\Users\logan\OneDrive\Documents\AutoProjects")
if AUTOPROJECT_PATH.exists() and str(AUTOPROJECT_PATH) not in sys.path:
    sys.path.insert(1, str(AUTOPROJECT_PATH))

# ── Lazy Imports (tools loaded on first use) ─────────────────────────────────
_eta_instance = None
_hashguard_instance = None
_schema_diff_loaded = False
_token_tracker_instance = None
_knowledge_sync_instance = None


# =============================================================================
# TIER 1: EmotionalTextureAnalyzer Integration
# =============================================================================

def _get_eta():
    """Lazy-load EmotionalTextureAnalyzer singleton."""
    global _eta_instance
    if _eta_instance is not None:
        return _eta_instance
    try:
        from EmotionalTextureAnalyzer.emotionaltextureanalyzer import EmotionalTextureAnalyzer
        _eta_instance = EmotionalTextureAnalyzer()
        logger.info("EmotionalTextureAnalyzer loaded successfully")
        return _eta_instance
    except Exception as e:
        logger.warning(f"EmotionalTextureAnalyzer not available: {e}")
        return None


def analyze_emotional_texture(text: str) -> list[dict]:
    """
    Analyze text for emotional dimensions and return annotation-ready tokens.

    Returns list of dicts: [{"token": "emotion_warmth", "weight": 3.2, "source": "emotional"}, ...]
    Only dimensions with score > 0 are returned. Top 5 dimensions max.
    """
    eta = _get_eta()
    if not eta or not text or len(text) < 20:
        return []
    try:
        result = eta.analyze(text)
        scores = result.get("dimension_scores", {})
        # Filter to dimensions with actual signal, sort by score descending
        active = [(dim, score) for dim, score in scores.items() if score > 0.5]
        active.sort(key=lambda x: x[1], reverse=True)
        annotations = []
        for dim, score in active[:5]:
            token = f"emotion_{dim.lower()}"
            annotations.append({
                "token": token,
                "weight": round(min(score, 5.0), 2),  # cap at 5.0
                "source": "emotional",
                "category": "emotion",
            })
        # Add dominant emotion as a separate high-weight annotation
        dominant = result.get("dominant_emotion", "")
        if dominant and result.get("dominant_score", 0) > 1.0:
            token = f"dominant_{dominant.lower()}"
            if not any(a["token"] == token for a in annotations):
                annotations.append({
                    "token": token,
                    "weight": round(min(result["dominant_score"] * 1.5, 5.0), 2),
                    "source": "emotional",
                    "category": "emotion",
                })
        return annotations
    except Exception as e:
        logger.debug(f"ETA analysis failed: {e}")
        return []


# =============================================================================
# TIER 1: HashGuard Integration
# =============================================================================

def _get_hashguard():
    """Lazy-load HashGuard singleton."""
    global _hashguard_instance
    if _hashguard_instance is not None:
        return _hashguard_instance
    try:
        from HashGuard.hashguard import HashGuard
        _hashguard_instance = HashGuard()
        logger.info("HashGuard loaded successfully")
        return _hashguard_instance
    except Exception as e:
        logger.warning(f"HashGuard not available: {e}")
        return None


# OPT-001: Cache backup hash keyed on (mtime, size) with 5-minute TTL
_backup_hash_cache: dict = {"mtime": 0, "size": 0, "hash": None, "time": 0.0, "path": ""}
_BACKUP_HASH_TTL = 300  # 5 minutes

def verify_backup_integrity(backup_dir: str, db_name: str = "uaimc.db") -> dict:
    """
    Verify backup database integrity using HashGuard.
    Returns {"status": "ok"|"warn"|"error", "details": ...}
    OPT-001: Results cached for 5 minutes keyed on file mtime+size.
    """
    backup_path = Path(backup_dir) / db_name
    if not backup_path.exists():
        return {"status": "warn", "details": "No backup file found"}

    # OPT-001: Return cached hash if file unchanged and cache fresh
    stat = backup_path.stat()
    now = time.time()
    cache = _backup_hash_cache
    if (cache["hash"] is not None
            and cache["mtime"] == stat.st_mtime
            and cache["size"] == stat.st_size
            and (now - cache["time"]) < _BACKUP_HASH_TTL):
        return {
            "status": "ok",
            "hash": cache["hash"],
            "size_bytes": cache["size"],
            "age_seconds": int(now - stat.st_mtime),
            "path": str(backup_path),
            "cached": True,
        }

    def _hashlib_fallback(path: Path) -> dict:
        import hashlib
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return {
            "status": "ok",
            "hash": h.hexdigest(),
            "size_bytes": path.stat().st_size,
            "age_seconds": int(time.time() - path.stat().st_mtime),
            "path": str(path),
            "method": "fallback_hashlib",
        }

    try:
        hg = _get_hashguard()
        if hg:
            try:
                file_hash = hg.hash_file(str(backup_path))
            except ValueError:
                result = _hashlib_fallback(backup_path)
                _backup_hash_cache.update(mtime=stat.st_mtime, size=stat.st_size,
                                          hash=result["hash"], time=now)
                return result
            size = stat.st_size
            age = int(now - stat.st_mtime)
            _backup_hash_cache.update(mtime=stat.st_mtime, size=stat.st_size,
                                      hash=file_hash, time=now)
            return {
                "status": "ok",
                "hash": file_hash,
                "size_bytes": size,
                "age_seconds": age,
                "path": str(backup_path),
            }
        else:
            result = _hashlib_fallback(backup_path)
            _backup_hash_cache.update(mtime=stat.st_mtime, size=stat.st_size,
                                      hash=result["hash"], time=now)
            return result
    except Exception as e:
        return {"status": "error", "details": str(e)}


# =============================================================================
# TIER 1: SQLSchemaDiff Integration
# =============================================================================

def _load_schema_diff():
    """Lazy-load SQLSchemaDiff module."""
    global _schema_diff_loaded
    if _schema_diff_loaded:
        return True
    try:
        from SQLSchemaDiff.sqlschemadiff import auto_parse, DiffEngine, MigrationGenerator  # noqa: F401
        _schema_diff_loaded = True
        logger.info("SQLSchemaDiff loaded successfully")
        return True
    except Exception as e:
        logger.warning(f"SQLSchemaDiff not available: {e}")
        return False


def check_schema_drift(live_db_path: str, backup_db_path: str) -> dict:
    """
    Compare schemas between live DB and backup DB.
    Returns {"drifted": bool, "changes": [...], "migration_sql": str}
    """
    if not Path(live_db_path).exists() or not Path(backup_db_path).exists():
        return {"drifted": False, "details": "One or both databases not found"}
    try:
        if _load_schema_diff():
            from SQLSchemaDiff.sqlschemadiff import auto_parse, DiffEngine, MigrationGenerator
            live_snap = auto_parse(live_db_path)
            backup_snap = auto_parse(backup_db_path)
            diff = DiffEngine().diff(live_snap, backup_snap)
            changes = []
            for attr, label in [("added_tables", "Added"), ("removed_tables", "Removed"), ("changed_tables", "Changed")]:
                items = getattr(diff, attr, None)
                if items:
                    for item in items:
                        name = getattr(item, "table_name", str(item)) if label == "Changed" else str(item)
                        changes.append(f"{label} table: {name}")
            migration_sql = ""
            if changes:
                try:
                    migration_sql = MigrationGenerator().generate(diff)
                except Exception:
                    migration_sql = "-- migration generation not supported for this diff"
            return {
                "drifted": len(changes) > 0,
                "changes": changes,
                "migration_sql": migration_sql,
            }
        else:
            # Fallback: compare table lists via pragma
            live_tables = _get_table_names(live_db_path)
            backup_tables = _get_table_names(backup_db_path)
            added = live_tables - backup_tables
            removed = backup_tables - live_tables
            return {
                "drifted": bool(added or removed),
                "changes": [
                    *(f"Added: {t}" for t in added),
                    *(f"Removed: {t}" for t in removed),
                ],
                "method": "fallback_pragma",
            }
    except Exception as e:
        return {"drifted": False, "error": str(e)}


def _get_table_names(db_path: str) -> set[str]:
    """Get set of table names from a SQLite database."""
    conn = sqlite3.connect(db_path)
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()}
    conn.close()
    return tables


# =============================================================================
# v1.0 PRODUCTION POLISH: ContextDecayMeter Integration
# =============================================================================

def calculate_context_decay(db_conn, agent: str = "", window_messages: int = 50) -> dict:
    """
    Calculate context decay metrics for the memory system.
    Uses conversation age and retrieval patterns to estimate decay.

    Returns {"decay_rate": float, "health": str, "recommendation": str}
    """
    try:
        # Calculate based on age distribution of recent retrievals
        rows = db_conn.execute("""
            SELECT created_at FROM summaries
            ORDER BY created_at DESC LIMIT ?
        """, (window_messages,)).fetchall()

        if len(rows) < 5:
            return {"decay_rate": 0.0, "health": "healthy", "recommendation": "Too few entries to measure decay"}

        now = time.time()
        ages_hours = []
        for row in rows:
            try:
                dt = datetime.strptime(row[0][:19], "%Y-%m-%d %H:%M:%S")
                # DB stores UTC timestamps — convert to UTC epoch
                import calendar
                age_h = (now - calendar.timegm(dt.timetuple())) / 3600
                ages_hours.append(age_h)
            except (ValueError, OSError):
                continue

        if not ages_hours:
            return {"decay_rate": 0.0, "health": "healthy", "recommendation": "No valid timestamps"}

        avg_age = sum(ages_hours) / len(ages_hours)
        max_age = max(ages_hours)

        # Decay rate: 0-1 based on how old the newest entries are getting
        # If average age > 24h, decay is happening
        decay_rate = min(1.0, avg_age / 168)  # 168h = 7 days max

        # Check for activity gaps (no new entries in last N hours)
        newest_age = min(ages_hours)
        gap_hours = newest_age

        if gap_hours > 24:
            health = "decaying"
            recommendation = f"No new entries in {gap_hours:.0f}h. Consider ingesting recent activity."
        elif gap_hours > 4:
            health = "aging"
            recommendation = f"Last entry was {gap_hours:.1f}h ago. Context freshness declining."
        else:
            health = "healthy"
            recommendation = "Context is fresh and actively maintained."

        # Agent-specific metrics
        agent_stats = {}
        if agent:
            agent_rows = db_conn.execute("""
                SELECT MAX(timestamp) as last_active, COUNT(*) as activities
                FROM agent_activity WHERE agent_name = ?
            """, (agent,)).fetchone()
            if agent_rows and agent_rows[0]:
                agent_stats = {
                    "last_active": agent_rows[0],
                    "total_activities": agent_rows[1],
                }

        return {
            "decay_rate": round(decay_rate, 3),
            "health": health,
            "recommendation": recommendation,
            "avg_entry_age_hours": round(avg_age, 1),
            "newest_entry_age_hours": round(newest_age, 1),
            "oldest_entry_age_hours": round(max_age, 1),
            "entries_sampled": len(ages_hours),
            "agent_stats": agent_stats,
        }
    except Exception as e:
        return {"decay_rate": 0.0, "health": "unknown", "error": str(e)}


# =============================================================================
# v1.0 PRODUCTION POLISH: TokenTracker Integration
# =============================================================================

def _get_token_tracker(db_path: str = None):
    """Lazy-load TokenTracker with UAIMC's own tracking database."""
    global _token_tracker_instance
    if _token_tracker_instance is not None:
        return _token_tracker_instance
    try:
        from TokenTracker.tokentracker import TokenTracker
        tracker_db = Path(db_path).parent / "token_usage.db" if db_path else Path(__file__).parent / "data" / "token_usage.db"
        _token_tracker_instance = TokenTracker(db_path=tracker_db)
        logger.info(f"TokenTracker loaded, DB: {tracker_db}")
        return _token_tracker_instance
    except Exception as e:
        logger.warning(f"TokenTracker not available: {e}")
        return None


def track_api_usage(agent: str, action: str, input_chars: int = 0,
                    output_chars: int = 0, db_path: str = None) -> None:
    """Track UAIMC API call usage per agent. Estimates token count from chars."""
    tracker = _get_token_tracker(db_path)
    if not tracker:
        return
    try:
        # Rough estimate: 4 chars ≈ 1 token
        input_tokens = input_chars // 4
        output_tokens = output_chars // 4
        tracker.log_usage(
            agent=agent or "UNKNOWN",
            model="uaimc-local",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            notes=f"UAIMC {action}",
        )
    except Exception as e:
        logger.debug(f"Token tracking failed: {e}")


def get_token_usage_summary(db_path: str = None) -> dict:
    """Get token usage summary across all agents."""
    tracker = _get_token_tracker(db_path)
    if not tracker:
        return {"available": False}
    try:
        return tracker.get_usage_summary()
    except Exception as e:
        return {"available": False, "error": str(e)}


# =============================================================================
# v1.0 PRODUCTION POLISH: KnowledgeSync Integration
# =============================================================================

def _get_knowledge_sync():
    """Lazy-load KnowledgeSync."""
    global _knowledge_sync_instance
    if _knowledge_sync_instance is not None:
        return _knowledge_sync_instance
    try:
        from KnowledgeSync.knowledgesync import KnowledgeSync
        _knowledge_sync_instance = KnowledgeSync(agent="UAIMC")
        logger.info("KnowledgeSync loaded successfully")
        return _knowledge_sync_instance
    except Exception as e:
        logger.warning(f"KnowledgeSync not available: {e}")
        return None


def sync_to_knowledge_graph(text: str, source: str, author: str = "",
                            topics: list[str] | None = None) -> bool:
    """Push a new entry to the KnowledgeSync graph for cross-agent discovery."""
    ks = _get_knowledge_sync()
    if not ks:
        return False
    try:
        ks.add(
            content=text[:500],  # Summarized content for knowledge graph
            source=author or source,
            category="FACT",
            topics=topics or [],
            confidence=0.8,
        )
        return True
    except Exception as e:
        logger.debug(f"KnowledgeSync add failed: {e}")
        return False


def query_knowledge_graph(query: str, agent: str = "") -> list[dict]:
    """Query the KnowledgeSync graph for cross-agent knowledge."""
    ks = _get_knowledge_sync()
    if not ks:
        return []
    try:
        results = ks.query(query)
        if agent:
            agent_results = ks.query_agent(agent)
            results.extend(agent_results)
        return [r.to_dict() if hasattr(r, "to_dict") else r for r in results[:20]]
    except Exception as e:
        logger.debug(f"KnowledgeSync query failed: {e}")
        return []


def get_knowledge_stats() -> dict:
    """Get KnowledgeSync statistics."""
    ks = _get_knowledge_sync()
    if not ks:
        return {"available": False}
    try:
        return ks.get_stats()
    except Exception as e:
        return {"available": False, "error": str(e)}


# =============================================================================
# v1.0 PRODUCTION POLISH: MCPBridge — UAIMC as MCP Server
# =============================================================================

def get_mcp_tool_definitions() -> list[dict]:
    """
    Return UAIMC's endpoints as MCP-compatible tool definitions.
    These can be registered with MCPBridge for agent discovery.
    """
    return [
        {
            "name": "uaimc_ingest",
            "description": "Store content into UAIMC shared memory with auto-annotation",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "content": {"type": "string", "description": "Text content to store"},
                    "source": {"type": "string", "description": "Source identifier (e.g., 'cursor', 'bch')"},
                    "author": {"type": "string", "description": "Agent or user name"},
                },
                "required": ["content", "source"],
            },
        },
        {
            "name": "uaimc_query",
            "description": "Search UAIMC shared memory by keywords",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "q": {"type": "string", "description": "Search query"},
                    "limit": {"type": "integer", "description": "Max results", "default": 10},
                },
                "required": ["q"],
            },
        },
        {
            "name": "uaimc_context",
            "description": "Get assembled context from UAIMC for prompt injection",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "description": "Agent requesting context"},
                    "topic": {"type": "string", "description": "Topic to recall"},
                    "max_chars": {"type": "integer", "default": 4000},
                },
                "required": [],
            },
        },
        {
            "name": "uaimc_health",
            "description": "Get UAIMC system health status",
            "inputSchema": {"type": "object", "properties": {}},
        },
    ]


def register_with_mcp_bridge() -> dict:
    """
    Register UAIMC tools with MCPBridge for protocol discovery.
    Returns status of registration attempt.
    """
    try:
        from MCPBridge.mcpbridge import ProtocolBridge  # noqa: F401
        tools = get_mcp_tool_definitions()
        logger.info(f"MCP tool definitions ready: {len(tools)} tools")
        return {
            "status": "ready",
            "tools_registered": len(tools),
            "tool_names": [t["name"] for t in tools],
            "note": "MCP server can expose these via ProtocolBridge",
        }
    except Exception as e:
        return {"status": "unavailable", "error": str(e)}


# =============================================================================
# TOOL STATUS REPORT
# =============================================================================

def get_tools_status() -> dict:
    """Return status of all integrated tools."""
    status = {}

    # Tier 1
    eta = _get_eta()
    status["EmotionalTextureAnalyzer"] = {
        "tier": 1, "loaded": eta is not None,
        "purpose": "Emotional dimension annotations",
    }

    hg = _get_hashguard()
    status["HashGuard"] = {
        "tier": 1, "loaded": hg is not None,
        "purpose": "Backup integrity verification",
    }

    status["SQLSchemaDiff"] = {
        "tier": 1, "loaded": _schema_diff_loaded,
        "purpose": "Schema drift detection",
    }

    # v1.0 Production Polish
    tt = _get_token_tracker()
    status["TokenTracker"] = {
        "tier": "v1.0", "loaded": tt is not None,
        "purpose": "Per-agent API usage accounting",
    }

    ks = _get_knowledge_sync()
    status["KnowledgeSync"] = {
        "tier": "v1.0", "loaded": ks is not None,
        "purpose": "Cross-agent knowledge graph",
    }

    mcp = register_with_mcp_bridge()
    status["MCPBridge"] = {
        "tier": "v1.0", "loaded": mcp.get("status") == "ready",
        "purpose": "MCP protocol discovery",
        "tools": mcp.get("tool_names", []),
    }

    # Tier 2 (available but not loaded by default)
    for tool_name, purpose in [
        ("RegexLab", "Debug annotation patterns"),
        ("JSONQuery", "Query annotation data"),
        ("TextTransform", "Format conversion utilities"),
    ]:
        tool_path = _BUNDLED_TOOLS / tool_name
        if not tool_path.exists():
            tool_path = AUTOPROJECT_PATH / tool_name
        status[tool_name] = {
            "tier": 2, "loaded": False,
            "available": tool_path.exists(),
            "purpose": purpose,
        }

    return status
