"""
Guardian Tools — Tool Integration Layer for GUARDIAN LIB
=========================================================
Imports all 12 approved AutoProject tools as Python modules.
Each tool is imported with graceful fallback — no missing tool
crashes Guardian. Tools are called by Guardian's pipeline code,
NOT exposed to Opus via tool_use API.

Spec Reference: GUARDIAN_AI_SPECIFICATION.md Section 18
Architecture: Python-calls-tools (NOT Anthropic tool_use)

PRIVATE -- Not for publication
AEGIS (Team Brain) | B-004 Guardian Alpha | 2026-03-17
"""

import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional

logger = logging.getLogger("uaimc.guardian.tools")

# ── AutoProjects Path ────────────────────────────────────────────────────────
AUTOPROJECT_PATH = r"C:\Users\logan\OneDrive\Documents\AutoProjects"
if AUTOPROJECT_PATH not in sys.path:
    sys.path.insert(0, AUTOPROJECT_PATH)

# ── Tool Registry ────────────────────────────────────────────────────────────
# Each entry: { "name": str, "class": type|None, "available": bool, "tier": 1|2 }
TOOLS: dict[str, dict[str, Any]] = {}

# ── Safe Imports (Tier 1 — Core) ─────────────────────────────────────────────

def _import_tool(name: str, module_path: str, class_name: str, tier: int) -> None:
    """Import a tool class with graceful fallback."""
    try:
        parts = module_path.split(".")
        mod = __import__(module_path, fromlist=[parts[-1]])
        cls = getattr(mod, class_name)
        TOOLS[name] = {"name": name, "class": cls, "available": True, "tier": tier}
        logger.info(f"Tool loaded: {name} (Tier {tier})")
    except Exception as e:
        TOOLS[name] = {"name": name, "class": None, "available": False, "tier": tier}
        logger.warning(f"Tool unavailable: {name} — {e}")


# Tier 1 — Core (7 tools)
_import_tool("TokenTracker",       "TokenTracker.tokentracker",       "TokenTracker",       1)
_import_tool("ContextCompressor",  "ContextCompressor.contextcompressor", "ContextCompressor", 1)
_import_tool("SQLiteExplorer",     "SQLiteExplorer.sqliteexplorer",   "SQLiteExplorer",     1)
_import_tool("KnowledgeSync",      "KnowledgeSync.knowledgesync",     "KnowledgeSync",      1)
_import_tool("ContextSynth",       "ContextSynth.contextsynth",       "ContextSynth",       1)
_import_tool("SQLSchemaDiff",      "SQLSchemaDiff.sqlschemadiff",     "DiffEngine",         1)
_import_tool("ConversationThreadReconstructor",
             "ConversationThreadReconstructor.conversationthreadreconstructor",
             "ConversationThreadReconstructor", 1)

# Tier 2 — High Value (5 tools)
_import_tool("MemoryBridge",       "MemoryBridge.memorybridge",       "MemoryBridge",       2)
_import_tool("SemanticFirewall",   "SemanticFirewall.semanticfirewall", "SemanticFirewall",  2)
_import_tool("EchoGuard",          "EchoGuard.echoguard",             "EchoGuard",          2)
_import_tool("ConversationAuditor", "ConversationAuditor.conversationauditor", "ConversationAuditor", 2)
_import_tool("TextTransform",      "TextTransform.texttransform",     "TextTransformer",    2)

# ── Tool Usage Logging ──────────────────────────────────────────────────────
_tool_usage_log: list[dict] = []


def log_tool_usage(tool_name: str, success: bool, error: str = "",
                   duration_ms: float = 0.0) -> None:
    """Record a tool invocation for compliance tracking."""
    _tool_usage_log.append({
        "tool": tool_name,
        "success": success,
        "error": error,
        "duration_ms": round(duration_ms, 1),
        "timestamp": time.time(),
    })


def get_tool_usage_stats() -> dict:
    """Return tool usage statistics for the current session."""
    total = len(_tool_usage_log)
    successes = sum(1 for e in _tool_usage_log if e["success"])
    by_tool: dict[str, dict] = {}
    for entry in _tool_usage_log:
        name = entry["tool"]
        if name not in by_tool:
            by_tool[name] = {"calls": 0, "successes": 0, "failures": 0, "total_ms": 0.0}
        by_tool[name]["calls"] += 1
        if entry["success"]:
            by_tool[name]["successes"] += 1
        else:
            by_tool[name]["failures"] += 1
        by_tool[name]["total_ms"] += entry["duration_ms"]
    return {
        "total_calls": total,
        "successes": successes,
        "failures": total - successes,
        "by_tool": by_tool,
    }


# ── Safe Tool Execution ─────────────────────────────────────────────────────

async def run_tool_safe(tool_name: str, func: Callable, *args: Any,
                        timeout: float = 30.0, **kwargs: Any) -> Optional[Any]:
    """Call a tool function with timeout and error handling.

    Never crashes Guardian. Returns None on failure.
    Per spec Section 18.5 — 30s max per tool call.
    """
    if tool_name in TOOLS and not TOOLS[tool_name]["available"]:
        logger.debug(f"Skipping unavailable tool: {tool_name}")
        return None

    start = time.monotonic()
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(func, *args, **kwargs),
            timeout=timeout,
        )
        elapsed = (time.monotonic() - start) * 1000
        log_tool_usage(tool_name, success=True, duration_ms=elapsed)
        return result
    except asyncio.TimeoutError:
        elapsed = (time.monotonic() - start) * 1000
        log_tool_usage(tool_name, success=False, error="timeout", duration_ms=elapsed)
        logger.warning(f"Tool timeout ({timeout}s): {tool_name}")
        return None
    except Exception as e:
        elapsed = (time.monotonic() - start) * 1000
        log_tool_usage(tool_name, success=False, error=str(e)[:200], duration_ms=elapsed)
        logger.error(f"Tool error: {tool_name} — {e}")
        return None


# ── Tool Accessors ───────────────────────────────────────────────────────────

def get_tool(name: str) -> Optional[type]:
    """Get a tool class by name. Returns None if unavailable."""
    entry = TOOLS.get(name)
    if entry and entry["available"]:
        return entry["class"]
    return None


def get_available_tools() -> list[str]:
    """Return names of all successfully loaded tools."""
    return [name for name, info in TOOLS.items() if info["available"]]


def get_tool_inventory() -> dict[str, dict]:
    """Return full tool inventory with availability status."""
    return {
        name: {
            "available": info["available"],
            "tier": info["tier"],
            "class_name": info["class"].__name__ if info["class"] else None,
        }
        for name, info in TOOLS.items()
    }


# ── Tiered Activation ───────────────────────────────────────────────────────
# Per spec Section 18.3 — tools activate based on query classification

TIER_TOOLS = {
    "LITE": ["TokenTracker", "ContextCompressor", "SQLiteExplorer", "SemanticFirewall"],
    "STANDARD": ["TokenTracker", "ContextCompressor", "SQLiteExplorer",
                  "KnowledgeSync", "ContextSynth", "SemanticFirewall"],
    "DEEP": ["TokenTracker", "ContextCompressor", "SQLiteExplorer",
             "KnowledgeSync", "ContextSynth", "SemanticFirewall",
             "ConversationThreadReconstructor", "MemoryBridge"],
    "EXHAUSTIVE": ["TokenTracker", "ContextCompressor", "SQLiteExplorer",
                   "KnowledgeSync", "ContextSynth", "SemanticFirewall",
                   "ConversationThreadReconstructor", "MemoryBridge",
                   "ConversationAuditor"],
    "CURATION": ["SQLiteExplorer", "SQLSchemaDiff", "EchoGuard",
                 "TextTransform", "ConversationAuditor", "SemanticFirewall"],
    "STARTUP": ["SQLiteExplorer", "SQLSchemaDiff", "TokenTracker"],
}


def get_tools_for_tier(tier: str) -> list[str]:
    """Return tool names that should activate for a given query tier.

    Only returns tools that are actually available (loaded successfully).
    """
    tier_upper = tier.upper()
    if tier_upper not in TIER_TOOLS:
        logger.warning(f"Unknown tier: {tier} — defaulting to LITE")
        tier_upper = "LITE"
    return [name for name in TIER_TOOLS[tier_upper] if TOOLS.get(name, {}).get("available")]
