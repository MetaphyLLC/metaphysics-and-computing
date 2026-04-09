r"""
UAIMC Service v1.0 — Universal AI Memory Core
====================================================
FastAPI service exposing shared memory for ALL AI agents.
Ported from server UMA unified_memory.py, adapted for local multi-agent use.

v1.0: GPU-AM live, auto-backup, auto-start on boot, file watcher auto-ingest.

Endpoints:
  POST /ingest          — Store content + auto-annotate
  GET  /query           — Search annotations (GPU-first, FTS5 fallback)
  GET  /context         — Assembled context for agent prompt injection
  GET  /health          — Full system health + GPU-AM stats
  GET  /stats           — Quick stats
  GET  /recent          — Recent entries
  GET  /agents          — All known agents with activity timestamps
  GET  /gpu             — GPU-AM detailed statistics
  POST /backup          — Trigger manual backup
  GET  /watcher         — File watcher status
  GET  /reminders       — Scheduled reminders status
  WS   /ws              — WebSocket for real-time feed
  GET  /graph/file      — File node, children, edges, domain, summary
  GET  /graph/search    — FTS5 search on kg_nodes with parent resolution
  GET  /graph/stats     — Node/edge/domain/orphan counts

Architecture:
  SQLite WAL + FTS5 + GPU-AM (CUDA tensors) -> sub-ms annotation recall
  Auto-backup every 5 min to D:\BEACON_HQ\PROJECTS\00_ACTIVE\UAIMC\backup\

PRIVATE -- Not for publication
COPILOT_VSCODE (Team Brain) | March 15, 2026
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import logging
import zstandard as zstd
import math
import os
import pickle
import queue
import re
import shutil
import sqlite3
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import math as _math

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from ppr_engine import compute_ppr_push, load_edge_overrides
from pydantic import BaseModel


def _sanitize_floats(obj):
    """Recursively replace inf/nan with None so JSON serialization never crashes."""
    if isinstance(obj, float):
        if _math.isinf(obj) or _math.isnan(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _sanitize_floats(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_floats(v) for v in obj]
    return obj


class SafeJSONResponse(JSONResponse):
    """JSONResponse that sanitizes inf/nan floats before rendering."""
    def render(self, content) -> bytes:
        return super().render(_sanitize_floats(content))

# Add parent dir for local imports
sys.path.insert(0, str(Path(__file__).parent))
import uaimc_anno
import uaimc_ambient
import uaimc_gpu
import uaimc_tools
import uaimc_watcher

# Guardian AI (B-004)
_guardian = None
try:
    from uaimc_guardian import GuardianAI
    _GUARDIAN_AVAILABLE = True
except ImportError as e:
    logging.getLogger("uaimc").warning(f"Guardian module not available: {e}")
    _GUARDIAN_AVAILABLE = False


def get_guardian():
    """Get the global Guardian instance (may be None if unavailable)."""
    return _guardian

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "uaimc_service.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("uaimc")

# ── Configuration ────────────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent / "config" / "config.json"


def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    return {}


CONFIG = load_config()

HOST = os.environ.get("HOST", CONFIG.get("service", {}).get("host", "127.0.0.1"))
PORT = int(os.environ.get("PORT", CONFIG.get("service", {}).get("port", 8765)))
DB_PATH = os.environ.get("DB_PATH", CONFIG.get("ramdisk", {}).get("db_path", "U:\\uaimc\\uaimc.db"))
BACKUP_DIR = Path(CONFIG.get("backup", {}).get("dir", str(Path(__file__).parent / "backup")))
BACKUP_INTERVAL = CONFIG.get("backup", {}).get("interval_seconds", 300)
_GPU_ENABLED = CONFIG.get("gpu", {}).get("enabled", True)
CONTEXT_LIMIT = CONFIG.get("context", {}).get("default_token_budget", 4000)
MAX_CONTEXT = CONFIG.get("context", {}).get("max_token_budget", 8000)
# RECENCY_BOOST_24H / _7D removed (dead code, superseded by _apply_temporal_scoring)

# ── Phase B1-Step1: Ambient result cache (module-level, TTL-based) ────────────
_AMBIENT_CACHE: dict[tuple, tuple[float, str]] = {}
_AMBIENT_CACHE_TTL: float = float(CONFIG.get("context", {}).get("ambient_cache_ttl_seconds", 120))
_AMBIENT_CACHE_MAX: int = 64  # OPT-034: bounded size

# ── Sprint 2: Stats + Health caches ──────────────────────────────────────────
_stats_cache: dict = {"data": None, "time": 0.0}
_STATS_TTL: float = 15.0  # seconds

_health_cache: dict = {"data": None, "time": 0.0}
_HEALTH_TTL: float = 5.0  # seconds

_graph_stats_cache: dict = {"data": None, "time": 0.0}
_GRAPH_STATS_TTL: float = 30.0  # seconds

# ── OPT-024: Background stats updater ────────────────────────────────────────
_bg_stats_task: asyncio.Task | None = None
_BG_STATS_INTERVAL: float = 20.0  # seconds

# ── OPT-029: Query result LRU cache (TTL-based dict) ─────────────────────────
_QUERY_CACHE: dict[tuple, tuple[float, dict]] = {}  # key -> (timestamp, result)
_QUERY_CACHE_TTL: float = 30.0  # seconds
_QUERY_CACHE_MAX: int = 128  # max entries

# ── Phase B1-FC2: Config hot-reload support ───────────────────────────────────
_CONFIG_MTIME: float = CONFIG_PATH.stat().st_mtime if CONFIG_PATH.exists() else 0.0
_CONFIG_LAST_CHECK: float = 0.0  # OPT-033: rate-limit config checks
_CONFIG_CHECK_INTERVAL: float = 5.0  # seconds


def reload_config_if_changed() -> bool:
    """Check config.json mtime; reload CONFIG + SCORING globals if changed. Returns True if reloaded.
    OPT-033: Rate-limited to once per 5 seconds."""
    global CONFIG, SCORING, _CONFIG_MTIME, _AMBIENT_CACHE_TTL, _CONFIG_LAST_CHECK
    now = time.time()
    if (now - _CONFIG_LAST_CHECK) < _CONFIG_CHECK_INTERVAL:
        return False
    _CONFIG_LAST_CHECK = now
    if not CONFIG_PATH.exists():
        return False
    mtime = CONFIG_PATH.stat().st_mtime
    if mtime <= _CONFIG_MTIME:
        return False
    try:
        with open(CONFIG_PATH, "r") as f:
            new_cfg = json.load(f)
        CONFIG = new_cfg
        SCORING = CONFIG.get("scoring", {})
        _AMBIENT_CACHE_TTL = float(CONFIG.get("context", {}).get("ambient_cache_ttl_seconds", 120))
        _CONFIG_MTIME = mtime
        logger.info("Config hot-reloaded from %s (mtime=%s)", CONFIG_PATH, mtime)
        return True
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Config hot-reload failed: %s", e)
        return False

# ── Scoring Config (Phase A Dynamic Ranking) ─────────────────────────────────
SCORING = CONFIG.get("scoring", {})

# ── CANS/GAAMA Config (Sprint 4 — Hybrid PPR) ────────────────────────────────
GAAMA_ENABLED = CONFIG.get("gaama", {}).get("enabled", True)
GAAMA_PPR_WEIGHT = CONFIG.get("gaama", {}).get("ppr_weight", 0.3)
GAAMA_PPR_ALPHA = CONFIG.get("gaama", {}).get("ppr_alpha", 0.15)
GAAMA_PPR_MAX_ITER = CONFIG.get("gaama", {}).get("ppr_max_iter", 50)
GAAMA_PPR_EPSILON = CONFIG.get("gaama", {}).get("ppr_epsilon", 1e-4)
GAAMA_HUB_THETA_DEFAULT = CONFIG.get("gaama", {}).get("hub_dampening_theta", 50)
GAAMA_PPR_HOPS = CONFIG.get("gaama", {}).get("ppr_hops", 2)
GAAMA_PPR_TOP_K = CONFIG.get("gaama", {}).get("ppr_top_k", 200)
GAAMA_BLEND_MODE = CONFIG.get("gaama", {}).get("blend_mode", "weighted")
GAAMA_CACHE_TTL = CONFIG.get("gaama", {}).get("cache_ttl_seconds", 3600)

# Validate half_life config values (must be > 0 to avoid division-by-zero)
for _hl_key in ("temporal_half_life_hours", "heat_decay_half_life_hours", "rejection_half_life_days"):
    _hl_val = SCORING.get(_hl_key)
    if _hl_val is not None and _hl_val <= 0:
        logging.getLogger("uaimc").warning(
            "Scoring config '%s' = %s is invalid (must be > 0), defaulting to 1", _hl_key, _hl_val)
        SCORING[_hl_key] = 1

# B-009: Date patterns for generation timestamp extraction
_DATE_PATTERNS = [
    (re.compile(r"(\d{4}-\d{2}-\d{2})"), "%Y-%m-%d"),
    (re.compile(r"(\d{4}_\d{2}_\d{2})"), "%Y_%m_%d"),
    (re.compile(
        r"((?:January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+\d{1,2},?\s+\d{4})", re.I
    ), None),  # Month DD, YYYY — handled separately
]

# B-009: Temporal intent keyword sets (configurable)
TEMPORAL_RECENT_SIGNALS = set(SCORING.get("temporal_recent_signals",
    ["current", "latest", "now", "today", "recent", "status", "update"]))
TEMPORAL_HISTORICAL_SIGNALS = set(SCORING.get("temporal_historical_signals",
    ["original", "first", "history", "foundation", "origin", "initial", "when", "earliest"]))

# ── Schema ───────────────────────────────────────────────────────────────────
_SCHEMA = """
CREATE TABLE IF NOT EXISTS verbatim (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    content      TEXT NOT NULL,
    content_hash TEXT,
    source       TEXT NOT NULL,
    author       TEXT DEFAULT '',
    channel      TEXT DEFAULT '',
    metadata     TEXT,
    byte_size    INTEGER,
    created_at   TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS summaries (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    content      TEXT NOT NULL,
    verbatim_id  INTEGER,
    source       TEXT NOT NULL,
    metadata     TEXT,
    created_at   TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (verbatim_id) REFERENCES verbatim(id)
);

CREATE TABLE IF NOT EXISTS annotations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    token       TEXT NOT NULL,
    token_hash  INTEGER NOT NULL,
    weight      REAL DEFAULT 1.0,
    summary_id  INTEGER NOT NULL,
    source      TEXT NOT NULL,
    created_at  TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (summary_id) REFERENCES summaries(id)
);

CREATE TABLE IF NOT EXISTS agent_activity (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_name TEXT NOT NULL,
    action     TEXT NOT NULL,
    details    TEXT,
    timestamp  TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_annotations_hash    ON annotations(token_hash);
CREATE INDEX IF NOT EXISTS idx_annotations_token   ON annotations(token);
CREATE INDEX IF NOT EXISTS idx_annotations_source  ON annotations(source);
CREATE INDEX IF NOT EXISTS idx_annotations_summary ON annotations(summary_id);
CREATE INDEX IF NOT EXISTS idx_anno_token_summary  ON annotations(token, summary_id);
CREATE INDEX IF NOT EXISTS idx_summaries_source    ON summaries(source);
CREATE INDEX IF NOT EXISTS idx_verbatim_source     ON verbatim(source);
CREATE INDEX IF NOT EXISTS idx_verbatim_author     ON verbatim(author);
CREATE INDEX IF NOT EXISTS idx_agent_activity_name ON agent_activity(agent_name);

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

_FTS_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS annotations_fts USING fts5(
    token,
    content='annotations',
    content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS annotations_ai AFTER INSERT ON annotations BEGIN
    INSERT INTO annotations_fts(rowid, token) VALUES (new.id, new.token);
END;

CREATE TRIGGER IF NOT EXISTS annotations_ad AFTER DELETE ON annotations BEGIN
    INSERT INTO annotations_fts(annotations_fts, rowid, token) VALUES('delete', old.id, old.token);
END;

CREATE TRIGGER IF NOT EXISTS annotations_au AFTER UPDATE ON annotations BEGIN
    INSERT INTO annotations_fts(annotations_fts, rowid, token) VALUES('delete', old.id, old.token);
    INSERT INTO annotations_fts(rowid, token) VALUES (new.id, new.token);
END;
"""


# ── Phase B4: Aspect Inverted Index ──────────────────────────────────────────

class AspectInvertedIndex:
    """In-memory inverted index for O(1) aspect lookup + set intersection.

    Keyed by (aspect_name, aspect_hash) -> set of summary_ids.
    Loaded from aspect_index table at startup. Updated on each ingest.
    Core of Logan's triangulation principle: find docs where >= N aspects converge.
    """

    ASPECT_NAMES = ("sem_hash", "source_hash", "agent_hash", "intent_hash", "project_hash")

    def __init__(self):
        self._index: dict[tuple[str, int], set[int]] = {}
        self._doc_aspects: dict[int, dict[str, int]] = {}

    def load_from_db(self, db: sqlite3.Connection):
        """Bulk load from aspect_index table."""
        rows = db.execute(
            "SELECT summary_id, sem_hash, source_hash, agent_hash, intent_hash, project_hash "
            "FROM aspect_index"
        ).fetchall()
        for row in rows:
            sid = row["summary_id"]
            aspects = {name: row[name] for name in self.ASPECT_NAMES}
            self._doc_aspects[sid] = aspects
            for name in self.ASPECT_NAMES:
                key = (name, row[name])
                if key not in self._index:
                    self._index[key] = set()
                self._index[key].add(sid)
        logger.info(f"AspectInvertedIndex loaded: {len(self._doc_aspects)} docs, "
                     f"{len(self._index)} index entries")

    def add(self, summary_id: int, aspects: dict[str, int]):
        """Add a document's aspects to the index. Called after DB insert."""
        self._doc_aspects[summary_id] = aspects
        for name in self.ASPECT_NAMES:
            key = (name, aspects[name])
            if key not in self._index:
                self._index[key] = set()
            self._index[key].add(summary_id)

    def remove(self, summary_id: int):
        """Remove a document from the index. Called on delete."""
        aspects = self._doc_aspects.pop(summary_id, None)
        if aspects:
            for name in self.ASPECT_NAMES:
                key = (name, aspects[name])
                if key in self._index:
                    self._index[key].discard(summary_id)
                    if not self._index[key]:
                        del self._index[key]

    def triangulate(self, query_aspects: dict[str, int],
                    min_convergence: int = 2) -> list[tuple[int, int]]:
        """Find documents where >= min_convergence aspects match.

        Returns list of (summary_id, match_count) sorted by match_count desc.
        This is the core triangulation operation — Logan's insight formalized.
        """
        from collections import Counter
        convergence: Counter[int] = Counter()
        for name in self.ASPECT_NAMES:
            h = query_aspects.get(name)
            if h is None:
                continue
            key = (name, h)
            for sid in self._index.get(key, set()):
                convergence[sid] += 1
        return [(sid, count) for sid, count in convergence.most_common()
                if count >= min_convergence]

    def dedup_check(self, new_aspects: dict[str, int],
                    threshold: int = 3) -> int | None:
        """Check if a new document is a semantic duplicate.

        Returns the summary_id of the best match, or None.
        Uses higher threshold (3 of 5) than search (2 of 5).
        """
        matches = self.triangulate(new_aspects, min_convergence=threshold)
        return matches[0][0] if matches else None

    @property
    def size(self) -> int:
        return len(self._doc_aspects)


# ── Phase B4.3: Bloom Filter + Aspect Bloom Cascade ─────────────────────────

class BloomFilter:
    """Minimal Bloom filter using bytearray + mmh3.

    Zero external deps beyond mmh3 (already installed).
    Uses k independent hash functions derived from mmh3 with different seeds.
    """

    def __init__(self, capacity: int = 200000, error_rate: float = 0.01):
        self._capacity = capacity
        self._error_rate = error_rate
        # Optimal bit count: m = -n*ln(p) / (ln(2)^2)
        self._num_bits = max(64, int(-capacity * math.log(error_rate) / (math.log(2) ** 2)))
        # Optimal hash count: k = (m/n) * ln(2)
        self._num_hashes = max(1, int((self._num_bits / capacity) * math.log(2)))
        self._bits = bytearray(self._num_bits // 8 + 1)
        self._count = 0

    def _get_bit_positions(self, key: str) -> list[int]:
        """Generate k bit positions for a key using mmh3 with different seeds."""
        import mmh3
        positions = []
        for i in range(self._num_hashes):
            h = mmh3.hash(key, seed=i) % self._num_bits
            if h < 0:
                h += self._num_bits
            positions.append(h)
        return positions

    def add(self, key: str):
        """Add a key to the filter."""
        for pos in self._get_bit_positions(key):
            byte_idx = pos >> 3
            bit_idx = pos & 7
            self._bits[byte_idx] |= (1 << bit_idx)
        self._count += 1

    def __contains__(self, key: str) -> bool:
        """Check if a key might be in the filter."""
        for pos in self._get_bit_positions(key):
            byte_idx = pos >> 3
            bit_idx = pos & 7
            if not (self._bits[byte_idx] & (1 << bit_idx)):
                return False
        return True

    @property
    def count(self) -> int:
        return self._count

    @property
    def memory_bytes(self) -> int:
        return len(self._bits)


class AspectBloomCascade:
    """3-level Bloom filter cascade for fast dedup pre-screening.

    Short-circuits at the earliest level where the triad is NOT found.
    Level 3 hit -> high probability of duplicate -> proceed to full convergence check.
    Level 1 miss -> certainly new content -> skip all further dedup checks.

    | Level  | Key Composition            | FPR    | Purpose                     |
    |--------|---------------------------|--------|-----------------------------|
    | Coarse | sem_hash only              | ~1%    | "Seen this TOPIC before?"   |
    | Medium | sem_hash + source_hash     | ~0.1%  | "Same topic from same src?" |
    | Fine   | sem + source + agent       | ~0.01% | "Same topic, src, agent?"   |
    """

    def __init__(self, capacity: int = 200000):
        self.coarse = BloomFilter(capacity=capacity, error_rate=0.01)
        self.medium = BloomFilter(capacity=capacity, error_rate=0.001)
        self.fine = BloomFilter(capacity=capacity, error_rate=0.0001)

    def check_and_register(self, aspects: dict[str, int]) -> str:
        """Check cascade and register the aspect hashes.

        Returns:
            'new'        - Level 1 miss. Certainly new content.
            'topic_seen' - Level 1 hit, Level 2 miss. Same topic, different source path.
            'likely_dup' - Level 2+ hit. Probably a duplicate or near-duplicate.
        """
        coarse_key = str(aspects["sem_hash"])
        medium_key = f"{aspects['sem_hash']}:{aspects['source_hash']}"
        fine_key = f"{aspects['sem_hash']}:{aspects['source_hash']}:{aspects['agent_hash']}"

        if coarse_key not in self.coarse:
            self.coarse.add(coarse_key)
            self.medium.add(medium_key)
            self.fine.add(fine_key)
            return "new"

        if medium_key not in self.medium:
            self.medium.add(medium_key)
            self.fine.add(fine_key)
            return "topic_seen"

        if fine_key not in self.fine:
            self.fine.add(fine_key)
            return "topic_seen"

        return "likely_dup"

    def register_only(self, aspects: dict[str, int]):
        """Register aspects in all 3 levels without checking (for bulk rebuild)."""
        self.coarse.add(str(aspects["sem_hash"]))
        self.medium.add(f"{aspects['sem_hash']}:{aspects['source_hash']}")
        self.fine.add(f"{aspects['sem_hash']}:{aspects['source_hash']}:{aspects['agent_hash']}")

    @property
    def memory_bytes(self) -> int:
        return self.coarse.memory_bytes + self.medium.memory_bytes + self.fine.memory_bytes


# ── Connection Pool (OPT-009) ────────────────────────────────────────────────
class ConnectionPool:
    """Queue-based SQLite connection pool for parallel scoring operations."""

    _PRAGMAS = (
        "PRAGMA journal_mode=WAL",
        "PRAGMA synchronous=NORMAL",
        "PRAGMA busy_timeout=10000",
        "PRAGMA cache_size=-512000",
        "PRAGMA mmap_size=2147483648",
        "PRAGMA temp_store=MEMORY",
    )
    # Lite mode PRAGMAs: conservative memory for Railway/CPU-only deployment
    _PRAGMAS_LITE = (
        "PRAGMA journal_mode=WAL",
        "PRAGMA synchronous=NORMAL",
        "PRAGMA busy_timeout=10000",
        "PRAGMA cache_size=-64000",
        "PRAGMA mmap_size=268435456",
        "PRAGMA temp_store=MEMORY",
        "PRAGMA query_only=ON",
    )

    def __init__(self, db_path: str, size: int = 4):
        effective_size = size if _GPU_ENABLED else min(size, 4)
        pragmas = self._PRAGMAS if _GPU_ENABLED else self._PRAGMAS_LITE
        self._pool: queue.Queue[sqlite3.Connection] = queue.Queue(maxsize=effective_size)
        self._size = effective_size
        for _ in range(effective_size):
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            for pragma in pragmas:
                conn.execute(pragma)
            self._pool.put(conn)

    def get(self, timeout: float = 30.0) -> sqlite3.Connection:
        """Borrow a connection from the pool (caller MUST call put() when done)."""
        return self._pool.get(timeout=timeout)

    def put(self, conn: sqlite3.Connection) -> None:
        """Return a connection to the pool."""
        self._pool.put(conn)

    def close_all(self) -> None:
        """Drain and close every connection in the pool."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except queue.Empty:
                break


# ── Unified Memory Class ────────────────────────────────────────────────────
class UnifiedMemory:
    """3-tier linked memory: Annotations → Summaries → Verbatim with FTS5."""

    def __init__(self, db_path: str = DB_PATH):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA busy_timeout=10000")   # 10s retry on lock contention (multi-process safe)
        if _GPU_ENABLED:
            self.db.execute("PRAGMA cache_size=-512000")  # 512MB page cache (OPT-007: was 64MB)
            self.db.execute("PRAGMA mmap_size=2147483648") # 2GB memory-mapped I/O (OPT-007: was 256MB)
        else:
            self.db.execute("PRAGMA cache_size=-64000")   # 64MB page cache (Railway lite mode)
            self.db.execute("PRAGMA mmap_size=268435456") # 256MB mmap (Railway lite mode)
            self.db.execute("PRAGMA query_only=ON")       # Read-only on Railway
        self.db.execute("PRAGMA temp_store=MEMORY")    # Temp tables in RAM
        if _GPU_ENABLED:
            self.db.execute("PRAGMA optimize")          # OPT-011: Re-analyze table statistics (skip on read-only)
        self._write_lock = threading.Lock()  # B10: serialize writes
        _pool_size = 16 if _GPU_ENABLED else 4  # OPT-009: smaller pool on Railway
        self._pool = ConnectionPool(db_path, size=_pool_size)
        if _GPU_ENABLED:
            self._init_schema()
        else:
            logger.info("Railway lite mode: skipping schema init (query_only=ON)")

        # Phase B1-Step1: Context result cache (TTL-based)
        self._context_cache: dict[tuple, tuple[float, str]] = {}
        # Phase 5: Disclosure layers cache (same keys as context cache)
        self._context_layers_cache: dict[tuple, tuple[float, list]] = {}
        # Phase 5: Cache hit/miss counters for /health stats
        self._context_cache_hits: int = 0
        self._context_cache_misses: int = 0
        _ctx_cfg = CONFIG.get("context", {})
        self._context_cache_ttl: float = float(_ctx_cfg.get("cache_ttl_seconds", 60))
        self._context_cache_max: int = int(_ctx_cfg.get("max_cache_entries", 256))

        # Phase 6: Agent-Aware Query Enhancement
        self._agent_topic_priors: dict[str, list[tuple[str, int]]] = {}  # agent -> [(topic, count), ...]
        self._agent_topic_priors_ts: float = 0.0  # last refresh timestamp
        self._agent_topic_priors_ttl: float = 300.0  # refresh every 5 min
        self._agent_query_sequences: dict[str, list[str]] = {}  # agent -> [last N topics]
        self._agent_query_seq_max: int = 20  # max sequence length per agent

        # GPU-AM tier (RC)
        self._gpu = uaimc_gpu.get_instance(db_path)

        # Phase B4.4: GPU-Triad Index for parallel triangulation
        _gpu_triad_enabled = SCORING.get("gpu_triad_enabled", True)
        if _gpu_triad_enabled:
            self._gpu_triad = uaimc_gpu.get_triad_instance(db_path)
            self._gpu_triad.load_from_db(self.db)
        else:
            self._gpu_triad = None

        # Phase B4.2: Aspect Inverted Index for triangulation + convergence (CPU fallback)
        self._aspect_index = AspectInvertedIndex()
        self._aspect_index.load_from_db(self.db)
        self._current_tri_candidates = None
        self._current_tri_scores = None
        self._tri_source = "none"  # B4.5: track triangulation source for logging
        self._tls = threading.local()  # OPT-009: per-thread query state
        self._gpu_lock = threading.Lock()  # OPT-009: serialize GPU/triad tensor access only

        # Phase B4.3: Bloom Cascade for dedup pre-screening
        _bloom_enabled = SCORING.get("bloom_cascade_enabled", True)
        _bloom_capacity = int(SCORING.get("bloom_capacity", 200000))
        if _bloom_enabled:
            self._bloom_cascade = AspectBloomCascade(capacity=_bloom_capacity)
        else:
            self._bloom_cascade = None
        self._bloom_path = Path(db_path).parent / "bloom_cascade.bin"
        if self._bloom_cascade:
            self._load_bloom()

        _gt_status = f"ON ({self._gpu_triad.count} docs)" if self._gpu_triad and self._gpu_triad.enabled else "OFF"
        _bl_status = f"{self._bloom_cascade.memory_bytes // 1024}KB" if self._bloom_cascade else "OFF"
        logger.info(f"UnifiedMemory initialized at {db_path} "
                     f"(GPU-AM: {self._gpu.device_name}, "
                     f"GPU-Triad: {_gt_status}, "
                     f"AspectIndex: {self._aspect_index.size} docs, "
                     f"Bloom: {_bl_status})")

    def _init_schema(self):
        self.db.executescript(_SCHEMA)
        self.db.commit()
        try:
            self.db.executescript(_FTS_SCHEMA)
            self.db.commit()
        except sqlite3.OperationalError as e:
            if "already exists" not in str(e).lower():
                logger.warning(f"FTS5 schema issue: {e}")
        self._run_dedup_migration()
        self._run_scoring_migration()
        self._run_phase_a2_migration()
        self._run_phase_a3_migration()
        self._run_phase_a4_migration()
        self._run_phase_b4_migration()
        self._run_sprint5_migration()

    def _run_dedup_migration(self):
        """Dedup Gate migration: add content_hash index + summary_hash column."""
        cursor = self.db.cursor()
        # Layer 1: Index on verbatim.content_hash (was missing — full table scan)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_verbatim_content_hash ON verbatim(content_hash)")
        # Layer 2: summary_hash column for summary-level dedup
        try:
            cursor.execute("ALTER TABLE summaries ADD COLUMN summary_hash TEXT")
            logger.info("Dedup Gate: added summary_hash column to summaries table")
        except sqlite3.OperationalError:
            pass  # Column already exists
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_summaries_summary_hash ON summaries(summary_hash)")
        self.db.commit()
        logger.info("Dedup Gate: migration complete (content_hash index + summary_hash)")

    def _run_scoring_migration(self):
        """Phase A1: Add generated_at column + coverage index for scoring pipeline."""
        cursor = self.db.cursor()
        try:
            cursor.execute("ALTER TABLE summaries ADD COLUMN generated_at TEXT DEFAULT NULL")
            logger.info("Scoring migration: added generated_at column to summaries")
        except sqlite3.OperationalError:
            pass  # Column already exists
        # B-001: Index on summary_id for fast coverage ratio lookups
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_annotations_summary ON annotations(summary_id)")
            logger.info("Scoring migration: created idx_annotations_summary index")
        except sqlite3.OperationalError:
            pass  # Index already exists
        self.db.commit()
        logger.info("Scoring migration: Phase A1 schema ready")

    def _run_phase_a2_migration(self):
        """Phase A2: Create selection_log (B-010) and topic_heat (B-011) tables."""
        cursor = self.db.cursor()

        # B-010: selection_log — tracks which documents are selected/promoted
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS selection_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                query_hash   TEXT NOT NULL,
                summary_id   INTEGER NOT NULL,
                signal_type  TEXT NOT NULL,
                agent        TEXT DEFAULT '',
                query_text   TEXT DEFAULT '',
                position     INTEGER DEFAULT 0,
                created_at   TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (summary_id) REFERENCES summaries(id)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_selection_summary ON selection_log(summary_id, signal_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_selection_query ON selection_log(query_hash, signal_type)")

        # B-011: topic_heat — rolling topic temperature tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS topic_heat (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_token  TEXT NOT NULL,
                agent        TEXT DEFAULT '*',
                heat_type    TEXT NOT NULL,
                weight       REAL DEFAULT 1.0,
                created_at   TEXT DEFAULT (datetime('now'))
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_heat_topic ON topic_heat(topic_token, agent, created_at)")

        self.db.commit()
        logger.info("Phase A2 migration: selection_log + topic_heat tables ready")

    def _run_phase_a3_migration(self):
        """Phase A3: Create document_links (B-012) table with B-013 columns."""
        cursor = self.db.cursor()

        # B-012: document_links — graph connections between documents
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS document_links (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id          INTEGER NOT NULL,
                target_id          INTEGER NOT NULL,
                link_type          TEXT NOT NULL,
                strength           REAL DEFAULT 1.0,
                discovered_by      TEXT DEFAULT 'system',
                explored           INTEGER DEFAULT 0,
                created_at         TEXT DEFAULT (datetime('now')),
                link_state         TEXT DEFAULT 'active',
                rejection_count    INTEGER DEFAULT 0,
                last_rejected_at   TEXT DEFAULT NULL,
                last_explored_at   TEXT DEFAULT NULL,
                presentation_count INTEGER DEFAULT 0,
                ignore_count       INTEGER DEFAULT 0,
                FOREIGN KEY (source_id) REFERENCES summaries(id),
                FOREIGN KEY (target_id) REFERENCES summaries(id)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_source ON document_links(source_id, link_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_target ON document_links(target_id, link_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_explored ON document_links(explored)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_state ON document_links(link_state)")
        # OPT-003: Compound covering indexes for BFS queries (source_id+link_state → target_id+strength)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_source_state ON document_links(source_id, link_state, target_id, strength)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_target_state ON document_links(target_id, link_state, source_id, strength)")

        self.db.commit()
        logger.info("Phase A3 migration: document_links table ready")

    def _run_phase_a4_migration(self):
        """Phase A4: Create presentation_log (B-013) table for progressive disclosure."""
        cursor = self.db.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS presentation_log (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id       TEXT NOT NULL,
                query_hash       TEXT NOT NULL,
                summary_id       INTEGER NOT NULL,
                layer            INTEGER NOT NULL,
                position         INTEGER DEFAULT 0,
                response         TEXT NOT NULL,
                cohort_ids       TEXT DEFAULT '',
                agent            TEXT DEFAULT '',
                created_at       TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (summary_id) REFERENCES summaries(id)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pres_session ON presentation_log(session_id, layer)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pres_summary ON presentation_log(summary_id, response)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pres_query   ON presentation_log(query_hash, response)")

        self.db.commit()
        logger.info("Phase A4 migration: presentation_log table ready")

    def _run_phase_b4_migration(self):
        """Phase B4: Create aspect_index table for triangulation fingerprinting."""
        cursor = self.db.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aspect_index (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                summary_id   INTEGER NOT NULL UNIQUE,
                sem_hash     INTEGER NOT NULL,
                source_hash  INTEGER NOT NULL,
                agent_hash   INTEGER NOT NULL,
                intent_hash  INTEGER NOT NULL,
                project_hash INTEGER NOT NULL,
                created_at   TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (summary_id) REFERENCES summaries(id) ON DELETE CASCADE
            )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_aspect_sem     ON aspect_index(sem_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_aspect_source  ON aspect_index(source_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_aspect_agent   ON aspect_index(agent_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_aspect_intent  ON aspect_index(intent_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_aspect_project ON aspect_index(project_hash)")

        self.db.commit()
        logger.info("Phase B4 migration: aspect_index table ready")

    def _run_sprint5_migration(self):
        """Sprint 5 / OPT-017: Create top_neighbors table for pre-computed graph lookups."""
        cursor = self.db.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS top_neighbors (
                summary_id   INTEGER NOT NULL,
                neighbor_id  INTEGER NOT NULL,
                hops         INTEGER NOT NULL,
                strength     REAL NOT NULL,
                is_hidden    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (summary_id, neighbor_id),
                FOREIGN KEY (summary_id) REFERENCES summaries(id) ON DELETE CASCADE,
                FOREIGN KEY (neighbor_id) REFERENCES summaries(id) ON DELETE CASCADE
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_topn_summary ON top_neighbors(summary_id)")
        self.db.commit()
        logger.info("Sprint 5 migration: top_neighbors table ready")

    def _compute_top_neighbors(self, summary_id: int, conn: sqlite3.Connection | None = None, max_hops: int = 2, top_n: int = 20):
        """OPT-017: Pre-compute top-N neighbors via BFS and store in top_neighbors table.

        Called at ingest time after auto-links are created. Replaces live BFS at query time.
        """
        db = conn or self.db
        connected: dict[int, tuple[int, float, bool]] = {}
        frontier = {summary_id}
        visited = {summary_id}
        for hop in range(1, max_hops + 1):
            if not frontier:
                break
            placeholders = ",".join("?" * len(frontier))
            frontier_list = list(frontier)
            rows = db.execute(
                f"""SELECT target_id, strength, explored FROM document_links
                    WHERE source_id IN ({placeholders}) AND link_state = 'active'
                    ORDER BY strength DESC LIMIT 100""",
                frontier_list
            ).fetchall()
            rows += db.execute(
                f"""SELECT source_id AS target_id, strength, explored FROM document_links
                    WHERE target_id IN ({placeholders}) AND link_state = 'active'
                    ORDER BY strength DESC LIMIT 100""",
                frontier_list
            ).fetchall()
            next_frontier: set[int] = set()
            for row in rows:
                tid = row["target_id"]
                strength = row["strength"]
                is_hidden = (row["explored"] == 0)
                if tid not in visited:
                    existing = connected.get(tid)
                    if not existing or hop < existing[0]:
                        connected[tid] = (hop, strength, is_hidden)
                    next_frontier.add(tid)
                    visited.add(tid)
            frontier = next_frontier

        # Sort by strength descending, keep top N
        top = sorted(connected.items(), key=lambda x: x[1][1], reverse=True)[:top_n]

        # Replace existing neighbors for this summary
        db.execute("DELETE FROM top_neighbors WHERE summary_id = ?", (summary_id,))
        if top:
            db.executemany(
                "INSERT INTO top_neighbors (summary_id, neighbor_id, hops, strength, is_hidden) VALUES (?, ?, ?, ?, ?)",
                [(summary_id, nid, hops, strength, int(is_hidden)) for nid, (hops, strength, is_hidden) in top]
            )

    # ── Phase B4.3: Bloom Persistence ────────────────────────────────────

    def _load_bloom(self):
        """Load Bloom cascade from disk, or rebuild from aspect_index table."""
        if self._bloom_path.exists():
            try:
                with open(self._bloom_path, "rb") as f:
                    data = pickle.load(f)
                self._bloom_cascade.coarse = data["coarse"]
                self._bloom_cascade.medium = data["medium"]
                self._bloom_cascade.fine = data["fine"]
                logger.info(f"Bloom cascade loaded from {self._bloom_path}")
                return
            except Exception as e:
                logger.warning(f"Bloom load failed, rebuilding: {e}")
        self._rebuild_bloom_from_db()

    def _rebuild_bloom_from_db(self):
        """Rebuild Bloom cascade from aspect_index table."""
        if not self._bloom_cascade:
            return
        rows = self.db.execute(
            "SELECT sem_hash, source_hash, agent_hash FROM aspect_index"
        ).fetchall()
        for row in rows:
            aspects = {"sem_hash": row["sem_hash"], "source_hash": row["source_hash"],
                       "agent_hash": row["agent_hash"]}
            self._bloom_cascade.register_only(aspects)
        logger.info(f"Bloom cascade rebuilt from DB: {len(rows)} entries")

    def _save_bloom(self):
        """Persist Bloom cascade to disk."""
        if not self._bloom_cascade:
            return
        try:
            self._bloom_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._bloom_path, "wb") as f:
                pickle.dump({
                    "coarse": self._bloom_cascade.coarse,
                    "medium": self._bloom_cascade.medium,
                    "fine": self._bloom_cascade.fine,
                }, f)
        except Exception as e:
            logger.warning(f"Bloom save failed: {e}")

    # ── Phase B4.3: Convergence-Scored Ingestion ─────────────────────────

    CONVERGENCE_THRESHOLDS = {
        "dup": 4,     # 4 of 5 aspects match → almost certainly duplicate → SKIP
        "merge": 3,   # 3 of 5 aspects match → same topic, slightly different → MERGE
    }

    def _convergence_ingest_decision(self, new_aspects: dict[str, int]) -> tuple[str, int | None]:
        """Determine SKIP, MERGE, or INSERT based on aspect convergence.

        Returns:
            ("skip", existing_sid)  — Near-exact duplicate. Don't store.
            ("merge", existing_sid) — Similar enough to enrich existing. Merge annotations.
            ("insert", None)        — Genuinely new content. Store normally.
        """
        matches = self._aspect_index.triangulate(new_aspects, min_convergence=3)
        if not matches:
            return ("insert", None)

        best_sid, best_count = matches[0]
        thresholds = SCORING.get("convergence_thresholds", self.CONVERGENCE_THRESHOLDS)
        dup_threshold = int(thresholds.get("dup", 4))
        merge_threshold = int(thresholds.get("merge", 3))

        if best_count >= dup_threshold:
            return ("skip", best_sid)
        elif best_count >= merge_threshold:
            return ("merge", best_sid)
        else:
            return ("insert", None)

    def _merge_into_existing(self, existing_sid: int, new_annotations: list,
                             new_source: str):
        """Merge new annotations into an existing summary.

        Prevents triplication while preserving genuinely new annotation tokens.
        """
        existing_hashes = set(
            row["token_hash"] for row in self.db.execute(
                "SELECT token_hash FROM annotations WHERE summary_id = ?",
                (existing_sid,)
            ).fetchall()
        )

        new_annos = [(a.token, a.token_hash, a.weight, existing_sid, new_source)
                     for a in new_annotations if a.token_hash not in existing_hashes]

        if new_annos:
            self.db.executemany(
                "INSERT INTO annotations (token, token_hash, weight, summary_id, source) "
                "VALUES (?,?,?,?,?)",
                new_annos,
            )
            self.db.commit()

            if self._gpu and self._gpu.enabled:
                gpu_entries = [(token_hash, existing_sid, weight)
                              for _, token_hash, weight, _, _ in new_annos]
                self._gpu.add_batch(gpu_entries)

        logger.info(f"B4.3: Merged {len(new_annos)} new annotations into sid={existing_sid}")
        return existing_sid

    # ── Phase A1: Generation Date Extraction ───────────────────────────────

    def _extract_generation_date(self, text: str, metadata: dict | None, source: str) -> str | None:
        """Best-effort extraction of content generation date. Returns ISO string or None."""
        # 1. Explicit metadata
        if metadata:
            for key in ("generated_at", "source_date", "date", "created"):
                val = metadata.get(key)
                if val and isinstance(val, str):
                    try:
                        datetime.strptime(val[:10], "%Y-%m-%d")
                        return val[:19]
                    except ValueError:
                        continue

        # 2. Filename date
        if source:
            m = re.search(r"(\d{4}[-_]\d{2}[-_]\d{2})", source)
            if m:
                date_str = m.group(1).replace("_", "-")
                return f"{date_str} 00:00:00"

        # 3. Content header (first 500 chars — don't scan entire document)
        header = text[:500]
        for pattern, fmt in _DATE_PATTERNS:
            m = pattern.search(header)
            if m and fmt:
                try:
                    dt = datetime.strptime(m.group(1).replace("_", "-"), fmt)
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue

        return None  # Fall back to created_at

    # ── OPT-005: Batch Prefetch Annotations (Sprint 3) ──────────────────

    def _batch_prefetch_annotations(self, sids: list[int], conn: sqlite3.Connection | None = None) -> dict:
        """Fetch ALL annotation data for given summary_ids in ONE query.
        Returns a dict usable by heat, BM25, coverage, and length_norm methods."""
        if not sids:
            return {"token_map": {}, "hash_map": {}, "tf_map": {}, "dl_map": {}}

        db = conn or self.db
        placeholders = ",".join("?" * len(sids))
        rows = db.execute(
            f"SELECT summary_id, token, token_hash FROM annotations WHERE summary_id IN ({placeholders})",
            sids
        ).fetchall()

        token_map: dict[int, set[str]] = {}  # sid → set(token_strings) for heat
        hash_map: dict[int, set[int]] = {}   # sid → set(token_hashes) for coverage
        tf_map: dict[int, dict[int, int]] = {}  # sid → {hash: count} for BM25
        dl_map: dict[int, int] = {}          # sid → annotation_count for BM25/length_norm

        for row in rows:
            sid = row["summary_id"]
            token_map.setdefault(sid, set()).add(row["token"].lower())
            hash_map.setdefault(sid, set()).add(row["token_hash"])
            tf_map.setdefault(sid, {})
            h = row["token_hash"]
            tf_map[sid][h] = tf_map[sid].get(h, 0) + 1
            dl_map[sid] = dl_map.get(sid, 0) + 1

        return {"token_map": token_map, "hash_map": hash_map, "tf_map": tf_map, "dl_map": dl_map}

    def _batch_prefetch_selection(self, sids: list[int], conn: sqlite3.Connection | None = None) -> dict[int, dict[str, int]]:
        """Prefetch selection_log data for given summary_ids. Returns {sid: {signal_type: count}}."""
        if not sids:
            return {}
        db = conn or self.db
        placeholders = ",".join("?" * len(sids))
        rows = db.execute(
            f"""SELECT summary_id, signal_type, COUNT(*) as cnt
                FROM selection_log WHERE summary_id IN ({placeholders})
                GROUP BY summary_id, signal_type""",
            sids
        ).fetchall()
        sel_map: dict[int, dict[str, int]] = {}
        for row in rows:
            sel_map.setdefault(row["summary_id"], {})[row["signal_type"]] = row["cnt"]
        return sel_map

    def _batch_prefetch_negation(self, sids: list[int], conn: sqlite3.Connection | None = None) -> dict[int, list[str]]:
        """Prefetch rejection history from presentation_log. Returns {sid: [created_at, ...]}."""
        if not sids:
            return {}
        db = conn or self.db
        placeholders = ",".join("?" * len(sids))
        rows = db.execute(
            f"""SELECT summary_id, created_at FROM presentation_log
                WHERE summary_id IN ({placeholders}) AND response = 'reject'
                AND created_at > datetime('now', '-180 days')""",
            sids
        ).fetchall()
        rej_map: dict[int, list[str]] = {}
        for row in rows:
            rej_map.setdefault(row["summary_id"], []).append(row["created_at"])
        return rej_map

    def _batch_prefetch_graph(self, seed_ids: list[int], max_hops: int = 2,
                              conn: sqlite3.Connection | None = None) -> dict[int, tuple[int, float, bool]]:
        """OPT-017: Fetch pre-computed neighbors from top_neighbors table.

        Falls back to live BFS if top_neighbors is empty for a seed (pre-backfill).
        Returns {sid: (min_hops, strength, is_hidden)}.
        """
        if not seed_ids:
            return {}
        db = conn or self.db
        connected: dict[int, tuple[int, float, bool]] = {}

        # Primary path: indexed lookup from pre-computed table
        placeholders = ",".join("?" * len(seed_ids))
        rows = db.execute(
            f"""SELECT neighbor_id, hops, strength, is_hidden
                FROM top_neighbors WHERE summary_id IN ({placeholders})""",
            list(seed_ids)
        ).fetchall()

        if rows:
            for row in rows:
                nid = row["neighbor_id"]
                hops = row["hops"]
                strength = row["strength"]
                is_hidden = bool(row["is_hidden"])
                existing = connected.get(nid)
                if not existing or hops < existing[0]:
                    connected[nid] = (hops, strength, is_hidden)
            return connected

        # Fallback: live BFS for seeds without pre-computed neighbors
        # OPT-021: Links are bidirectional (both A→B and B→A stored),
        # so we only need source_id lookup — no UNION/reverse query needed.
        frontier = set(seed_ids)
        visited = set(seed_ids)
        for hop in range(1, max_hops + 1):
            if not frontier:
                break
            ph = ",".join("?" * len(frontier))
            frontier_list = list(frontier)
            bfs_rows = db.execute(
                f"""SELECT target_id, strength, explored FROM document_links
                    WHERE source_id IN ({ph}) AND link_state = 'active'
                    ORDER BY strength DESC LIMIT 100""",
                frontier_list
            ).fetchall()
            next_frontier: set[int] = set()
            for row in bfs_rows:
                tid = row["target_id"]
                strength = row["strength"]
                is_hidden = (row["explored"] == 0)
                if tid not in visited:
                    existing = connected.get(tid)
                    if not existing or hop < existing[0]:
                        connected[tid] = (hop, strength, is_hidden)
                    next_frontier.add(tid)
                    visited.add(tid)
            frontier = next_frontier
        return connected

    # ── Phase A1: Coverage Ratio Scoring (B-001) ──────────────────────────

    def _apply_coverage_ratio(self, results: list[dict], query_keywords: list[str],
                              prefetched: dict | None = None) -> list[dict]:
        """B-001: Boost documents matching MORE query terms. Multi-term queries reward breadth.

        Uses prefetched annotation data when available, falling back to direct query.
        """
        if not query_keywords or not results:
            return results

        query_hashes = set(uaimc_anno.fnv1a_hash(kw.lower().strip()) for kw in query_keywords if kw.strip())
        if not query_hashes:
            return results

        coverage_floor = SCORING.get("coverage_floor", 0.5)

        # Use prefetched hash_map if available (OPT-005), else fallback to direct query
        if prefetched and "hash_map" in prefetched:
            doc_hash_map = prefetched["hash_map"]
        else:
            sids = [r.get("summary_id") for r in results if r.get("summary_id")]
            if not sids:
                return results
            placeholders = ",".join("?" * len(sids))
            rows = self.db.execute(
                f"SELECT summary_id, token_hash FROM annotations WHERE summary_id IN ({placeholders})",
                sids
            ).fetchall()
            doc_hash_map: dict[int, set[int]] = {}
            for row in rows:
                doc_hash_map.setdefault(row["summary_id"], set()).add(row["token_hash"])

        n_terms = len(query_hashes)

        for r in results:
            sid = r.get("summary_id")
            if not sid:
                continue
            doc_hashes = doc_hash_map.get(sid, set())

            matched = len(query_hashes & doc_hashes)
            coverage = matched / n_terms

            # Exponential coverage penalty: multi-term queries strongly
            # penalise results lacking most query terms.
            # coverage_floor sets the minimum multiplier (for 0-match docs).
            # Power curve (coverage ** exponent) means:
            #   1/3 match → 0.333^1.5 ≈ 0.19  ×(1-floor) + floor
            #   2/3 match → 0.667^1.5 ≈ 0.54  ×(1-floor) + floor
            #   3/3 match → 1.0                = 1.0
            exponent = 1.0 if n_terms <= 1 else 1.5
            curved = coverage ** exponent
            multiplier = coverage_floor + (1.0 - coverage_floor) * curved
            r["relevance_score"] = r.get("relevance_score", 0) * multiplier
            r["coverage_ratio"] = round(coverage, 3)
            r["matched_query_terms"] = matched
            r["total_query_terms"] = n_terms

        return results

    # ── Phase B3 Step 8: Query-Time BM25 Reweighting ────────────────────

    # Class-level IDF cache: {token_hash: (idf_value, timestamp)}
    _idf_cache: dict[int, tuple[float, float]] = {}
    _corpus_stats_cache: tuple[int, float, float] | None = None  # (N, avgdl, timestamp)
    _IDF_CACHE_TTL = 3600.0  # 1 hour
    _CORPUS_STATS_TTL = 3600.0  # 1 hour

    def _get_corpus_stats(self) -> tuple[int, float]:
        """Return (total_docs, avg_doc_length). Cached for 1 hour."""
        now = time.time()
        if self._corpus_stats_cache and (now - self._corpus_stats_cache[2]) < self._CORPUS_STATS_TTL:
            return self._corpus_stats_cache[0], self._corpus_stats_cache[1]

        row = self.db.execute("SELECT COUNT(*) as n FROM summaries").fetchone()
        N = row["n"] if row else 1

        row2 = self.db.execute("SELECT AVG(cnt) as avg FROM (SELECT COUNT(*) as cnt FROM annotations GROUP BY summary_id)").fetchone()
        avgdl = row2["avg"] if row2 and row2["avg"] else 1.0

        self._corpus_stats_cache = (N, avgdl, now)
        return N, avgdl

    def _get_idf(self, token_hash: int, N: int) -> float:
        """Return IDF for a token hash. Cached for 1 hour."""
        now = time.time()
        cached = self._idf_cache.get(token_hash)
        if cached and (now - cached[1]) < self._IDF_CACHE_TTL:
            return cached[0]

        row = self.db.execute(
            "SELECT COUNT(DISTINCT summary_id) as df FROM annotations WHERE token_hash = ?",
            (token_hash,)
        ).fetchone()
        df = row["df"] if row else 0

        # Standard BM25 IDF: ln((N - df + 0.5) / (df + 0.5) + 1)
        idf = math.log((N - df + 0.5) / (df + 0.5) + 1.0)
        self._idf_cache[token_hash] = (idf, now)
        return idf

    def _apply_bm25_scoring(self, results: list[dict], query_keywords: list[str],
                             prefetched: dict | None = None) -> list[dict]:
        """B3-Step8: Query-time BM25 reweighting.
        Computes fresh BM25 scores and blends with existing relevance_score.
        Formula: BM25(D,Q) = sum_t IDF(t) * (f(t,D) * (k1+1)) / (f(t,D) + k1 * (1 - b + b * |D|/avgdl))
        Blend: final = (1 - bm25_blend) * existing + bm25_blend * bm25_score
        """
        if not results or not query_keywords:
            return results

        # Filename-style lookups (e.g., "01_MR.md") are usually high-precision
        # token hits already; skip BM25 math/DB work to reduce first-hit latency.
        kw_clean = [k.lower().strip() for k in query_keywords if k and k.strip()]
        if kw_clean and len(kw_clean) <= 2 and any(("." in k or "_" in k or "-" in k) for k in kw_clean):
            for r in results:
                r["bm25_blend"] = 0.0
            return results

        bm25_k1 = SCORING.get("bm25_k1", 1.5)
        bm25_b = SCORING.get("bm25_b", 0.75)
        bm25_blend = SCORING.get("bm25_blend", 0.4)

        if not SCORING.get("bm25_enabled", True):
            return results

        N, avgdl = self._get_corpus_stats()

        # Pre-compute query token hashes and IDF values
        query_hashes = []
        query_idfs = {}
        for kw in query_keywords:
            if kw.strip():
                h = uaimc_anno.fnv1a_hash(kw.lower().strip())
                query_hashes.append(h)
                query_idfs[h] = self._get_idf(h, N)

        if not query_hashes:
            return results

        # Use prefetched tf_map/dl_map if available (OPT-005), else fallback to direct queries
        sids = [r.get("summary_id") for r in results if r.get("summary_id")]
        if not sids:
            return results

        if prefetched and "tf_map" in prefetched and "dl_map" in prefetched:
            tf_map = prefetched["tf_map"]
            dl_map = prefetched["dl_map"]
        else:
            sid_placeholders = ",".join("?" * len(sids))
            hash_placeholders = ",".join("?" * len(query_hashes))
            rows = self.db.execute(
                f"SELECT summary_id, token_hash, COUNT(*) as tf FROM annotations "
                f"WHERE summary_id IN ({sid_placeholders}) AND token_hash IN ({hash_placeholders}) "
                f"GROUP BY summary_id, token_hash",
                sids + query_hashes
            ).fetchall()
            tf_map: dict[int, dict[int, int]] = {}
            for row in rows:
                tf_map.setdefault(row["summary_id"], {})[row["token_hash"]] = row["tf"]
            dl_rows = self.db.execute(
                f"SELECT summary_id, COUNT(*) as dl FROM annotations WHERE summary_id IN ({sid_placeholders}) GROUP BY summary_id",
                sids
            ).fetchall()
            dl_map = {row["summary_id"]: row["dl"] for row in dl_rows}

        for r in results:
            sid = r.get("summary_id")
            if not sid:
                continue

            doc_tf = tf_map.get(sid, {})
            dl = dl_map.get(sid, 1)

            bm25_score = 0.0
            for h in query_hashes:
                tf = doc_tf.get(h, 0)
                idf = query_idfs.get(h, 0)
                numerator = tf * (bm25_k1 + 1.0)
                denominator = tf + bm25_k1 * (1.0 - bm25_b + bm25_b * dl / avgdl)
                bm25_score += idf * (numerator / denominator) if denominator > 0 else 0

            # Blend: weighted combination of existing score and BM25
            existing = r.get("relevance_score", 0)
            r["relevance_score"] = (1.0 - bm25_blend) * existing + bm25_blend * bm25_score
            r["bm25_score"] = round(bm25_score, 4)
            r["bm25_blend"] = bm25_blend

        return results

    # ── Phase B4.2: Convergence Boost (Triangulation Scoring) ────────────

    def _apply_convergence_boost(self, results: list[dict]) -> list[dict]:
        """B4.2: Boost documents based on aspect convergence count.

        Documents matching more query aspects get multiplicatively boosted.
        2 of 5 = base match, 3 of 5 = strong match, 4+ of 5 = near-exact match.
        """
        _tri_scores = getattr(self._tls, 'tri_scores', None) or self._current_tri_scores
        if not _tri_scores:
            return results

        default_weights = {2: 1.0, 3: 1.3, 4: 1.7, 5: 2.2}
        raw_weights = SCORING.get("convergence_weights", default_weights)
        # Ensure int keys (JSON config may have string keys)
        convergence_weights = {int(k): v for k, v in raw_weights.items()}

        for r in results:
            sid = r.get("summary_id")
            if not sid:
                continue
            match_count = _tri_scores.get(sid, 0)
            boost = convergence_weights.get(match_count, 1.0)
            r["relevance_score"] = r.get("relevance_score", 0) * boost
            r["convergence_matches"] = match_count
            r["convergence_boost"] = round(boost, 3)

        return results

    # ── Phase B2 Step 5: Document-Length Normalization ────────────────────

    def _apply_length_normalization(self, results: list[dict], query_keywords: list[str],
                                     prefetched: dict | None = None) -> list[dict]:
        """B2-Step5: Dual-layer length normalization.
        Layer 1: Normalize by annotation count (prevents keyword-stuffed docs from dominating).
        Layer 2: Normalize by summary text length (prevents short-doc bias).
        """
        if not results:
            return results

        length_norm_k = SCORING.get("length_norm_k", 0.15)

        # Use prefetched dl_map if available (OPT-005), else fallback to direct query
        if prefetched and "dl_map" in prefetched:
            anno_counts = prefetched["dl_map"]
        else:
            sids = [r.get("summary_id") for r in results if r.get("summary_id")]
            anno_counts: dict[int, int] = {}
            if sids:
                placeholders = ",".join("?" * len(sids))
                rows = self.db.execute(
                    f"SELECT summary_id, COUNT(*) as cnt FROM annotations WHERE summary_id IN ({placeholders}) GROUP BY summary_id",
                    sids
                ).fetchall()
                for row in rows:
                    anno_counts[row["summary_id"]] = row["cnt"]

        for r in results:
            sid = r.get("summary_id")
            anno_count = anno_counts.get(sid, 1)
            summary_len = len(r.get("summary", ""))

            # Layer 1: annotation count normalization
            layer1 = 1.0 / (1.0 + math.log(max(anno_count, 1)))
            # Layer 2: summary length normalization
            layer2 = 1.0 / (1.0 + length_norm_k * math.log(max(summary_len, 1))) if summary_len > 0 else 1.0

            combined = layer1 * layer2
            r["relevance_score"] = r.get("relevance_score", 0) * combined
            r["length_norm"] = round(combined, 4)
            r["annotation_count"] = anno_count
            r["summary_length"] = summary_len

        return results

    # ── Phase A1: Temporal Intent Detection (B-009) ───────────────────────

    def _detect_temporal_intent(self, query_keywords: list[str]) -> str:
        """Detect temporal intent from query keywords. Returns 'recent', 'historical', or 'neutral'."""
        kw_lower = set(kw.lower().strip() for kw in query_keywords)
        recent_hits = len(kw_lower & TEMPORAL_RECENT_SIGNALS)
        historical_hits = len(kw_lower & TEMPORAL_HISTORICAL_SIGNALS)
        if recent_hits > historical_hits:
            return "recent"
        elif historical_hits > recent_hits:
            return "historical"
        return "neutral"

    def _apply_temporal_scoring(self, results: list[dict], query_keywords: list[str]) -> list[dict]:
        """B-009: Replace blind recency with intent-aware temporal scoring.

        Uses generated_at (when content was CREATED) instead of created_at (ingested).
        Detects intent: 'recent' boosts new, 'historical' boosts old, 'neutral' applies mild decay.
        Smooth exponential curve replaces the old step-function cliff.
        """
        if not results:
            return results

        intent = self._detect_temporal_intent(query_keywords)
        use_generated = SCORING.get("use_generated_at", True)
        max_boost = SCORING.get("recency_max_boost", 1.0)
        half_life = SCORING.get("recency_half_life_hours", 48)
        neutral_decay = SCORING.get("neutral_decay_multiplier", 0.2)
        now = time.time()

        for r in results:
            # OPT-019: Use pre-computed age_hours from SQL if available
            age_hours = r.get("age_hours")
            if age_hours is None:
                # Fallback: parse timestamp in Python (for non-GPU paths)
                ts_str = None
                if use_generated:
                    ts_str = r.get("generated_at")
                if not ts_str:
                    ts_str = r.get("created_at", "")
                if not ts_str:
                    r["temporal_intent"] = intent
                    continue
                try:
                    dt = datetime.strptime(ts_str[:19], "%Y-%m-%d %H:%M:%S")
                    age_hours = (now - dt.replace(tzinfo=timezone.utc).timestamp()) / 3600
                except (ValueError, OSError):
                    r["temporal_intent"] = intent
                    continue

            # Exponential decay: boost = max_boost * 2^(-age/half_life)
            decay = max_boost * (2 ** (-age_hours / max(half_life, 1)))

            if intent == "recent":
                # Boost newer content: full decay curve favoring freshness
                boost = 1.0 + decay
            elif intent == "historical":
                # Boost older content: invert the curve — older = higher boost
                inverse_decay = max_boost * (1.0 - (2 ** (-age_hours / max(half_life, 1))))
                boost = 1.0 + inverse_decay
            else:
                # Neutral: mild recency bias, much weaker than directional intent
                boost = 1.0 + decay * neutral_decay

            r["relevance_score"] = r.get("relevance_score", 0) * boost
            r["temporal_intent"] = intent
            r["temporal_boost"] = round(boost, 4)

        return results

    # ── Phase A2: B-010 Selection Feedback Loop ────────────────────────────

    def _apply_selection_boost(self, results: list[dict], query_keywords: list[str],
                               prefetched: dict | None = None) -> list[dict]:
        """B-010: Boost results that have historically been selected as 'right answers'."""
        if not results:
            return results

        signal_weights = SCORING.get("selection_signal_weights", {
            "ranking_promoted": 0.3,
            "curator_suggested": 0.5,
            "user_selected": 1.0,
        })

        # Use prefetched data if available, otherwise query DB
        if prefetched and "sel_map" in prefetched:
            sel_map = prefetched["sel_map"]
        else:
            summary_ids = [r["summary_id"] for r in results if r.get("summary_id")]
            if not summary_ids:
                return results
            sel_map = self._batch_prefetch_selection(summary_ids)

        for r in results:
            sid = r.get("summary_id")
            signals = sel_map.get(sid, {})
            if not signals:
                r["selection_score"] = 0.0
                r["selection_boost"] = 1.0
                continue

            total_score = sum(
                count * signal_weights.get(sig_type, 0.3)
                for sig_type, count in signals.items()
            )

            # Logarithmic normalization: prevents runaway boosting
            selection_boost = 1.0 + 0.3 * math.log1p(total_score)
            r["relevance_score"] = r.get("relevance_score", 0) * selection_boost
            r["selection_score"] = round(total_score, 2)
            r["selection_boost"] = round(selection_boost, 3)

        return results

    # ── Phase A4: Negation Scoring (B-013) ─────────────────────────────────

    def _apply_negation_score(self, results: list[dict], query_keywords: list[str],
                              prefetched: dict | None = None) -> list[dict]:
        """B-013: Apply rejection penalties — the negating metric.
        Reduces scores for items that have been rejected for this query topic.
        Does NOT penalize ignored items (ignore != reject)."""
        if not results:
            return results

        rejection_half_life = SCORING.get("rejection_half_life_days", 60)
        rejection_base_penalty = SCORING.get("rejection_base_penalty", 0.1)
        negation_floor = SCORING.get("negation_floor", 0.2)

        # Use prefetched data if available, otherwise query DB
        if prefetched and "rej_map" in prefetched:
            rej_map = prefetched["rej_map"]
        else:
            summary_ids = [r["summary_id"] for r in results if r.get("summary_id")]
            if not summary_ids:
                return results
            rej_map = self._batch_prefetch_negation(summary_ids)

        now = time.time()
        for r in results:
            sid = r.get("summary_id")
            rejections = rej_map.get(sid, [])
            if not rejections:
                r["negation_penalty"] = 0.0
                r["negation_multiplier"] = 1.0
                continue

            total_penalty = 0.0
            for ts in rejections:
                try:
                    dt = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
                    age_days = (now - dt.replace(tzinfo=timezone.utc).timestamp()) / 86400
                    total_penalty += rejection_base_penalty * math.exp(-age_days / max(rejection_half_life, 1))
                except (ValueError, OSError):
                    total_penalty += rejection_base_penalty * 0.5

            penalty_multiplier = max(negation_floor, 1.0 - total_penalty)
            r["relevance_score"] = r.get("relevance_score", 0) * penalty_multiplier
            r["negation_penalty"] = round(total_penalty, 3)
            r["negation_multiplier"] = round(penalty_multiplier, 3)

        return results

    # ── Phase B2 Step 6: Source Authority Weighting ────────────────────────

    def _apply_source_authority(self, results: list[dict], query_keywords: list[str]) -> list[dict]:
        """B2-Step6: Weight results by source type authority.
        Configurable multiplier per source type — memory_core, active_project, etc.
        Uses substring matching against source field for flexibility.
        """
        if not results:
            return results

        authority_map = SCORING.get("source_authority_weights", {})
        default_weight = authority_map.get("default", 1.0)

        for r in results:
            source = (r.get("source") or "").lower()
            weight = default_weight
            for key, w in authority_map.items():
                if key != "default" and key in source:
                    weight = w
                    break
            r["relevance_score"] = r.get("relevance_score", 0) * weight
            r["source_authority"] = round(weight, 3)

        return results

    # ── Sprint 3.6: Code-Intent Relevance Tuning ───────────────────────────

    def _apply_code_intent_boost(self, results: list[dict], query_keywords: list[str]) -> list[dict]:
        """Boost extension/path relevance for code-intent queries.

        Minimal, surgical pass focused on:
          - extension-aware boost (.py when query implies python/code)
          - DRGFC path weighting when query contains drgfc
        """
        if not results or not query_keywords:
            return results

        kw = {k.lower().strip() for k in query_keywords if k and k.strip()}
        code_intent = any(k in kw for k in {
            "python", "code", ".py", "py", "script", "scripts", "function", "module"
        })
        python_intent = "python" in kw or ".py" in kw or "py" in kw
        drgfc_focus = "drgfc" in kw

        if not code_intent and not drgfc_focus:
            return results

        code_exts = {".py", ".ipynb", ".ts", ".tsx", ".js", ".jsx", ".cpp", ".c", ".h", ".hpp", ".rs", ".go", ".java"}

        for r in results:
            metadata = r.get("metadata") or {}
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except Exception:
                    metadata = {}

            filepath = (metadata.get("filepath") or metadata.get("filename") or "").lower()
            ext = os.path.splitext(filepath)[1] if filepath else ""

            boost = 1.0

            if code_intent:
                if ext == ".py":
                    boost *= 2.10 if python_intent else 1.55
                elif ext in code_exts:
                    boost *= 0.35 if python_intent else 1.08
                elif ext:
                    boost *= 0.20 if python_intent else 0.72

            if drgfc_focus:
                if "drgfc" in filepath:
                    boost *= 1.20
                    if ext == ".py":
                        boost *= 1.80
                    elif python_intent and ext:
                        boost *= 0.10
                else:
                    boost *= 0.92

            r["relevance_score"] = r.get("relevance_score", 0) * boost
            r["code_intent_boost"] = round(boost, 3)

        return results

    # ── Phase A5: Step 48 — Debug Logging Scaffold ──────────────────────────

    def _log_scoring_breakdown(self, results: list[dict], query_keywords: list[str], pipeline_ms: float) -> None:
        """Step 48 + B4.5: Emit structured scoring breakdown at DEBUG level.
        Includes convergence/triangulation stage info.
        Zero overhead when DEBUG is disabled (isEnabledFor guard)."""
        if not logger.isEnabledFor(logging.DEBUG) or not results:
            return
        top_n = results[:10]
        # B4.5: Triangulation stage summary
        _tri_cands = getattr(self._tls, 'tri_candidates', None) or self._current_tri_candidates
        tri_count = len(_tri_cands) if _tri_cands else 0
        breakdown = {
            "query": " ".join(query_keywords),
            "pipeline_ms": round(pipeline_ms, 2),
            "results_count": len(results),
            "triangulation": {
                "source": getattr(self._tls, "tri_source", getattr(self, "_tri_source", "none")),
                "candidates": tri_count,
                "bloom_enabled": self._bloom_cascade is not None,
                "gpu_triad_enabled": self._gpu_triad is not None and self._gpu_triad.enabled,
            },
            "top_results": [],
        }
        for r in top_n:
            entry = {
                "summary_id": r.get("summary_id"),
                "relevance_score": round(r.get("relevance_score", 0), 6),
                "coverage_ratio": r.get("coverage_ratio", 0),
                "matched_query_terms": r.get("matched_query_terms", 0),
                "total_query_terms": r.get("total_query_terms", 0),
                "convergence_matches": r.get("convergence_matches", 0),
                "convergence_boost": r.get("convergence_boost", 1.0),
                "temporal_boost": r.get("temporal_boost", 0),
                "temporal_intent": r.get("temporal_intent", ""),
                "heat_boost": r.get("heat_boost", 0),
                "selection_boost": r.get("selection_boost", 0),
                "graph_boost": r.get("graph_boost", 0),
                "negation_multiplier": r.get("negation_multiplier", 1.0),
            }
            breakdown["top_results"].append(entry)
        logger.debug("scoring_breakdown: %s", json.dumps(breakdown))

    # ── Phase A5: Step 49 — Config Tuning Verification ─────────────────────

    def get_scoring_distribution(self, results: list[dict]) -> dict:
        """Step 49: Analyze per-dimension min/max/mean/spread for scored results.
        Returns distribution stats to verify no single dimension dominates."""
        dimensions = [
            "coverage_ratio", "matched_query_terms", "total_query_terms",
            "temporal_boost", "heat_boost", "selection_boost",
            "graph_boost", "negation_multiplier",
        ]
        dist: dict[str, dict] = {}
        for dim in dimensions:
            values = [r.get(dim, 0) for r in results if r.get(dim) is not None]
            if not values:
                dist[dim] = {"min": 0, "max": 0, "mean": 0, "spread": 0, "count": 0}
                continue
            mn, mx = min(values), max(values)
            mean = sum(values) / len(values)
            dist[dim] = {
                "min": round(mn, 6),
                "max": round(mx, 6),
                "mean": round(mean, 6),
                "spread": round(mx - mn, 6),
                "count": len(values),
            }
        return dist

    def log_ranking_promoted(self, results: list[dict], query_text: str, agent: str = ""):
        """B-010: Log ranking_promoted signals for all returned results.
        B-012: Mark graph links as explored when surfaced to user."""
        if not results:
            return
        query_hash = str(uaimc_anno.fnv1a_hash(" ".join(sorted(
            kw.lower() for kw in query_text.split() if kw.strip()
        ))))
        with self._write_lock:
            try:
                promoted_ids = []
                for i, r in enumerate(results):
                    sid = r.get("summary_id")
                    if sid:
                        promoted_ids.append(sid)
                        self.db.execute(
                            """INSERT INTO selection_log
                               (query_hash, summary_id, signal_type, agent, query_text, position)
                               VALUES (?, ?, 'ranking_promoted', ?, ?, ?)""",
                            (query_hash, sid, agent, query_text[:500], i + 1),
                        )
                # B-012: Mark graph links involving surfaced docs as explored
                if promoted_ids:
                    placeholders = ",".join("?" * len(promoted_ids))
                    self.db.execute(
                        f"""UPDATE document_links
                            SET explored = 1, last_explored_at = datetime('now')
                            WHERE explored = 0
                            AND (source_id IN ({placeholders}) OR target_id IN ({placeholders}))""",
                        promoted_ids + promoted_ids
                    )
                self.db.commit()
            except Exception as e:
                logger.error(f"log_ranking_promoted failed: {e}")

    # ── Phase A2: B-011 Topic Heat Map ─────────────────────────────────────

    def _get_topic_heat(self, query_keywords: list[str], agent: str = "*", conn=None) -> dict[str, float]:
        """B-011: Calculate current heat coefficients for query-related topics."""
        heat_window = SCORING.get("heat_window_hours", 168)
        heat_half_life = SCORING.get("heat_decay_half_life_hours", 24)
        _db = conn if conn is not None else self.db

        topic_heat: dict[str, float] = {}
        for kw in query_keywords:
            token = kw.lower().strip()
            if not token:
                continue

            rows = _db.execute(
                """SELECT weight, created_at FROM topic_heat
                   WHERE topic_token = ? AND (agent = ? OR agent = '*')
                   AND created_at > datetime('now', ?)""",
                (token, agent, f"-{heat_window} hours"),
            ).fetchall()

            heat = 0.0
            now = time.time()
            for row in rows:  # B1-Step2: named access
                w = row["weight"]
                ts = row["created_at"]
                try:
                    dt = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
                    age_hours = (now - dt.replace(tzinfo=timezone.utc).timestamp()) / 3600
                    heat += w * math.exp(-age_hours / max(heat_half_life, 1))
                except (ValueError, OSError):
                    heat += w * 0.5  # Fallback: half weight if timestamp parse fails

            topic_heat[token] = round(heat, 3)

        return topic_heat

    def _apply_heat_map_boost(self, results: list[dict], query_keywords: list[str],
                               agent: str = "*", prefetched: dict | None = None, conn=None) -> list[dict]:
        """B-011: Boost results whose topics are currently 'hot' for this agent."""
        if not results:
            return results

        topic_heat = self._get_topic_heat(query_keywords, agent, conn=conn)
        if not topic_heat or all(v == 0 for v in topic_heat.values()):
            return results

        max_heat = max(topic_heat.values()) if topic_heat.values() else 1.0
        heat_boost_cap = SCORING.get("heat_boost_cap", 1.5)

        # Use prefetched token_map if available (OPT-005), else fallback to direct query
        if prefetched and "token_map" in prefetched:
            token_map = prefetched["token_map"]
        else:
            summary_ids = [r["summary_id"] for r in results if r.get("summary_id")]
            if not summary_ids:
                return results
            placeholders = ",".join("?" for _ in summary_ids)
            rows = self.db.execute(
                f"SELECT summary_id, token FROM annotations WHERE summary_id IN ({placeholders})",
                summary_ids,
            ).fetchall()
            token_map: dict[int, set[str]] = {}
            for row in rows:
                sid = row["summary_id"]
                tok = row["token"]
                token_map.setdefault(sid, set()).add(tok.lower())

        for r in results:
            sid = r.get("summary_id")
            doc_tokens = token_map.get(sid, set())

            doc_heat = sum(topic_heat.get(t, 0) for t in doc_tokens)
            normalized = doc_heat / max_heat if max_heat > 0 else 0

            boost = 1.0 + (heat_boost_cap - 1.0) * min(normalized, 1.0)
            r["relevance_score"] = r.get("relevance_score", 0) * boost
            r["heat_boost"] = round(boost, 3)
            r["topic_heat"] = {k: v for k, v in topic_heat.items() if k in doc_tokens}

        return results

    def log_heat_event(self, tokens: list[str], agent: str = "*", heat_type: str = "query"):
        """B-011: Record heat events for topic tokens."""
        if not tokens:
            return
        source_weights = SCORING.get("heat_source_weights", {
            "query": 1.0, "ingest": 0.5, "selection": 2.0, "session_start": 0.3
        })
        weight = source_weights.get(heat_type, 1.0)
        with self._write_lock:
            try:
                for tok in tokens:
                    t = tok.lower().strip()
                    if t:
                        self.db.execute(
                            """INSERT INTO topic_heat (topic_token, agent, heat_type, weight)
                               VALUES (?, ?, ?, ?)""",
                            (t, agent or "*", heat_type, weight),
                        )
                self.db.commit()
            except Exception as e:
                logger.error(f"log_heat_event failed: {e}")

    # ── Phase A3: Graph Distance Scoring (B-012) ──────────────────────────

    def _apply_graph_distance_boost(self, results: list[dict], query_keywords: list[str],
                                     max_hops: int = 2, prefetched: dict | None = None) -> list[dict]:
        """B-012: Boost results that are graph-connected to the query topic.

        BFS from seed results (top-5) to find connected documents up to max_hops.
        Hidden links (explored=0) receive discovery_bonus for unexplored territory.
        """
        if not results:
            return results

        seed_ids = [r["summary_id"] for r in results[:5] if r.get("summary_id")]
        if not seed_ids:
            return results

        discovery_bonus = SCORING.get("discovery_bonus", 1.3)
        link_decay_per_hop = SCORING.get("link_decay_per_hop", 0.5)

        # Use prefetched graph data if available, otherwise BFS from DB
        if prefetched and "graph_map" in prefetched:
            connected = prefetched["graph_map"]
        else:
            connected = self._batch_prefetch_graph(seed_ids, max_hops)

        # Apply boost to results
        for r in results:
            sid = r.get("summary_id")
            if sid and sid in connected:
                hops, strength, is_hidden = connected[sid]
                distance_factor = strength * (link_decay_per_hop ** (hops - 1))
                boost = 1.0 + distance_factor
                if is_hidden:
                    boost *= discovery_bonus
                r["relevance_score"] = r.get("relevance_score", 0) * boost
                r["graph_hops"] = hops
                r["graph_boost"] = round(boost, 3)
                r["is_hidden_link"] = is_hidden

        return results

    # ── CANS Sprint 4: GAAMA Hybrid PPR (Replaces Stage 6 when GAAMA_ENABLED) ──

    def _bridge_summary_to_kg_node(self, summary_id: int, conn: sqlite3.Connection | None = None) -> list[str]:
        """Bridge summary_id (INTEGER) to kg_node ids (TEXT).

        Join path: summaries.metadata->filepath → normalize slashes → prepend 'file:'
        → matches kg_nodes.id. Covers 84% of summaries (9,174 / 10,901).
        """
        db = conn or self.db
        rows = db.execute("""
            SELECT k.id
            FROM summaries s
            JOIN kg_nodes k ON k.type = 'file'
                AND k.id = 'file:' || REPLACE(json_extract(s.metadata, '$.filepath'), '\\', '/')
            WHERE s.id = ? AND s.metadata IS NOT NULL
        """, (summary_id,)).fetchall()
        return [r[0] for r in rows]

    def _batch_bridge_summaries(self, summary_ids: list[int],
                                conn: sqlite3.Connection | None = None) -> dict[int, list[str]]:
        """Batch bridge: summary_ids → {summary_id: [kg_node_id, ...]}."""
        if not summary_ids:
            return {}
        db = conn or self.db
        placeholders = ",".join("?" for _ in summary_ids)
        rows = db.execute(f"""
            SELECT s.id as sid, k.id as kid
            FROM summaries s
            JOIN kg_nodes k ON k.type = 'file'
                AND k.id = 'file:' || REPLACE(json_extract(s.metadata, '$.filepath'), '\\', '/')
            WHERE s.id IN ({placeholders}) AND s.metadata IS NOT NULL
        """, list(summary_ids)).fetchall()
        result: dict[int, list[str]] = {}
        for r in rows:
            result.setdefault(r[0], []).append(r[1])
        return result

    def _batch_load_subgraph(self, seed_kg_ids: list[str], max_hops: int = 3,
                             conn: sqlite3.Connection | None = None) -> list[tuple]:
        """Load multi-hop subgraph from kg_edges via recursive CTE.

        Returns list of (source_id, target_id, type, weight) tuples.
        Single query, no N+1. Hop limit prevents combinatorial explosion.
        FIX: UNION (not UNION ALL) to deduplicate during recursion and prevent
        exponential path explosion. LIMIT 10000 as safety net. Max 2 hops.
        """
        if not seed_kg_ids:
            return []
        db = conn or self.db
        # Cap hops to prevent combinatorial explosion in dense graphs
        safe_hops = min(max_hops, 2)
        placeholders = ",".join("?" for _ in seed_kg_ids)
        rows = db.execute(f"""
            WITH RECURSIVE subgraph(src, tgt, etype, weight, hop) AS (
                SELECT source_id, target_id, type, weight, 0
                FROM kg_edges WHERE source_id IN ({placeholders})
                UNION
                SELECT e.source_id, e.target_id, e.type, e.weight, s.hop + 1
                FROM kg_edges e JOIN subgraph s ON e.source_id = s.tgt
                WHERE s.hop < ?
            )
            SELECT DISTINCT src, tgt, etype, weight FROM subgraph
            LIMIT 10000
        """, list(seed_kg_ids) + [safe_hops]).fetchall()
        return [(r[0], r[1], r[2], r[3]) for r in rows]

    def _batch_load_concept_metadata(self, edges: list[tuple],
                                     conn: sqlite3.Connection | None = None) -> dict[str, dict]:
        """Batch-load concept_nodes metadata for all nodes in the subgraph.

        Returns {concept_id: parsed_metadata_dict}. Single query, no N+1.
        """
        unique_nodes = {e[0] for e in edges} | {e[1] for e in edges}
        if not unique_nodes:
            return {}
        db = conn or self.db
        placeholders = ",".join("?" for _ in unique_nodes)
        rows = db.execute(f"""
            SELECT concept_id, metadata FROM concept_nodes
            WHERE concept_id IN ({placeholders})
        """, list(unique_nodes)).fetchall()
        result = {}
        for r in rows:
            try:
                result[r[0]] = json.loads(r[1] or "{}")
            except (json.JSONDecodeError, TypeError):
                result[r[0]] = {}
        return result

    def _apply_ppr_boost(self, results: list[dict], query_keywords: list[str],
                         prefetched: dict | None = None, agent_name: str = "") -> list[dict]:
        """CANS Sprint 4: GAAMA Hybrid PPR — replaces Stage 6 Graph Distance Boost.
        Phase 6: Agent-affinity parameters for per-agent graph walk behavior.

        Seeds top-5 summaries → bridges to kg_node_ids → runs push-based PPR
        via ppr_engine.compute_ppr_push with hub dampening + teleport.
        Falls back to legacy BFS if GAAMA_ENABLED is False.

        Weighted blend: score = 0.7*semantic_base + 0.3*ppr_normalized
        Push-based engine: α=0.15, ε=1e-4, top_k=200
        Hub dampening: θ from concept_nodes.metadata (default 50)
        """
        if not results:
            return results

        if not GAAMA_ENABLED:
            return self._apply_graph_distance_boost(results, query_keywords, prefetched=prefetched)

        # 1. Bridge top-5 summary_ids to kg_node_ids
        seed_summary_ids = [r["summary_id"] for r in results[:5] if r.get("summary_id")]
        if not seed_summary_ids:
            return results

        bridge_map = self._batch_bridge_summaries(seed_summary_ids)
        seed_kg_ids = []
        for sid in seed_summary_ids:
            seed_kg_ids.extend(bridge_map.get(sid, []))

        if not seed_kg_ids:
            # No kg_nodes found for seeds — fall back to legacy BFS
            return self._apply_graph_distance_boost(results, query_keywords, prefetched=prefetched)

        # 2. Check ppr_cache
        cache_key = hashlib.sha256(str(sorted(seed_kg_ids)).encode()).hexdigest()[:16]
        cached = self.db.execute(
            "SELECT neighbors_json FROM ppr_cache WHERE concept_id = ? "
            "AND computed_at > datetime('now', '-1 hour')",
            (cache_key,)
        ).fetchone()

        if cached:
            try:
                ppr = json.loads(cached[0])
            except (json.JSONDecodeError, TypeError):
                ppr = None

        if not cached or ppr is None:
            # Phase 6: Agent-affinity PPR parameters
            _affinity = self.get_agent_affinity_config(agent_name) if agent_name else {}
            _ppr_hops = _affinity.get("ppr_hops", GAAMA_PPR_HOPS)
            _ppr_alpha = _affinity.get("ppr_alpha", GAAMA_PPR_ALPHA)
            _ppr_weight = _affinity.get("ppr_weight", GAAMA_PPR_WEIGHT)

            # 3. Load multi-hop subgraph
            edges = self._batch_load_subgraph(seed_kg_ids, max_hops=_ppr_hops)

            if not edges:
                return self._apply_graph_distance_boost(results, query_keywords, prefetched=prefetched)

            # 4-5. Compute PPR via push-based engine (replaces inline power iteration)
            concept_meta = self._batch_load_concept_metadata(edges)
            hub_dampening = {
                cid: meta.get("hub_dampening_theta", GAAMA_HUB_THETA_DEFAULT)
                for cid, meta in concept_meta.items()
            }
            # Phase 3: Load reversible edge weight overrides
            edge_overrides = load_edge_overrides(self.db_path)
            ppr = compute_ppr_push(
                edges=edges,
                seed_ids=seed_kg_ids,
                alpha=_ppr_alpha,
                epsilon=GAAMA_PPR_EPSILON,
                max_iterations=GAAMA_PPR_MAX_ITER,
                hub_dampening=hub_dampening,
                edge_type_weights=None,  # use engine defaults
                edge_overrides=edge_overrides,
                top_k=GAAMA_PPR_TOP_K,
            )

            # 6. Cache result (top-K only, not all nodes)
            try:
                self.db.execute(
                    "INSERT OR REPLACE INTO ppr_cache (concept_id, neighbors_json, computed_at) "
                    "VALUES (?, ?, datetime('now'))",
                    (cache_key, json.dumps(ppr))
                )
                self.db.commit()
            except Exception as e:
                logger.warning(f"ppr_cache write failed: {e}")

        # 7. Batch-bridge ALL result summary_ids and apply hybrid score
        # Phase 6: Use agent-specific PPR weight from affinity config
        _affinity = self.get_agent_affinity_config(agent_name) if agent_name else {}
        _ppr_weight = _affinity.get("ppr_weight", GAAMA_PPR_WEIGHT)
        all_sids = [r["summary_id"] for r in results if r.get("summary_id")]
        full_bridge = self._batch_bridge_summaries(all_sids)
        discovery_bonus = SCORING.get("discovery_bonus", 1.3)
        max_ppr = max(ppr.values()) if ppr else 1.0

        for r in results:
            sid = r.get("summary_id")
            if not sid:
                continue
            linked_kg = full_bridge.get(sid, [])
            ppr_mass = sum(ppr.get(kid, 0.0) for kid in linked_kg)
            if ppr_mass > 0:
                semantic_base = r.get("relevance_score", 0.0)
                if GAAMA_BLEND_MODE == "weighted":
                    ppr_normalized = ppr_mass / max(max_ppr, 1e-9)
                    r["relevance_score"] = (1 - _ppr_weight) * semantic_base + _ppr_weight * ppr_normalized
                else:
                    # Legacy additive mode (for A/B testing)
                    r["relevance_score"] = semantic_base + _ppr_weight * ppr_mass
                r["graph_ppr_score"] = round(ppr_mass, 4)
                r["graph_boost"] = round(1.0 + _ppr_weight * ppr_mass / max(semantic_base, 0.001), 3)

        return results

    def _cleanup_ppr_cache(self):
        """Delete expired ppr_cache entries (older than TTL)."""
        deleted = self.db.execute(
            "DELETE FROM ppr_cache WHERE computed_at < datetime('now', '-' || ? || ' seconds')",
            (GAAMA_CACHE_TTL,)
        ).rowcount
        self.db.commit()
        if deleted:
            logger.info("Cleaned %d stale ppr_cache entries", deleted)

    # ── Phase 3: GAAMA Context Assembly (CANS Sprint 4) ──────────────────

    def _is_cans_populated(self, conn=None) -> bool:
        """Check if CANS tables have data (fact_nodes as canary)."""
        _t = time.time()
        db = conn or self.db
        count = db.execute("SELECT COUNT(*) FROM fact_nodes").fetchone()[0]
        logger.debug("_is_cans_populated: %d facts in %.1fms", count, (time.time() - _t) * 1000)
        return count > 0

    def _apply_context_assembly(self, ranked_results: list[dict], agent_mode: str = "human", agent_name: str = "") -> dict:
        """Phase 3 GAAMA: caps → compressor → chrono sort.
        
        Full graceful fallback when CANS tables are empty (FLAG-017/022).
        Global caps enforced after loop (FLAG-015/021).
        Phase 5: Per-agent word budgets from config agent_overrides.
        """
        logger.info("_apply_context_assembly START: %d results, mode=%s, agent=%s", len(ranked_results), agent_mode, agent_name)
        if not ranked_results:
            return {"context": "", "pack_stats": {}, "disclosure_layers": []}

        max_facts = 60
        max_reflections = 20
        max_episodes = 80

        # Phase 5: Per-agent adaptive word budget
        _ctx_cfg = CONFIG.get("context", {})
        _agent_ov = _ctx_cfg.get("agent_overrides", {}).get(agent_name, {}) if agent_name else {}
        if agent_mode == "human":
            total_target_words = _agent_ov.get("word_budget", _ctx_cfg.get("default_word_budget", 1000))
        else:
            total_target_words = _agent_ov.get("word_budget", _ctx_cfg.get("default_word_budget", 8000))

        if not self._is_cans_populated():
            return self._legacy_context_assembly(ranked_results, agent_mode)

        facts, reflections, episodes = self._gather_cans_nodes(
            ranked_results, max_episodes
        )

        # Global caps AFTER loop — safety net (Phase 1: primary caps now in _gather_cans_nodes)
        if len(facts) > max_facts:
            logger.warning("_apply_context_assembly: facts exceeded cap (%d > %d) — safety slice applied", len(facts), max_facts)
            facts = facts[:max_facts]
        if len(reflections) > max_reflections:
            logger.warning("_apply_context_assembly: reflections exceeded cap (%d > %d) — safety slice applied", len(reflections), max_reflections)
            reflections = reflections[:max_reflections]
        episodes = episodes[:max_episodes]

        # Phase 5: Per-agent compression overrides for BTC path
        _full_pct = _agent_ov.get("full_verbatim_pct", _ctx_cfg.get("full_verbatim_pct", 0.3))
        _summary_pct = _agent_ov.get("summary_only_pct", _ctx_cfg.get("summary_only_pct", 0.3))

        packed = self._apply_btc_compressor(facts, reflections, episodes, total_target_words,
                                             full_verbatim_pct=_full_pct, summary_only_pct=_summary_pct)
        packed = self._sort_chronologically(packed)
        layers = self._build_progressive_disclosure(packed, agent_mode)

        context = "\n\n".join(layers[0]) if agent_mode == "human" else packed["full_text"]
        stats = {
            "facts_capped": len(facts), "reflections_capped": len(reflections),
            "episodes_packed": len(episodes), "compression_ratio": packed.get("ratio", 1.0),
            "ppr_boost_applied": True, "fallback_used": False,
        }
        return {"context": context, "pack_stats": stats, "disclosure_layers": layers}

    def _legacy_context_assembly(self, ranked_results: list[dict], agent_mode: str) -> dict:
        """Graceful fallback using existing summaries (FLAG-022).
        
        Re-uses proven pipeline output — result dicts have 'summary' key.
        """
        pieces = [r.get("summary", "") for r in ranked_results[:50] if r.get("summary")]
        context = "\n\n".join(pieces)
        return {
            "context": context,
            "pack_stats": {"fallback_used": True, "facts_capped": 0,
                           "reflections_capped": 0, "episodes_packed": len(pieces)},
            "disclosure_layers": [pieces[:3]],
        }

    def _gather_cans_nodes(self, ranked_results: list[dict], max_episodes: int
                           ) -> tuple[list[dict], list[dict], list[dict]]:
        """Batch-gather facts, reflections, episodes from CANS tables.
        
        Uses a dedicated pool connection to avoid blocking on self.db writes.
        Phase 1 fix: per-iteration caps + early exit (was unbounded accumulation → 69GB OOM).
        """
        _t0 = time.time()
        _conn = self._pool.get()
        try:
            facts: list[dict] = []
            reflections: list[dict] = []
            episodes: list[dict] = []
            bridged = 0

            # Per-iteration budgets (prevent any one kg_node from dominating)
            FACTS_PER_KG = 8
            REFLECTIONS_PER_KG = 3
            # Global caps (same as FLAG-015/021)
            MAX_FACTS = 60
            MAX_REFLECTIONS = 20

            for i, r in enumerate(ranked_results[:max_episodes]):
                # EARLY EXIT: stop gathering facts/reflections if both caps are met
                if len(facts) >= MAX_FACTS and len(reflections) >= MAX_REFLECTIONS:
                    logger.debug("_gather_cans_nodes: early exit at iteration %d (caps met)", i)
                    # Still collect remaining episodes for completeness
                    sid = r.get("summary_id")
                    if sid:
                        episodes.append(self._get_verbatim_episode(sid, conn=_conn))
                    continue

                sid = r.get("summary_id")
                if not sid:
                    logger.debug("_gather_cans_nodes: result %d has no summary_id, keys=%s", i, list(r.keys()))
                    continue
                _tb = time.time()
                kg_ids = self._bridge_summary_to_kg_node(sid, conn=_conn)
                logger.debug("_gather_cans_nodes: sid=%s bridge=%d in %.1fms", sid, len(kg_ids), (time.time() - _tb) * 1000)
                if kg_ids:
                    bridged += 1
                    # Load with per-kg limits, don't exceed remaining global budget
                    remaining_facts = max(0, MAX_FACTS - len(facts))
                    per_kg_fact_limit = min(FACTS_PER_KG, remaining_facts)
                    if per_kg_fact_limit > 0:
                        facts.extend(self._load_derived_facts(kg_ids, conn=_conn, limit=per_kg_fact_limit))

                    remaining_refls = max(0, MAX_REFLECTIONS - len(reflections))
                    per_kg_refl_limit = min(REFLECTIONS_PER_KG, remaining_refls)
                    if per_kg_refl_limit > 0:
                        reflections.extend(self._load_reflections(kg_ids, conn=_conn, limit=per_kg_refl_limit))
                episodes.append(self._get_verbatim_episode(sid, conn=_conn))

            elapsed = (time.time() - _t0) * 1000
            logger.info("_gather_cans_nodes DONE: %d bridged, %d facts, %d reflections, %d episodes in %.0fms",
                         bridged, len(facts), len(reflections), len(episodes), elapsed)
            return facts, reflections, episodes
        finally:
            self._pool.put(_conn)

    def _load_derived_facts(self, kg_ids: list[str], conn=None, limit: int = 100) -> list[dict]:
        """Batch load facts linked to kg_nodes via DERIVED_FROM edges.
        
        GAAMA edge direction: fact -[DERIVED_FROM]-> kg_node
        So facts are SOURCES where kg_nodes are TARGETS.
        Phase 1 fix: JOIN + ORDER BY recency + LIMIT (was unbounded fetchall → 69GB OOM).
        """
        if not kg_ids:
            return []
        db = conn or self.db
        ph = ",".join("?" * len(kg_ids))
        rows = db.execute(f"""
            SELECT f.fact_id, f.fact_text, f.created_at
            FROM fact_nodes f
            JOIN kg_edges e ON f.fact_id = e.source_id
            WHERE e.target_id IN ({ph}) AND e.type = 'DERIVED_FROM'
            ORDER BY f.created_at DESC
            LIMIT ?
        """, [*kg_ids, limit]).fetchall()
        return [{"id": r[0], "content": r[1], "created_at": r[2]} for r in rows]

    def _load_reflections(self, kg_ids: list[str], conn=None, limit: int = 20) -> list[dict]:
        """Batch load reflections linked to kg_nodes via 2-hop traversal.
        
        GAAMA edge directions:
          reflection -[DERIVED_FROM_FACT]-> fact -[DERIVED_FROM]-> kg_node
        So reflections are SOURCES of DERIVED_FROM_FACT, targeting facts
        that are SOURCES of DERIVED_FROM, targeting kg_nodes.
        Phase 1 fix: inner LIMIT 500 + JOIN + ORDER BY recency + outer LIMIT (was unbounded 2-hop → 69GB OOM).
        """
        if not kg_ids:
            return []
        db = conn or self.db
        ph = ",".join("?" * len(kg_ids))
        rows = db.execute(f"""
            SELECT r.reflection_id, r.reflection_text, r.created_at
            FROM reflection_nodes r
            JOIN kg_edges e2 ON r.reflection_id = e2.source_id
            WHERE e2.type = 'DERIVED_FROM_FACT'
            AND e2.target_id IN (
                SELECT e1.source_id FROM kg_edges e1
                WHERE e1.target_id IN ({ph}) AND e1.type = 'DERIVED_FROM'
                LIMIT 500
            )
            ORDER BY r.created_at DESC
            LIMIT ?
        """, [*kg_ids, limit]).fetchall()
        return [{"id": r[0], "content": r[1], "created_at": r[2]} for r in rows]

    def _get_verbatim_episode(self, summary_id: int, conn=None) -> dict:
        """Load verbatim content for a summary."""
        db = conn or self.db
        row = db.execute("""
            SELECT v.content, s.source, s.created_at FROM verbatim v
            JOIN summaries s ON s.verbatim_id = v.id
            WHERE s.id = ?
        """, (summary_id,)).fetchone()
        return {"content": row[0] if row else "", "source": row[1] if row else None, "created_at": row[2] if row else None}

    def _apply_btc_compressor(self, facts: list[dict], reflections: list[dict],
                              episodes: list[dict], target_words: int,
                              full_verbatim_pct: float = 0.3, summary_only_pct: float = 0.3) -> dict:
        """B-TC-004 compressor: tiered compression by PPR relevance.
        
        Facts arrive ordered by PPR relevance (position from _gather_cans_nodes).
        Phase 5: full_verbatim_pct and summary_only_pct are now configurable per-agent.
        Top full_verbatim_pct = full text, middle = first 2 sentences, bottom summary_only_pct = first 15 words.
        Reflections always get full text (already synthesized).
        """
        import re as _re

        def _first_n_sentences(text: str, n: int) -> str:
            """Extract first n sentences from text."""
            sentences = _re.split(r'(?<=[.!?])\s+', text.strip())
            return " ".join(sentences[:n]) if sentences else text

        def _first_n_words(text: str, n: int) -> str:
            """Extract first n words with ellipsis."""
            words = text.split()
            if len(words) <= n:
                return text
            return " ".join(words[:n]) + "..."

        # Build unified item list with type tags and original position
        items = []
        original_words = 0

        for i, f in enumerate(facts):
            content = f.get("content", "")
            if not content:
                continue
            original_words += len(content.split())
            items.append({
                "type": "fact", "id": f.get("id"), "content": content,
                "created_at": f.get("created_at"), "position": i,
                "original_content": content,
            })

        for i, r in enumerate(reflections):
            content = r.get("content", "")
            if not content:
                continue
            original_words += len(content.split())
            items.append({
                "type": "reflection", "id": r.get("id"), "content": content,
                "created_at": r.get("created_at"), "position": i,
                "original_content": content,
            })

        for i, e in enumerate(episodes):
            content = e.get("content", "")
            if not content:
                continue
            original_words += len(content.split())
            items.append({
                "type": "episode", "id": e.get("source"), "content": content,
                "created_at": e.get("created_at"), "position": i,
                "original_content": content, "source": e.get("source"),
            })

        if not items:
            return {"full_text": "", "ratio": 1.0, "items": [], "tier_counts": {"full": 0, "truncated": 0, "summary": 0}}

        # Separate reflections (always full) from facts+episodes (tiered by position)
        reflection_items = [it for it in items if it["type"] == "reflection"]
        tierable_items = [it for it in items if it["type"] != "reflection"]

        # Apply tiered compression to facts+episodes by position (lower position = higher PPR rank)
        # Phase 5: tier percentages from per-agent config (full_verbatim_pct / summary_only_pct)
        n = len(tierable_items)
        full_cutoff = max(1, int(n * full_verbatim_pct))
        trunc_cutoff = max(full_cutoff + 1, int(n * (1.0 - summary_only_pct)))

        tier_counts = {"full": len(reflection_items), "truncated": 0, "summary": 0}

        for it in reflection_items:
            it["tier"] = "full"

        for i, it in enumerate(tierable_items):
            if i < full_cutoff:
                it["tier"] = "full"
                tier_counts["full"] += 1
            elif i < trunc_cutoff:
                it["tier"] = "truncated"
                it["content"] = _first_n_sentences(it["original_content"], 2)
                tier_counts["truncated"] += 1
            else:
                it["tier"] = "summary"
                it["content"] = _first_n_words(it["original_content"], 15)
                tier_counts["summary"] += 1

        # Rebuild combined list: reflections first, then tiered items
        all_items = reflection_items + tierable_items

        # Build full_text and compute ratio
        parts = [it["content"] for it in all_items if it["content"]]
        full_text = "\n\n".join(parts)
        compressed_words = len(full_text.split())
        ratio = compressed_words / max(original_words, 1)

        return {
            "full_text": full_text,
            "ratio": round(ratio, 4),
            "items": all_items,
            "tier_counts": tier_counts,
        }

    def _sort_chronologically(self, packed: dict) -> dict:
        """Chronological sort: NEXT edges for episodes, created_at fallback for all items.
        
        NEXT edges connect file-path sources (e.g., 'file:D:/...') — used for episode ordering.
        Facts/reflections sorted by created_at (newest first within each tier).
        Items without timestamps preserve their original position.
        """
        items = packed.get("items")
        if not items:
            return packed

        # Build NEXT edge ordering for episode sources
        episode_sources = [it.get("source") for it in items if it.get("type") == "episode" and it.get("source")]
        next_order = {}
        if episode_sources:
            try:
                _conn = self._pool.get()
                try:
                    # Build file-path keys matching NEXT edge format
                    file_keys = []
                    for src in episode_sources:
                        if src and not src.startswith("file:"):
                            file_keys.append(f"file:{src}")
                        else:
                            file_keys.append(src)
                    ph = ",".join("?" * len(file_keys))
                    rows = _conn.execute(f"""
                        SELECT source_id, target_id FROM kg_edges
                        WHERE type = 'NEXT' AND (source_id IN ({ph}) OR target_id IN ({ph}))
                    """, file_keys + file_keys).fetchall()
                    # BFS to assign order positions from NEXT chain
                    if rows:
                        successors = {}
                        all_nodes = set()
                        for src, tgt in rows:
                            successors.setdefault(src, []).append(tgt)
                            all_nodes.add(src)
                            all_nodes.add(tgt)
                        # Find chain roots (nodes with no predecessors)
                        targets = {tgt for _, tgt in rows}
                        roots = all_nodes - targets
                        pos = 0
                        visited = set()
                        for root in sorted(roots):
                            stack = [root]
                            while stack:
                                node = stack.pop(0)
                                if node in visited:
                                    continue
                                visited.add(node)
                                next_order[node] = pos
                                pos += 1
                                for child in successors.get(node, []):
                                    if child not in visited:
                                        stack.append(child)
                finally:
                    self._pool.put(_conn)
            except Exception as e:
                logger.warning("_sort_chronologically: NEXT edge lookup failed: %s", e)

        def _sort_key(item):
            # For episodes: try NEXT edge order first
            if item.get("type") == "episode" and item.get("source"):
                src = item["source"]
                file_src = f"file:{src}" if not src.startswith("file:") else src
                if file_src in next_order:
                    return (0, next_order[file_src], "")
            # For all items: use created_at (newest first = smallest sort key)
            ts = item.get("created_at")
            if ts:
                return (1, 0, ts)  # Will reverse below for newest-first
            # No timestamp: preserve original position at the end
            return (2, item.get("position", 9999), "")

        # Sort: items with NEXT edges first, then by created_at (newest first), then position
        items_with_ts = [(it, _sort_key(it)) for it in items]
        items_with_ts.sort(key=lambda x: (x[1][0], x[1][1], x[1][2]), reverse=False)
        # Reverse created_at group so newest comes first
        sorted_items = []
        for it, key in items_with_ts:
            sorted_items.append(it)

        # Newest-first for created_at items: reverse the group with sort_key[0]==1
        next_group = [it for it in sorted_items if _sort_key(it)[0] == 0]
        ts_group = [it for it in sorted_items if _sort_key(it)[0] == 1]
        no_ts_group = [it for it in sorted_items if _sort_key(it)[0] == 2]
        ts_group.sort(key=lambda it: it.get("created_at", ""), reverse=True)

        final_items = next_group + ts_group + no_ts_group

        # Rebuild full_text from sorted items
        parts = [it["content"] for it in final_items if it.get("content")]
        packed["items"] = final_items
        packed["full_text"] = "\n\n".join(parts)
        packed["next_edges_used"] = len(next_order) > 0

        return packed

    def _build_progressive_disclosure(self, packed: dict, agent_mode: str) -> list[list[str]]:
        """Progressive disclosure: 3-layer output for all modes.
        
        Phase 5: Always produces 3 layers so /context?layers= works for agents too.
          Layer 0 (Full): all BTC-compressed items
          Layer 1 (Detailed): top 70% items (full + truncated tiers)
          Layer 2 (Quick): top 30% items (full tier only) — max 5 facts + 2 reflections
        """
        items = packed.get("items", [])
        full_text = packed.get("full_text", "")

        if agent_mode != "human" and not items:
            # Agent fallback: all paragraphs when no structured items available
            paragraphs = [p.strip() for p in full_text.split("\n\n") if p.strip()]
            return [paragraphs, paragraphs, paragraphs[:7]]

        if not items:
            return [[full_text]] if full_text else [[]]

        # Layer 0 (Full): all items
        layer_full = [it["content"] for it in items if it.get("content")]

        # Layer 1 (Detailed): full + truncated tiers only (top 70%)
        layer_detailed = [it["content"] for it in items
                          if it.get("content") and it.get("tier") in ("full", "truncated")]

        # Layer 2 (Quick): full tier only, capped at 5 facts + 2 reflections
        quick_facts = 0
        quick_refls = 0
        layer_quick = []
        for it in items:
            if not it.get("content") or it.get("tier") != "full":
                continue
            if it.get("type") == "fact" and quick_facts < 5:
                layer_quick.append(it["content"])
                quick_facts += 1
            elif it.get("type") == "reflection" and quick_refls < 2:
                layer_quick.append(it["content"])
                quick_refls += 1
            elif it.get("type") == "episode" and quick_facts + quick_refls < 7:
                layer_quick.append(it["content"])

        return [layer_full, layer_detailed, layer_quick]

    # ── End Phase 3 ──────────────────────────────────────────────────────

    def _create_auto_links(self, summary_id: int, source: str, text: str, metadata: dict | None):
        """B-012: Orchestrate all auto-link creation at ingest time."""
        try:
            self._create_shared_annotation_links(summary_id, source)
            self._create_same_project_links(summary_id, source)
            self._create_same_author_links(summary_id, metadata)
            self._create_session_adjacent_links(summary_id, metadata)
            self._create_referenced_in_links(summary_id, text)
        except Exception as e:
            logger.error(f"Auto-link creation failed for summary {summary_id}: {e}")
            raise

    def _create_shared_annotation_links(self, summary_id: int, source: str):
        """B-012: Link documents sharing >= threshold annotation tokens.

        Uses batch GROUP BY to avoid N+1 queries.
        """
        overlap_threshold = SCORING.get("graph_link_overlap_threshold", 3)

        # Get this document's tokens
        rows = self.db.execute(
            "SELECT token FROM annotations WHERE summary_id = ?", (summary_id,)
        ).fetchall()
        doc_tokens = set(row["token"] for row in rows)
        if not doc_tokens:
            return

        # Batch: find candidates with overlap >= threshold in one query
        placeholders = ",".join("?" * len(doc_tokens))
        candidates = self.db.execute(
            f"""SELECT summary_id, COUNT(*) as overlap FROM annotations
                WHERE token IN ({placeholders}) AND summary_id != ?
                GROUP BY summary_id HAVING COUNT(*) >= ?
                LIMIT 50""",
            list(doc_tokens) + [summary_id, overlap_threshold]
        ).fetchall()

        for row in candidates:
            cand_id = row["summary_id"]
            overlap = row["overlap"]
            strength = min(0.3 + overlap * 0.1, 1.0)

            # Bidirectional links (prevent duplicates)
            for src, tgt in [(summary_id, cand_id), (cand_id, summary_id)]:
                existing = self.db.execute(
                    "SELECT id FROM document_links WHERE source_id = ? AND target_id = ? AND link_type = 'shared_annotation'",
                    (src, tgt)
                ).fetchone()
                if not existing:
                    self.db.execute(
                        """INSERT INTO document_links (source_id, target_id, link_type, strength, discovered_by)
                           VALUES (?, ?, 'shared_annotation', ?, 'system')""",
                        (src, tgt, strength)
                    )

    def _create_same_project_links(self, summary_id: int, source: str):
        """B-012: Link documents from same project directory (strength=0.7)."""
        parts = source.replace("\\", "/").split("/")
        project_idx = -1
        for i, p in enumerate(parts):
            if p in ("PROJECTS", "00_ACTIVE", "01_RESEARCH"):
                project_idx = i
                break

        if project_idx < 0 or project_idx + 2 >= len(parts):
            return

        project_path = "/".join(parts[:project_idx + 3])

        rows = self.db.execute(
            """SELECT DISTINCT s.id FROM summaries s
               WHERE s.source LIKE ? AND s.id != ?
               LIMIT 50""",
            (f"%{project_path}%", summary_id)
        ).fetchall()

        for row in rows:
            other_id = row["id"]
            existing = self.db.execute(
                "SELECT id FROM document_links WHERE source_id = ? AND target_id = ? AND link_type = 'same_project'",
                (summary_id, other_id)
            ).fetchone()
            if not existing:
                self.db.execute(
                    """INSERT INTO document_links (source_id, target_id, link_type, strength, discovered_by)
                       VALUES (?, ?, 'same_project', 0.7, 'system')""",
                    (summary_id, other_id)
                )

    def _create_same_author_links(self, summary_id: int, metadata: dict | None):
        """B-012: Link documents by same author (strength=0.5)."""
        author = None
        if metadata:
            author = metadata.get("author") or metadata.get("created_by")

        if not author:
            row = self.db.execute(
                "SELECT source FROM summaries WHERE id = ?", (summary_id,)
            ).fetchone()
            if row:
                source = row["source"]
                if source:
                    for agent_name in ("COPILOT_VSCODE", "CLIO", "FORGE", "IRIS", "PORTER", "ATLAS"):
                        if agent_name in source.upper():
                            author = agent_name
                            break

        if not author:
            return

        rows = self.db.execute(
            """SELECT DISTINCT id FROM summaries WHERE
               (metadata LIKE ? OR source LIKE ?) AND id != ?
               LIMIT 50""",
            (f"%{author}%", f"%{author}%", summary_id)
        ).fetchall()

        for row in rows:
            other_id = row["id"]
            existing = self.db.execute(
                "SELECT id FROM document_links WHERE source_id = ? AND target_id = ? AND link_type = 'same_author'",
                (summary_id, other_id)
            ).fetchone()
            if not existing:
                self.db.execute(
                    """INSERT INTO document_links (source_id, target_id, link_type, strength, discovered_by)
                       VALUES (?, ?, 'same_author', 0.5, 'system')""",
                    (summary_id, other_id)
                )

    def _create_session_adjacent_links(self, summary_id: int, metadata: dict | None):
        """B-012: Link documents ingested within 5 min by same agent (strength=0.4)."""
        agent = None
        if metadata:
            agent = metadata.get("agent") or metadata.get("ingested_by")

        row = self.db.execute(
            "SELECT created_at FROM summaries WHERE id = ?", (summary_id,)
        ).fetchone()
        if not row:
            return
        created_at = row["created_at"]
        if not created_at:
            return

        # Find docs ingested within +/- 5 minutes
        query = """SELECT DISTINCT s.id FROM summaries s
                   WHERE s.id != ?
                   AND ABS((julianday(s.created_at) - julianday(?)) * 24 * 60) <= 5
                   LIMIT 50"""
        params: list = [summary_id, created_at]

        if agent:
            query = """SELECT DISTINCT s.id FROM summaries s
                       WHERE s.id != ? AND s.metadata LIKE ?
                       AND ABS((julianday(s.created_at) - julianday(?)) * 24 * 60) <= 5
                       LIMIT 50"""
            params = [summary_id, f"%{agent}%", created_at]

        rows = self.db.execute(query, params).fetchall()

        for row in rows:
            other_id = row["id"]
            existing = self.db.execute(
                "SELECT id FROM document_links WHERE source_id = ? AND target_id = ? AND link_type = 'session_adjacent'",
                (summary_id, other_id)
            ).fetchone()
            if not existing:
                self.db.execute(
                    """INSERT INTO document_links (source_id, target_id, link_type, strength, discovered_by)
                       VALUES (?, ?, 'session_adjacent', 0.4, 'system')""",
                    (summary_id, other_id)
                )

    def _create_referenced_in_links(self, summary_id: int, text: str):
        """B-012: Link if document references another by filename (strength=0.8).

        Limited to recent 200 summaries for performance.
        """
        import re as _re

        rows = self.db.execute(
            "SELECT id, source FROM summaries WHERE id != ? ORDER BY id DESC LIMIT 200",
            (summary_id,)
        ).fetchall()

        text_lower = text.lower()
        for row in rows:
            other_id = row["id"]
            other_source = row["source"]
            if not other_source:
                continue

            filename = other_source.replace("\\", "/").split("/")[-1]
            filename_no_ext = filename.rsplit(".", 1)[0] if "." in filename else filename

            patterns = [
                filename.lower(),
                filename_no_ext.replace("_", " ").lower(),
                filename_no_ext.replace("-", " ").lower(),
            ]

            found = any(
                _re.search(r"\b" + _re.escape(p) + r"\b", text_lower)
                for p in patterns if len(p) > 5  # skip very short names
            )

            if found:
                existing = self.db.execute(
                    "SELECT id FROM document_links WHERE source_id = ? AND target_id = ? AND link_type = 'referenced_in'",
                    (summary_id, other_id)
                ).fetchone()
                if not existing:
                    self.db.execute(
                        """INSERT INTO document_links (source_id, target_id, link_type, strength, discovered_by)
                           VALUES (?, ?, 'referenced_in', 0.8, 'system')""",
                        (summary_id, other_id)
                    )

    # ── Chunk size for large file ingestion ────────────────────────────────
    CHUNK_SIZE = 100_000  # 100KB per chunk (characters)

    def store(self, text: str, source: str, author: str = "",
              channel: str = "", metadata: dict | None = None,
              summary_text: str | None = None) -> int:
        """Annotate text, create all 3 tiers, link them. Returns summary_id.

        Large files (>CHUNK_SIZE chars) are automatically split into chunks.
        Each chunk is stored independently with its own annotations/summary,
        linked via a shared chunk_group UUID in metadata for reassembly.
        """
        # Route large content to chunked ingestion
        if len(text) > self.CHUNK_SIZE:
            return self._store_chunked(text, source, author, channel, metadata, summary_text)

        return self._store_single(text, source, author, channel, metadata, summary_text)

    def _store_single(self, text: str, source: str, author: str = "",
                      channel: str = "", metadata: dict | None = None,
                      summary_text: str | None = None) -> int:
        """Store a single piece of content (<=CHUNK_SIZE). Returns summary_id."""
        annotations = uaimc_anno.annotate(text, source=source, author=author, metadata=metadata)
        annotations = uaimc_anno.apply_tfidf(annotations, db_conn=self.db)
        summary = summary_text or uaimc_anno.make_summary(text)
        meta_json = json.dumps(metadata, default=str) if metadata else None

        # Content hash for dedup (verbatim layer)
        content_hash = str(uaimc_anno.fnv1a_hash(text.strip().lower()))

        # Summary hash for dedup (summary layer — catches different content with identical summaries)
        summary_hash = str(uaimc_anno.fnv1a_hash(summary.strip().lower()))

        # B-009: Extract generation date for temporal intent scoring
        generated_at = self._extract_generation_date(text, metadata, source)

        # B4.3: Compute aspects early for Bloom + convergence check (BEFORE write lock)
        aspects = uaimc_anno.compute_aspects(annotations, source, author, metadata)

        # B4.3: Bloom Cascade pre-screen
        bloom_result = self._bloom_cascade.check_and_register(aspects) if self._bloom_cascade else "new"
        if bloom_result == "likely_dup":
            # Fast path: Bloom says likely duplicate. Confirm with convergence check.
            decision, existing_sid = self._convergence_ingest_decision(aspects)
            if decision == "skip":
                logger.info(f"B4.3: Semantic dedup SKIP (bloom=likely_dup, "
                            f"convergence=4+/5, matches sid={existing_sid})")
                return -2
            elif decision == "merge":
                logger.info(f"B4.3: Semantic dedup MERGE (bloom=likely_dup, "
                            f"convergence=3/5, merging into sid={existing_sid})")
                return self._merge_into_existing(existing_sid, annotations, source)

        with self._write_lock:  # B10: thread-safe writes
          try:
            cursor = self.db.cursor()

            # Check for duplicate verbatim content
            existing = cursor.execute(
                "SELECT id FROM verbatim WHERE content_hash = ?", (content_hash,)
            ).fetchone()
            if existing:
                return -2  # Already stored

            # Check for duplicate summary (different content, same summary)
            existing_summary = cursor.execute(
                "SELECT id FROM summaries WHERE summary_hash = ?", (summary_hash,)
            ).fetchone()
            if existing_summary:
                return -2  # Summary already exists from different content

            cursor.execute(
                "INSERT INTO verbatim (content, content_hash, source, author, channel, metadata, byte_size) "
                "VALUES (?,?,?,?,?,?,?)",
                (text, content_hash, source, author, channel, meta_json,
                 len(text.encode("utf-8"))),
            )
            verbatim_id = cursor.lastrowid

            cursor.execute(
                "INSERT INTO summaries (content, verbatim_id, source, metadata, summary_hash, generated_at) "
                "VALUES (?,?,?,?,?,?)",
                (summary, verbatim_id, source, meta_json, summary_hash, generated_at),
            )
            summary_id = cursor.lastrowid

            # Batch insert annotations for performance
            if annotations:
                cursor.executemany(
                    "INSERT INTO annotations (token, token_hash, weight, summary_id, source) VALUES (?,?,?,?,?)",
                    [(anno.token, anno.token_hash, anno.weight, summary_id, source) for anno in annotations],
                )

            # B-012: Create auto-links to related documents
            self._create_auto_links(summary_id, source, text, metadata)

            # OPT-017: Pre-compute top-N neighbors for this document
            self._compute_top_neighbors(summary_id)

            # B-014: Store aspect fingerprint (aspects computed before write lock)
            cursor.execute(
                "INSERT OR REPLACE INTO aspect_index "
                "(summary_id, sem_hash, source_hash, agent_hash, intent_hash, project_hash) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (summary_id, aspects["sem_hash"], aspects["source_hash"],
                 aspects["agent_hash"], aspects["intent_hash"], aspects["project_hash"]),
            )

            self.db.commit()

            # OPT-015: Invalidate IDF cache on new data
            self._idf_cache.clear()
            self._corpus_stats_cache = None

            # B4.2: Push aspects to in-memory inverted index
            self._aspect_index.add(summary_id, aspects)

            # B4.4: Push to GPU-Triad index
            if self._gpu_triad and self._gpu_triad.enabled:
                self._gpu_triad.add(summary_id, aspects)

            # B4.3: Save Bloom cascade periodically (every 100 new docs)
            if self._bloom_cascade and self._aspect_index.size % 100 == 0:
                self._save_bloom()

            # Push to GPU-AM (if enabled)
            if self._gpu and self._gpu.enabled:
                gpu_entries = [(anno.token_hash, summary_id, anno.weight) for anno in annotations]
                self._gpu.add_batch(gpu_entries)

            return summary_id

          except Exception as e:
            logger.error(f"store failed: {e}")
            self.db.rollback()
            return -1

    def _store_chunked(self, text: str, source: str, author: str = "",
                       channel: str = "", metadata: dict | None = None,
                       summary_text: str | None = None) -> int:
        """Store large content in chunks. Each chunk is independently searchable.

        Chunks share a chunk_group UUID in metadata for reassembly.
        Each chunk is marked with its range: [CHUNK N/M of filename | chars start-end]
        Returns the summary_id of the FIRST chunk (or -2 if all duplicate).
        """
        filename = (metadata or {}).get("filename", "unknown")
        chunk_group = str(uuid.uuid4())
        total_chars = len(text)

        # Split on paragraph/line boundaries when possible
        chunks = self._split_into_chunks(text, self.CHUNK_SIZE)
        total_chunks = len(chunks)
        first_summary_id = None
        char_offset = 0

        logger.info(f"Chunked ingest: {filename} ({total_chars:,} chars) → {total_chunks} chunks")

        for i, chunk_text in enumerate(chunks, 1):
            chunk_start = char_offset
            chunk_end = char_offset + len(chunk_text) - 1
            char_offset += len(chunk_text)

            # Build chunk metadata
            chunk_meta = dict(metadata) if metadata else {}
            chunk_meta["chunk_group"] = chunk_group
            chunk_meta["chunk_index"] = i
            chunk_meta["chunk_total"] = total_chunks
            chunk_meta["chunk_range"] = f"chars {chunk_start}-{chunk_end}"
            chunk_meta["total_file_chars"] = total_chars
            chunk_meta["chunk_marker"] = f"[CHUNK {i}/{total_chunks} of {filename} | chars {chunk_start}-{chunk_end}]"

            # Each chunk gets its own summary
            chunk_summary = summary_text if (i == 1 and summary_text) else None

            try:
                sid = self._store_single(
                    text=chunk_text,
                    source=source,
                    author=author,
                    channel=channel,
                    metadata=chunk_meta,
                    summary_text=chunk_summary,
                )
            except Exception as e:
                logger.error(f"Chunk {i}/{total_chunks} of {filename} failed: {e}")
                sid = -1

            if first_summary_id is None and sid > 0:
                first_summary_id = sid

        if first_summary_id is None:
            return -2  # All chunks were duplicates

        logger.info(f"Chunked ingest complete: {filename} → {total_chunks} chunks stored (group={chunk_group[:8]}...)")
        return first_summary_id

    @staticmethod
    def _split_into_chunks(text: str, chunk_size: int) -> list[str]:
        """Split text into chunks, preferring paragraph/line boundaries."""
        if len(text) <= chunk_size:
            return [text]

        chunks = []
        remaining = text
        while remaining:
            if len(remaining) <= chunk_size:
                chunks.append(remaining)
                break

            # Try to split at a paragraph boundary (double newline)
            candidate = remaining[:chunk_size]
            split_pos = candidate.rfind("\n\n")

            # Fallback: single newline
            if split_pos < chunk_size // 2:
                split_pos = candidate.rfind("\n")

            # Fallback: space
            if split_pos < chunk_size // 2:
                split_pos = candidate.rfind(" ")

            # Last resort: hard split at chunk_size
            if split_pos < chunk_size // 2:
                split_pos = chunk_size

            chunks.append(remaining[:split_pos])
            remaining = remaining[split_pos:].lstrip("\n")  # Don't start next chunk with blank lines

        return chunks

    def query(self, keywords: list[str], limit: int = 10, agent: str = "*") -> list[dict]:
        """GPU-first query with FTS5 fallback. Returns assembled results."""
        if not keywords:
            return []
        _qconn = self._pool.get()
        try:
            return self._query_inner(keywords, limit, agent, _qconn)
        finally:
            self._pool.put(_qconn)

    def _query_inner(self, keywords: list[str], limit: int, agent: str, _qconn) -> list[dict]:
        """Inner query logic using a dedicated read connection."""

        # B4.2/B4.4 Stage 0: Triangulation pre-filter (GPU-Triad → CPU fallback)
        query_text_joined = " ".join(keywords)
        query_annos = uaimc_anno.annotate(query_text_joined, source="query", author=agent)
        query_aspects = uaimc_anno.compute_aspects(query_annos, source="query", author=agent)
        tri_limit = max(limit * 5, 50)
        low_signal_query = len(query_annos) == 0

        # OPT-009: Lock only GPU-unsafe operations (triad + GPU-AM tensors)
        # Use timeout so queries fall through to FTS5 if GPU is busy
        tri_candidates = None
        gpu_results = None
        self._tls.tri_source = "none"
        self._tls.tri_candidates = None
        self._tls.tri_scores = None
        _got_gpu = self._gpu_lock.acquire(timeout=2)
        try:
            if _got_gpu:
                # B4.4: Try GPU-Triad first, fall back to CPU AspectInvertedIndex
                if self._gpu_triad and self._gpu_triad.enabled and not low_signal_query:
                    tri_candidates = self._gpu_triad.triangulate(
                        query_aspects,
                        min_convergence=2,
                        max_results=tri_limit,
                    )
                    # Also try fuzzy hamming search for near-semantic matches
                    _scoring = CONFIG.get("scoring", {})
                    max_ham = int(_scoring.get("simhash_max_hamming", 6))
                    fuzzy_results = self._gpu_triad.hamming_triangulate(
                        query_aspects["sem_hash"],
                        query_aspects,
                        max_hamming=max_ham,
                        min_convergence=2,
                        max_results=tri_limit,
                    )
                    if fuzzy_results:
                        # Merge: union of exact + fuzzy, keep highest count per sid
                        merged = {sid: count for sid, count in tri_candidates}
                        for sid, count in fuzzy_results:
                            if sid not in merged or count > merged[sid]:
                                merged[sid] = count
                        tri_candidates = sorted(merged.items(), key=lambda x: x[1], reverse=True)[:tri_limit]
                    if tri_candidates:
                        self._tls.tri_source = "gpu"

                if not tri_candidates and not low_signal_query:
                    tri_candidates = self._aspect_index.triangulate(query_aspects, min_convergence=2)
                    if tri_candidates:
                        tri_candidates = tri_candidates[:tri_limit]
                        self._tls.tri_source = "cpu"

                # GPU-AM path: hash keywords, query GPU tensors
                if self._gpu and self._gpu.enabled:
                    kw_set = {kw.lower().strip() for kw in keywords if kw.strip()}
                    code_intent = any(k in kw_set for k in {"python", "code", ".py", "script", "function", "module"})
                    drgfc_focus = "drgfc" in kw_set

                    gpu_fetch_limit = limit * 2  # default
                    if code_intent and drgfc_focus:
                        gpu_fetch_limit = max(limit * 300, 3000)
                    elif code_intent:
                        gpu_fetch_limit = max(limit * 20, 200)

                    token_hashes = [uaimc_anno.fnv1a_hash(kw.lower().strip()) for kw in keywords if kw.strip()]
                    gpu_results = self._gpu.query(token_hashes, limit=gpu_fetch_limit)  # targeted over-fetch for reranking
            else:
                logger.debug("GPU lock busy — falling through to FTS5 for: %s", " ".join(keywords))
        finally:
            if _got_gpu:
                self._gpu_lock.release()
        # END gpu_lock — all remaining work is thread-safe (pool connections + thread-local state)

        if tri_candidates:
            tri_sids = {sid for sid, _ in tri_candidates[:tri_limit]}
            self._tls.tri_candidates = tri_sids
            self._tls.tri_scores = {sid: count for sid, count in tri_candidates}

        # GPU-AM results: resolve and score (uses pool connections, thread-safe)
        if gpu_results:
            gpu_sids = [gr["summary_id"] for gr in gpu_results]
            weight_by_sid = {gr["summary_id"]: gr["weight"] for gr in gpu_results}
            sid_placeholders = ",".join("?" * len(gpu_sids))

            # Batch fetch summaries (OPT-019: compute age_hours in SQL via julianday)
            sum_rows = _qconn.execute(
                f"SELECT id, content, metadata, source, created_at, verbatim_id, generated_at, "
                f"(julianday('now') - julianday(COALESCE(generated_at, created_at))) * 24 AS age_hours "
                f"FROM summaries WHERE id IN ({sid_placeholders})",
                gpu_sids
            ).fetchall()
            sum_map = {r["id"]: r for r in sum_rows}

            # Batch fetch verbatim for rows that have verbatim_id
            verb_ids = [r["verbatim_id"] for r in sum_rows if r["verbatim_id"]]
            verb_map: dict[int, dict] = {}
            if verb_ids:
                vp = ",".join("?" * len(verb_ids))
                v_rows = _qconn.execute(
                    f"SELECT id, content, author, channel FROM verbatim WHERE id IN ({vp})",
                    verb_ids
                ).fetchall()
                verb_map = {r["id"]: r for r in v_rows}

            resolved = []
            for sid in gpu_sids:
                row = sum_map.get(sid)
                if not row:
                    continue
                # Compress GPU-AM weight with log1p to prevent huge documents
                # from dominating.  Raw weights scale with document size (sum of
                # all matching annotation weights), so a 1.7 MB file can produce
                # weights of ~100 000+ while a focused small doc scores ~10.
                # log1p preserves ordering but compresses the range:
                #   10 → 2.4,  100 → 4.6,  1000 → 6.9,  100000 → 11.5
                raw_w = weight_by_sid.get(sid, 0)
                if not isinstance(raw_w, (int, float)) or math.isinf(raw_w) or math.isnan(raw_w):
                    raw_w = 0.0
                compressed_w = math.log1p(max(raw_w, 0))
                result = {
                    "summary_id": row["id"],
                    "summary": row["content"],
                    "source": row["source"],
                    "created_at": row["created_at"],
                    "generated_at": row["generated_at"],
                    "age_hours": row["age_hours"],  # OPT-019: pre-computed in SQL
                    "matched_tokens": ", ".join(keywords),
                    "relevance_score": compressed_w,
                    "query_method": "gpu",
                }
                if row["metadata"]:
                    try:
                        result["metadata"] = json.loads(row["metadata"])
                    except json.JSONDecodeError:
                        result["metadata"] = {}
                else:
                    result["metadata"] = {}
                if row["verbatim_id"]:
                    v_row = verb_map.get(row["verbatim_id"])
                    if v_row:
                        content = v_row["content"]
                        result["verbatim_preview"] = content[:200] + ("..." if len(content) > 200 else "")
                        result["author"] = v_row["author"]
                        result["channel"] = v_row["channel"]
                resolved.append(result)
            if resolved:
                # OPT-005 + OPT-010: Parallel prefetch ALL scoring data using connection pool
                resolved_sids = [r["summary_id"] for r in resolved]
                seed_ids = [r["summary_id"] for r in resolved[:5] if r.get("summary_id")]

                with ThreadPoolExecutor(max_workers=4) as executor:
                    # Use pool connections for ALL independent DB queries
                    c0 = self._pool.get()
                    c1 = self._pool.get()
                    c2 = self._pool.get()
                    c3 = self._pool.get()
                    try:
                        f_anno = executor.submit(self._batch_prefetch_annotations, resolved_sids, c0)
                        f_sel = executor.submit(self._batch_prefetch_selection, resolved_sids, c1)
                        f_neg = executor.submit(self._batch_prefetch_negation, resolved_sids, c2)
                        f_graph = executor.submit(self._batch_prefetch_graph, seed_ids, 2, c3)
                        prefetched = f_anno.result()
                        prefetched["sel_map"] = f_sel.result()
                        prefetched["rej_map"] = f_neg.result()
                        prefetched["graph_map"] = f_graph.result()
                    finally:
                        self._pool.put(c0)
                        self._pool.put(c1)
                        self._pool.put(c2)
                        self._pool.put(c3)

                # Scoring pipeline: all methods use prefetched data (no DB I/O)
                _t0 = time.time()
                resolved = self._apply_coverage_ratio(resolved, keywords, prefetched=prefetched)
                resolved = self._apply_heat_map_boost(resolved, keywords, agent, prefetched=prefetched, conn=_qconn)
                resolved = self._apply_bm25_scoring(resolved, keywords, prefetched=prefetched)
                resolved = self._apply_convergence_boost(resolved)
                resolved = self._apply_length_normalization(resolved, keywords, prefetched=prefetched)
                resolved = self._apply_ppr_boost(resolved, keywords, prefetched=prefetched, agent_name=agent)
                resolved = self._apply_temporal_scoring(resolved, keywords)
                resolved = self._apply_selection_boost(resolved, keywords, prefetched=prefetched)
                resolved = self._apply_negation_score(resolved, keywords, prefetched=prefetched)
                resolved = self._apply_source_authority(resolved, keywords)

                # Phase 6: Cross-agent knowledge discovery injection (GPU path)
                if agent and agent != "*":
                    cross_results = self.get_cross_agent_results(keywords, agent, limit=5, conn=_qconn)
                    if cross_results:
                        existing_by_sid = {r.get("summary_id"): r for r in resolved}
                        cross_sid_set = {cr["summary_id"] for cr in cross_results}
                        cross_author_map = {cr["summary_id"]: cr.get("author", "") for cr in cross_results}
                        for sid in cross_sid_set:
                            if sid in existing_by_sid:
                                existing_by_sid[sid]["cross_agent_source"] = True
                                existing_by_sid[sid]["author"] = cross_author_map.get(sid, "")
                        for cr in cross_results:
                            if cr["summary_id"] not in existing_by_sid:
                                resolved.append(cr)

                resolved.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
                self._log_scoring_breakdown(resolved, keywords, (time.time() - _t0) * 1000)
                return resolved[:limit]
            # GPU returned summary_ids but none resolved — fall through to FTS5

        # FTS5 fallback
        try:
            def _quote_fts5(kw: str) -> str:
                """Quote keyword for literal FTS5 matching (escapes reserved words)."""
                escaped = kw.replace('"', '""')
                return f'"{ escaped }"'

            fts_query = " OR ".join(_quote_fts5(kw.lower().strip()) for kw in keywords if kw.strip())
            if not fts_query:
                return []

            rows = _qconn.execute("""
                SELECT DISTINCT s.id, s.content, s.metadata, s.source, s.created_at,
                       s.verbatim_id, s.generated_at,
                       GROUP_CONCAT(a.token, ', ') as matched_tokens,
                       SUM(a.weight) as total_weight
                FROM annotations_fts f
                JOIN annotations a ON a.id = f.rowid
                JOIN summaries s ON s.id = a.summary_id
                WHERE annotations_fts MATCH ?
                GROUP BY s.id
                ORDER BY total_weight DESC
                LIMIT ?
            """, (fts_query, limit)).fetchall()

            results = []
            for row in rows:
                # Compress FTS5 total_weight with log1p (same as GPU path)
                raw_fts_w = row["total_weight"] or 0
                if not isinstance(raw_fts_w, (int, float)) or math.isinf(raw_fts_w) or math.isnan(raw_fts_w):
                    raw_fts_w = 0.0
                compressed_fts_w = math.log1p(max(raw_fts_w, 0))
                result = {
                    "summary_id": row["id"],
                    "summary": row["content"],
                    "source": row["source"],
                    "created_at": row["created_at"],
                    "generated_at": row["generated_at"],
                    "matched_tokens": row["matched_tokens"],
                    "relevance_score": compressed_fts_w,
                    "query_method": "fts5",
                }
                if row["metadata"]:
                    try:
                        result["metadata"] = json.loads(row["metadata"])
                    except json.JSONDecodeError:
                        result["metadata"] = {}
                else:
                    result["metadata"] = {}

                if row["verbatim_id"]:
                    v_row = _qconn.execute(
                        "SELECT content, author, channel FROM verbatim WHERE id = ?",
                        (row["verbatim_id"],)
                    ).fetchone()
                    if v_row:
                        content = v_row["content"]
                        result["verbatim_preview"] = content[:200] + ("..." if len(content) > 200 else "")
                        result["author"] = v_row["author"]
                        result["channel"] = v_row["channel"]

                result["query_method"] = "fts5"
                results.append(result)

            # OPT-010: Prefetch scoring data for FTS5 path too (thread-safe with pool connections)
            if results:
                resolved_sids = [r["summary_id"] for r in results if r.get("summary_id")]
                seed_ids = [r["summary_id"] for r in results[:5] if r.get("summary_id")]
                with ThreadPoolExecutor(max_workers=4) as executor:
                    c0 = self._pool.get()
                    c1 = self._pool.get()
                    c2 = self._pool.get()
                    c3 = self._pool.get()
                    try:
                        f_anno = executor.submit(self._batch_prefetch_annotations, resolved_sids, c0)
                        f_sel = executor.submit(self._batch_prefetch_selection, resolved_sids, c1)
                        f_neg = executor.submit(self._batch_prefetch_negation, resolved_sids, c2)
                        f_graph = executor.submit(self._batch_prefetch_graph, seed_ids, 2, c3)
                        prefetched = f_anno.result()
                        prefetched["sel_map"] = f_sel.result()
                        prefetched["rej_map"] = f_neg.result()
                        prefetched["graph_map"] = f_graph.result()
                    finally:
                        self._pool.put(c0)
                        self._pool.put(c1)
                        self._pool.put(c2)
                        self._pool.put(c3)
            else:
                prefetched = None

            # Scoring pipeline: Coverage → Heat → BM25 → Convergence → LengthNorm → PPR(GAAMA) → Temporal → Selection → Negation → SourceAuth → CodeIntent
            _t0 = time.time()
            results = self._apply_coverage_ratio(results, keywords, prefetched=prefetched)
            results = self._apply_heat_map_boost(results, keywords, agent, prefetched=prefetched, conn=_qconn)
            results = self._apply_bm25_scoring(results, keywords, prefetched=prefetched)
            results = self._apply_convergence_boost(results)
            results = self._apply_length_normalization(results, keywords, prefetched=prefetched)
            results = self._apply_ppr_boost(results, keywords, prefetched=prefetched, agent_name=agent)
            results = self._apply_temporal_scoring(results, keywords)
            results = self._apply_selection_boost(results, keywords, prefetched=prefetched)
            results = self._apply_negation_score(results, keywords, prefetched=prefetched)
            results = self._apply_source_authority(results, keywords)
            results = self._apply_code_intent_boost(results, keywords)

            # Phase 6: Cross-agent knowledge discovery injection
            if agent and agent != "*":
                cross_results = self.get_cross_agent_results(keywords, agent, limit=5, conn=_qconn)
                if cross_results:
                    # Build lookup of existing results by summary_id
                    existing_by_sid = {r.get("summary_id"): r for r in results}
                    cross_sid_set = {cr["summary_id"] for cr in cross_results}
                    cross_author_map = {cr["summary_id"]: cr.get("author", "") for cr in cross_results}
                    # Annotate existing results that also appear in cross-agent
                    for sid in cross_sid_set:
                        if sid in existing_by_sid:
                            existing_by_sid[sid]["cross_agent_source"] = True
                            existing_by_sid[sid]["author"] = cross_author_map.get(sid, "")
                    # Add truly new cross-agent results not in main results
                    for cr in cross_results:
                        if cr["summary_id"] not in existing_by_sid:
                            results.append(cr)

            results.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
            self._log_scoring_breakdown(results, keywords, (time.time() - _t0) * 1000)
            return results

        except Exception as e:
            logger.error(f"query failed: {e}")
            return []

    def query_text(self, text: str, limit: int = 10, agent: str = "*") -> list[dict]:
        """Extract keywords from text, then query.
        Phase 6: Apply agent-specific keyword weighting for novelty/discovery balance."""
        annotations = uaimc_anno.annotate(text)
        keywords = [a.token for a in annotations[:8]]
        if not keywords:
            keywords = [w.lower() for w in text.split()[:5] if len(w) >= 3]
        # Phase 6: Re-order keywords by novelty for this agent
        if agent and agent != "*":
            keywords = self.apply_agent_keyword_weighting(keywords, agent)
        return self.query(keywords, limit=limit, agent=agent)

    def _context_cache_get(self, key: tuple) -> str | None:
        """Return cached context string if within TTL, else None."""
        entry = self._context_cache.get(key)
        if entry is None:
            self._context_cache_misses += 1
            return None
        ts, value = entry
        if (time.time() - ts) > self._context_cache_ttl:
            del self._context_cache[key]
            self._context_cache_misses += 1
            return None
        self._context_cache_hits += 1
        return value

    def _context_cache_put(self, key: tuple, value: str) -> None:
        """Store a context result in cache. Evicts oldest if over capacity."""
        if len(self._context_cache) >= self._context_cache_max:
            oldest_key = min(self._context_cache, key=lambda k: self._context_cache[k][0])
            del self._context_cache[oldest_key]
        self._context_cache[key] = (time.time(), value)

    def invalidate_context_cache(self) -> int:
        """Clear the context cache. Returns number of evicted entries."""
        n = len(self._context_cache)
        self._context_cache.clear()
        self._context_layers_cache.clear()
        return n

    def invalidate_context_cache_for_agent(self, agent_name: str) -> int:
        """Phase 5: Clear cached context entries for a specific agent. Returns evicted count."""
        if not agent_name:
            return self.invalidate_context_cache()
        to_remove = [k for k in self._context_cache if k[0] == agent_name]
        for k in to_remove:
            del self._context_cache[k]
            self._context_layers_cache.pop(k, None)
        return len(to_remove)

    def get_context_for_prompt(self, agent: str = "", topic: str = "",
                                max_chars: int = CONTEXT_LIMIT) -> str:
        """Assemble relevant context for an agent's prompt injection.

        The Context Recycler: returns formatted memory block ready
        for insertion into any agent's system prompt.
        Phase B1-Step1: TTL-based result cache keyed on (agent, topic, max_chars).
        """
        _ctx_t0 = time.time()
        logger.info("get_context_for_prompt START: agent=%s topic=%s max_chars=%d", agent, topic, max_chars)
        # Phase B1-Step1: Check cache first
        cache_key = (agent, topic, max_chars)
        cached = self._context_cache_get(cache_key)
        if cached is not None:
            return cached

        query_parts = []
        if topic:
            query_parts.append(topic)
        if agent:
            query_parts.append(agent)

        if not query_parts:
            # No topic — return most recent entries
            recent = self.get_recent(limit=10)
            if not recent:
                return ""
            source_count = len(recent)
            lines = [f"\n--- UAIMC MEMORY RECALL [UAIMC: {source_count} sources] ---"]
            char_count = len(lines[0])
            for r in recent:
                entry = f"\n[{r['source'].upper()}] ({r['created_at'][:10]}) {r['summary']}"
                if char_count + len(entry) > max_chars:
                    break
                lines.append(entry)
                char_count += len(entry)
            lines.append(f"\n[UAIMC: {len(lines) - 1} entries | Recent activity]")
            lines.append("--- END MEMORY RECALL ---\n")
            result = "\n".join(lines)
            self._context_cache_put(cache_key, result)
            return result

        query_text = " ".join(query_parts)
        results = self.query_text(query_text, limit=15, agent=agent)
        logger.info("get_context_for_prompt: query_text returned %d results in %.0fms", len(results) if results else 0, (time.time() - _ctx_t0) * 1000)
        if not results:
            return ""

        # Phase 3 GAAMA: Route through CANS context assembly when populated
        # Uses pool connections to avoid blocking on self.db write contention
        try:
            _cans_conn = self._pool.get()
            try:
                _cans_pop = self._is_cans_populated(conn=_cans_conn)
            finally:
                self._pool.put(_cans_conn)
            if _cans_pop:
                assembled = self._apply_context_assembly(results, agent_mode="agent", agent_name=agent)
                cans_context = assembled.get("context", "")
                if cans_context:
                    stats = assembled.get("pack_stats", {})
                    f_count = stats.get("facts_capped", 0)
                    r_count = stats.get("reflections_capped", 0)
                    e_count = stats.get("episodes_packed", 0)
                    source_count = f_count + r_count + e_count
                    agent_label = f" | Agent: {agent}" if agent else ""
                    topic_label = f" | Topic: {topic[:50]}" if topic else ""
                    result = (
                        f"\n--- UAIMC MEMORY RECALL [UAIMC: {source_count} sources | CANS] ---\n"
                        f"{cans_context}\n"
                        f"\n[UAIMC: {source_count} entries | facts={f_count} reflections={r_count} episodes={e_count}{agent_label}{topic_label}]\n"
                        f"--- END MEMORY RECALL ---\n"
                    )
                    self._context_cache_put(cache_key, result)
                    # Phase 5: Store disclosure layers for /context?layers= support
                    _disc_layers = assembled.get("disclosure_layers", [])
                    if _disc_layers:
                        self._context_layers_cache[cache_key] = (time.time(), _disc_layers)
                    return result
        except Exception as e:
            logger.warning(f"CANS context assembly failed (falling back to legacy): {e}")

        lines = ["\n--- UAIMC MEMORY RECALL [UAIMC: pending] ---"]
        char_count = len(lines[0])
        n = len(results)

        # B-TC-004β: Config-driven compression with per-agent overrides
        _ctx_cfg = CONFIG.get("context", {})
        _agent_overrides = _ctx_cfg.get("agent_overrides", {}).get(agent, {})
        compression_level = _agent_overrides.get("compression_level", _ctx_cfg.get("compression_level", 2))
        full_verbatim_pct = _agent_overrides.get("full_verbatim_pct", _ctx_cfg.get("full_verbatim_pct", 0.3))
        summary_only_pct = _agent_overrides.get("summary_only_pct", _ctx_cfg.get("summary_only_pct", 0.3))
        mid_truncate_chars = _agent_overrides.get("mid_truncate_chars", _ctx_cfg.get("mid_truncate_chars", 100))
        mid_threshold = 1.0 - summary_only_pct  # e.g., 0.7 if summary_only_pct=0.3

        for i, r in enumerate(results):
            pct = i / n if n > 1 else 0  # 0 = top result, approaching 1 = worst
            author_str = f" [{r.get('author', '')}]" if r.get("author") else ""
            channel_str = f" #{r.get('channel', '')}" if r.get("channel") else ""

            # B-TC-004β: Config-driven tiered content inclusion
            if compression_level == 0:
                # No compression: always include full verbatim
                detail = r.get("verbatim_preview", "")
                content = r["summary"]
                if detail and detail not in content:
                    content = f"{r['summary']}\n  > {detail}"
            elif compression_level == 1:
                # Mild: top full_verbatim_pct get full verbatim, rest get summary + truncated
                detail = r.get("verbatim_preview", "")
                content = r["summary"]
                if pct < full_verbatim_pct:
                    if detail and detail not in content:
                        content = f"{r['summary']}\n  > {detail}"
                elif detail and len(detail) > mid_truncate_chars:
                    content = f"{r['summary']}\n  > {detail[:mid_truncate_chars]}..."
                elif detail and detail not in content:
                    content = f"{r['summary']}\n  > {detail}"
            else:
                # Level 2 (default): 3-tier — full / truncated / summary-only
                if pct < full_verbatim_pct:
                    detail = r.get("verbatim_preview", "")
                    content = r["summary"]
                    if detail and detail not in content:
                        content = f"{r['summary']}\n  > {detail}"
                elif pct < mid_threshold:
                    detail = r.get("verbatim_preview", "")
                    content = r["summary"]
                    if detail and len(detail) > mid_truncate_chars:
                        content = f"{r['summary']}\n  > {detail[:mid_truncate_chars]}..."
                    elif detail and detail not in content:
                        content = f"{r['summary']}\n  > {detail}"
                else:
                    content = r["summary"]

            entry = f"\n[{r['source'].upper()}]{author_str}{channel_str} ({r['created_at'][:10]}) {content}"

            if char_count + len(entry) > max_chars:
                break
            lines.append(entry)
            char_count += len(entry)

        if len(lines) <= 1:
            return ""

        source_count = len(lines) - 1
        agent_label = f" | Agent: {agent}" if agent else ""
        topic_label = f" | Topic: {topic[:50]}" if topic else ""
        lines[0] = f"\n--- UAIMC MEMORY RECALL [UAIMC: {source_count} sources] ---"
        lines.append(f"\n[UAIMC: {source_count} entries{agent_label}{topic_label}]")
        lines.append("--- END MEMORY RECALL ---\n")
        result = "\n".join(lines)

        # Phase B1-Step1: Store in cache
        self._context_cache_put(cache_key, result)
        return result

    def log_agent_activity(self, agent_name: str, action: str, details: str = ""):
        """Log agent activity (query, ingest, context request)."""
        with self._write_lock:  # B10: thread-safe writes
            try:
                self.db.execute(
                    "INSERT INTO agent_activity (agent_name, action, details) VALUES (?,?,?)",
                    (agent_name, action, details),
                )
                self.db.commit()
            except Exception as e:
                logger.error(f"log_agent_activity failed: {e}")

        # Phase 6: Track query sequences for context prediction
        if action in ("query", "context") and agent_name and details:
            topic_snippet = details.split("=", 1)[1][:60] if "=" in details else details[:60]
            seq = self._agent_query_sequences.setdefault(agent_name, [])
            seq.append(topic_snippet)
            if len(seq) > self._agent_query_seq_max:
                seq.pop(0)
            # Invalidate topic priors cache if this agent isn't tracked yet
            if agent_name not in self._agent_topic_priors:
                self._agent_topic_priors_ts = 0.0

    # ── Phase 6: Agent-Aware Query Enhancement ──────────────────────────

    def get_agent_topic_priors(self, agent_name: str = "", top_n: int = 5) -> dict:
        """Phase 6 SC-01: Mine agent_activity for top topics per agent.

        Returns {agent: [(keyword, count), ...]} for the given agent,
        or all agents if agent_name is empty.
        Cached with TTL to avoid repeated DB scans.
        """
        now = time.time()
        if (now - self._agent_topic_priors_ts) < self._agent_topic_priors_ttl and self._agent_topic_priors:
            if agent_name:
                return {agent_name: self._agent_topic_priors.get(agent_name, [])}
            return self._agent_topic_priors

        # Mine the details column for query/context actions
        try:
            rows = self.db.execute(
                "SELECT agent_name, details FROM agent_activity "
                "WHERE action IN ('query', 'context') AND details IS NOT NULL AND details != '' "
                "ORDER BY timestamp DESC LIMIT 5000"
            ).fetchall()
        except Exception as e:
            logger.warning("get_agent_topic_priors DB query failed: %s", e)
            return {agent_name: []} if agent_name else {}

        # Extract keywords from details (format: "q=keyword text" or "topic=keyword text")
        from collections import Counter
        agent_keywords: dict[str, Counter] = {}
        stop_words = {"the", "a", "an", "is", "in", "at", "to", "for", "of", "and", "or", "on", "with", "by", "from", "as", "it"}
        for row in rows:
            aname = row["agent_name"]
            detail = row["details"]
            # Extract the text after q= or topic=
            if "=" in detail:
                text = detail.split("=", 1)[1]
            else:
                text = detail
            words = [w.lower().strip() for w in text.split() if len(w.strip()) >= 3 and w.lower().strip() not in stop_words]
            if aname not in agent_keywords:
                agent_keywords[aname] = Counter()
            agent_keywords[aname].update(words)

        priors = {}
        for aname, counter in agent_keywords.items():
            priors[aname] = counter.most_common(top_n)

        self._agent_topic_priors = priors
        self._agent_topic_priors_ts = now

        if agent_name:
            return {agent_name: priors.get(agent_name, [])}
        return priors

    def get_cross_agent_results(self, query_keywords: list[str], requesting_agent: str,
                                limit: int = 5, conn=None) -> list[dict]:
        """Phase 6 SC-02/SC-05: Surface items ingested by OTHER agents that match the query.

        Queries the summaries table joined with verbatim (for author) to find
        items authored by agents OTHER than the requesting agent.
        """
        if not query_keywords or not requesting_agent:
            return []

        db = conn or self.db
        # Build FTS query from keywords
        fts_terms = " OR ".join(f'"{kw}"' for kw in query_keywords[:6])
        if not fts_terms:
            return []

        try:
            rows = db.execute("""
                SELECT DISTINCT s.id as summary_id, s.content as summary, s.source,
                       s.created_at, s.generated_at, v.author, v.channel,
                       v.content as verbatim_content,
                       SUM(a.weight) as total_weight
                FROM annotations_fts fts
                JOIN annotations a ON a.id = fts.rowid
                JOIN summaries s ON s.id = a.summary_id
                LEFT JOIN verbatim v ON s.verbatim_id = v.id
                WHERE annotations_fts MATCH ?
                  AND v.author IS NOT NULL
                  AND v.author != ''
                  AND UPPER(v.author) != UPPER(?)
                GROUP BY s.id
                ORDER BY total_weight DESC
                LIMIT ?
            """, (fts_terms, requesting_agent, limit)).fetchall()

            results = []
            for row in rows:
                r = {
                    "summary_id": row["summary_id"],
                    "summary": row["summary"],
                    "source": row["source"],
                    "created_at": row["created_at"],
                    "generated_at": row["generated_at"],
                    "author": row["author"],
                    "channel": row["channel"] or "",
                    "cross_agent_source": True,
                    "relevance_score": 0.5,  # base score, will be boosted by scoring pipeline
                    "query_method": "cross_agent",
                }
                if row["verbatim_content"]:
                    vc = row["verbatim_content"]
                    r["verbatim_preview"] = vc[:200] + ("..." if len(vc) > 200 else "")
                results.append(r)
            return results
        except Exception as e:
            logger.warning("get_cross_agent_results failed: %s", e)
            return []

    def get_agent_affinity_config(self, agent_name: str) -> dict:
        """Phase 6 SC-03: Return PPR tuning parameters per agent.

        Different agents get different graph walk behaviors:
        - ORACLE: Broad discovery (higher max_hops, lower alpha = more exploration)
        - FORGE/CLIO: Tight technical focus (standard hops, standard alpha)
        - Default: Balanced
        """
        _ctx_cfg = CONFIG.get("context", {})
        _overrides = _ctx_cfg.get("agent_overrides", {}).get(agent_name, {})
        return {
            "ppr_hops": _overrides.get("ppr_hops", 2),
            "ppr_alpha": _overrides.get("ppr_alpha", 0.15),
            "ppr_weight": _overrides.get("ppr_weight", 0.3),
            "cross_agent_blend": _overrides.get("cross_agent_blend", 0.15),
        }

    def apply_agent_keyword_weighting(self, keywords: list[str], agent_name: str) -> list[str]:
        """Phase 6 SC-06: Weight keywords by agent history.

        If an agent frequently queries a keyword, lower its novelty (already served).
        Boost related but less-queried keywords for discovery.
        Returns re-ordered keyword list with novel keywords first.
        """
        if not agent_name or not keywords:
            return keywords

        priors = self.get_agent_topic_priors(agent_name)
        prior_topics = priors.get(agent_name, [])
        if not prior_topics:
            return keywords

        # Build frequency map from priors
        freq_map = {topic: count for topic, count in prior_topics}
        max_freq = max(freq_map.values()) if freq_map else 1

        # Score keywords: low frequency = high novelty
        scored = []
        for kw in keywords:
            freq = freq_map.get(kw.lower(), 0)
            novelty = 1.0 - (freq / max(max_freq, 1))
            scored.append((kw, novelty))

        # Sort by novelty (novel keywords first) but keep all keywords
        scored.sort(key=lambda x: x[1], reverse=True)
        return [kw for kw, _ in scored]

    def predict_next_queries(self, agent_name: str, top_n: int = 2) -> list[str]:
        """Phase 6 SC-07: Predict next queries from agent's query sequence.

        Uses simple bigram prediction: if agent queries A then B frequently,
        after seeing A again, predict B.
        """
        if not agent_name:
            return []

        seq = self._agent_query_sequences.get(agent_name, [])
        if len(seq) < 2:
            return []

        from collections import Counter
        # Build bigram counts from sequence
        bigrams: Counter = Counter()
        for i in range(len(seq) - 1):
            bigrams[(seq[i], seq[i + 1])] += 1

        # Find most likely next topics given the last query
        last_query = seq[-1]
        candidates = [(next_t, count) for (prev_t, next_t), count in bigrams.items()
                       if prev_t == last_query]
        candidates.sort(key=lambda x: x[1], reverse=True)
        return [t for t, _ in candidates[:top_n]]

    def _compute_stats_fresh(self) -> dict:
        """Compute stats from DB (no cache check). Used by background updater."""
        anno_count = self.db.execute("SELECT COUNT(*) FROM annotations").fetchone()[0]
        summary_count = self.db.execute("SELECT COUNT(*) FROM summaries").fetchone()[0]
        verbatim_count = self.db.execute("SELECT COUNT(*) FROM verbatim").fetchone()[0]
        activity_count = self.db.execute("SELECT COUNT(*) FROM agent_activity").fetchone()[0]

        sources = {}
        for row in self.db.execute(
            "SELECT source, COUNT(*) as cnt FROM summaries GROUP BY source ORDER BY cnt DESC"
        ).fetchall():
            sources[row["source"]] = row["cnt"]

        agents = {}
        for row in self.db.execute(
            "SELECT agent_name, COUNT(*) as cnt FROM agent_activity GROUP BY agent_name ORDER BY cnt DESC"
        ).fetchall():
            agents[row["agent_name"]] = row["cnt"]

        total_bytes = self.db.execute(
            "SELECT COALESCE(SUM(byte_size), 0) FROM verbatim"
        ).fetchone()[0]

        db_size = 0
        if os.path.exists(self.db_path):
            db_size = os.path.getsize(self.db_path)

        result = {
            "annotations": anno_count,
            "summaries": summary_count,
            "verbatim": verbatim_count,
            "agent_activities": activity_count,
            "sources": sources,
            "agents": agents,
            "total_verbatim_bytes": total_bytes,
            "db_file_size": db_size,
            "db_path": self.db_path,
        }

        # GPU-AM stats (RC)
        if self._gpu:
            result["gpu"] = self._gpu.stats()

        return result

    def stats(self) -> dict:
        """Return counts and status. OPT-024: reads from background-updated cache."""
        now = time.time()
        if _stats_cache["data"] and (now - _stats_cache["time"]) < _STATS_TTL:
            return _stats_cache["data"]
        # Fallback: compute on demand if background hasn't populated yet
        try:
            result = self._compute_stats_fresh()
            _stats_cache["data"] = result
            _stats_cache["time"] = time.time()
            return result
        except Exception as e:
            return {"error": str(e)}

    def get_recent(self, source: str | None = None, author: str | None = None,
                   limit: int = 20) -> list[dict]:
        """Return recent summaries with annotations."""
        try:
            conditions = []
            params: list = []
            if source:
                conditions.append("s.source = ?")
                params.append(source)
            if author:
                conditions.append("v.author = ?")
                params.append(author)

            where = ""
            join = ""
            if author:
                join = "JOIN verbatim v ON v.id = s.verbatim_id"
            if conditions:
                where = "WHERE " + " AND ".join(conditions)

            query = f"""
                SELECT s.id, s.content, s.source, s.metadata, s.created_at
                FROM summaries s {join}
                {where}
                ORDER BY s.created_at DESC LIMIT ?
            """
            params.append(limit)
            rows = self.db.execute(query, params).fetchall()

            results = []
            for row in rows:
                result = {
                    "summary_id": row["id"],
                    "summary": row["content"],
                    "source": row["source"],
                    "created_at": row["created_at"],
                }
                if row["metadata"]:
                    try:
                        result["metadata"] = json.loads(row["metadata"])
                    except json.JSONDecodeError:
                        result["metadata"] = {}

                annos = self.db.execute(
                    "SELECT token, weight FROM annotations WHERE summary_id = ? ORDER BY weight DESC",
                    (row["id"],)
                ).fetchall()
                result["annotations"] = [{"token": a["token"], "weight": a["weight"]} for a in annos]
                results.append(result)
            return results
        except Exception as e:
            logger.error(f"get_recent failed: {e}")
            return []

    def prune_weak_links(self, threshold: float = 0.1) -> int:
        """OPT-031: Remove links with strength below threshold. Returns count deleted."""
        count = self.db.execute(
            "SELECT COUNT(*) FROM document_links WHERE strength < ?", (threshold,)
        ).fetchone()[0]
        if count > 0:
            self.db.execute("DELETE FROM document_links WHERE strength < ?", (threshold,))
            self.db.commit()
            logger.info(f"OPT-031: Pruned {count} weak links (strength < {threshold})")
        return count

    def close(self):
        # OPT-013: Save Bloom cascade on shutdown
        if self._bloom_cascade:
            self._save_bloom()
            logger.info("Bloom cascade saved on shutdown")
        if self._gpu:
            self._gpu.shutdown()
        try:
            self._pool.close_all()
        except Exception:
            pass
        try:
            self.db.close()
        except Exception:
            pass


# ── Phase A4: Progressive Disclosure Session Manager (B-013) ────────────────

# In-memory storage for active disclosure sessions (keyed by session_id)
_disclosure_sessions: dict[str, "DisclosureSession"] = {}
_SESSION_TTL_SECONDS = 3600  # Sessions expire after 1 hour


class DisclosureSession:
    """Manages a single progressive disclosure session.

    A session starts when a user/agent queries and receives Layer 0 (annotations).
    As the user explores/ignores/rejects, the session tracks state and serves deeper layers.
    """

    def __init__(self, session_id: str, query_hash: str, seed_results: list[dict],
                 agent: str = "", db: sqlite3.Connection | None = None):
        self.session_id = session_id
        self.query_hash = query_hash
        self.agent = agent
        self.db = db
        self.current_layer = 0
        self.explored_ids: set[int] = set()
        self.ignored_ids: set[int] = set()
        self.rejected_ids: set[int] = set()
        self.convergence_trail: list[tuple[int, int]] = []
        self.seed_results = seed_results
        self.created_at = time.time()

    def get_layer_candidates(self, layer: int, parent_id: int | None = None) -> list[dict]:
        """Get candidates for a specific depth layer.

        Layer 0: annotations from seed results (top-K from scoring pipeline)
        Layer 1: summaries linked to explored annotations + more annotations
        Layer 2: verbatims linked to explored summaries
        Layer 3: everything above + hidden links (discovered_by='curator', explored=0)
        """
        candidates = []

        if layer == 0:
            for r in self.seed_results:
                sid = r.get("summary_id")
                if sid and sid not in self.rejected_ids:
                    state = self._get_link_state(sid)
                    if state == "once_thought":
                        continue
                    if state == "soft_suppressed" and int(hashlib.sha256(f"{self.session_id}{sid}".encode()).hexdigest(), 16) % 2 != 0:
                        continue  # Show 50% of the time (deterministic via sha256)
                    candidates.append({
                        "summary_id": sid,
                        "layer": 0,
                        "type": "annotation",
                        "annotations": r.get("matched_annotations", []),
                        "relevance_score": r.get("relevance_score", 0),
                        "link_state": state,
                    })

        elif layer == 1 and parent_id:
            rows = self.db.execute(
                """SELECT s.id, s.content, s.source
                   FROM summaries s
                   JOIN document_links dl ON (dl.target_id = s.id OR dl.source_id = s.id)
                   WHERE (dl.source_id = ? OR dl.target_id = ?)
                   AND dl.link_state IN ('active', 'soft_suppressed')
                   AND (dl.explored = 1 OR dl.link_type = 'shared_annotation')
                   ORDER BY dl.strength DESC LIMIT 10""",
                (parent_id, parent_id)
            ).fetchall()
            for row in rows:
                sid = row["id"]
                if sid not in self.rejected_ids and sid != parent_id:
                    content_text = row["content"]
                    candidates.append({
                        "summary_id": sid,
                        "layer": 1,
                        "type": "summary",
                        "summary_preview": (content_text or "")[:200],
                    })

        elif layer == 2 and parent_id:
            rows = self.db.execute(
                """SELECT v.id, v.content, v.source
                   FROM verbatim v
                   JOIN summaries s ON v.source = s.source
                   WHERE s.id = ?
                   LIMIT 5""",
                (parent_id,)
            ).fetchall()
            for row in rows:
                content = row["content"]
                candidates.append({
                    "summary_id": parent_id,
                    "layer": 2,
                    "type": "verbatim",
                    "verbatim_id": row["id"],
                    "verbatim_preview": (content or "")[:500],
                })

        elif layer == 3 and parent_id:
            rows = self.db.execute(
                """SELECT dl.target_id, dl.link_type, dl.strength, dl.discovered_by,
                          s.content
                   FROM document_links dl
                   JOIN summaries s ON dl.target_id = s.id
                   WHERE dl.source_id = ?
                   AND dl.explored = 0
                   AND dl.link_state IN ('active', 'soft_suppressed')
                   ORDER BY dl.strength DESC LIMIT 8""",
                (parent_id,)
            ).fetchall()
            for row in rows:
                tid = row["target_id"]
                if tid not in self.rejected_ids:
                    content_text = row["content"]
                    candidates.append({
                        "summary_id": tid,
                        "layer": 3,
                        "type": "hidden_link",
                        "link_type": row["link_type"],
                        "discovered_by": row["discovered_by"],
                        "summary_preview": (content_text or "")[:200],
                    })

        return candidates

    def record_response(self, summary_id: int, layer: int, response: str,
                        cohort_ids: list | None = None):
        """Record the user's response and update link states accordingly."""
        self.db.execute(
            """INSERT INTO presentation_log
               (session_id, query_hash, summary_id, layer, response, cohort_ids, agent)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (self.session_id, self.query_hash, summary_id, layer, response,
             json.dumps(cohort_ids or []), self.agent)
        )

        if response == "explore":
            self.explored_ids.add(summary_id)
            self.convergence_trail.append((summary_id, layer))
            # Mark links as explored
            self.db.execute(
                "UPDATE document_links SET explored = 1, last_explored_at = datetime('now') "
                "WHERE target_id = ? OR source_id = ?",
                (summary_id, summary_id)
            )
            # B-010 integration: generate selection signal
            self.db.execute(
                """INSERT INTO selection_log
                   (query_hash, summary_id, signal_type, agent, position)
                   VALUES (?, ?, 'user_selected', ?, ?)""",
                (self.query_hash, summary_id, self.agent, layer)
            )
            # B-011 integration: generate heat events for explored topics
            tokens = self.db.execute(
                "SELECT token FROM annotations WHERE summary_id = ?", (summary_id,)
            ).fetchall()
            for tok in tokens:
                t = tok[0] if not isinstance(tok, sqlite3.Row) else tok["token"]
                self.db.execute(
                    "INSERT INTO topic_heat (topic_token, agent, heat_type, weight) "
                    "VALUES (?, ?, 'selection', 2.0)",
                    (t, self.agent)
                )

        elif response == "ignore":
            self.ignored_ids.add(summary_id)
            self.db.execute(
                "UPDATE document_links SET presentation_count = presentation_count + 1, "
                "ignore_count = ignore_count + 1 WHERE target_id = ? OR source_id = ?",
                (summary_id, summary_id)
            )

        elif response == "reject":
            self.rejected_ids.add(summary_id)
            self.db.execute(
                "UPDATE document_links SET rejection_count = rejection_count + 1, "
                "last_rejected_at = datetime('now'), "
                "presentation_count = presentation_count + 1 "
                "WHERE target_id = ? OR source_id = ?",
                (summary_id, summary_id)
            )
            self._evaluate_link_degradation(summary_id)

        self.db.commit()

    def _evaluate_link_degradation(self, summary_id: int):
        """Check if accumulated rejections warrant a link state change."""
        rows = self.db.execute(
            """SELECT id, rejection_count, ignore_count, link_state,
                      (SELECT COUNT(*) FROM presentation_log
                       WHERE summary_id = ? AND response = 'explore') as explore_count
               FROM document_links
               WHERE target_id = ? OR source_id = ?""",
            (summary_id, summary_id, summary_id)
        ).fetchall()

        for row in rows:
            link_id = row["id"]
            rej = row["rejection_count"]
            state = row["link_state"]
            explores = row["explore_count"]

            new_state = state
            if rej >= 10 and state != "once_thought":
                new_state = "once_thought"
            elif rej >= 5 and explores == 0 and state not in ("sub_hidden", "once_thought"):
                new_state = "sub_hidden"
            elif rej >= 3 and state == "active":
                new_state = "soft_suppressed"

            if new_state != state:
                self.db.execute(
                    "UPDATE document_links SET link_state = ? WHERE id = ?",
                    (new_state, link_id)
                )

    def _get_link_state(self, summary_id: int) -> str:
        """Get the worst link_state for any link involving this summary."""
        row = self.db.execute(
            """SELECT link_state FROM document_links
               WHERE (target_id = ? OR source_id = ?)
               ORDER BY CASE link_state
                 WHEN 'once_thought' THEN 4
                 WHEN 'sub_hidden' THEN 3
                 WHEN 'soft_suppressed' THEN 2
                 WHEN 'active' THEN 1
               END DESC LIMIT 1""",
            (summary_id, summary_id)
        ).fetchone()
        return row["link_state"] if row else "active"

    def get_status(self) -> dict:
        """Return current session state for status endpoint."""
        return {
            "session_id": self.session_id,
            "query_hash": self.query_hash,
            "agent": self.agent,
            "current_layer": self.current_layer,
            "explored_count": len(self.explored_ids),
            "ignored_count": len(self.ignored_ids),
            "rejected_count": len(self.rejected_ids),
            "convergence_trail": self.convergence_trail,
            "seed_result_count": len(self.seed_results),
            "age_seconds": round(time.time() - self.created_at),
        }

    @staticmethod
    def group_into_cohorts(candidates: list[dict], cohort_size: int = 3) -> list[list[dict]]:
        """Group Layer 0 candidates into cohorts for comparative presentation.
        Items sharing some but not all annotations are placed together."""
        if not candidates:
            return []
        cohorts = []
        remaining = list(candidates)
        while remaining:
            cohort = remaining[:cohort_size]
            remaining = remaining[cohort_size:]
            # Tag each item with its cohort mates' IDs
            cohort_sids = [c["summary_id"] for c in cohort]
            for item in cohort:
                item["cohort_ids"] = [s for s in cohort_sids if s != item["summary_id"]]
            cohorts.append(cohort)
        return cohorts

    @staticmethod
    def rehabilitate_links(db: sqlite3.Connection, summary_id: int, reason: str = "cross_topic"):
        """Rehabilitate suppressed links when new evidence supports them.
        Promotes one state level and reduces rejection_count by 2."""
        db.execute(
            """UPDATE document_links
               SET link_state = CASE
                   WHEN link_state = 'once_thought' THEN 'sub_hidden'
                   WHEN link_state = 'sub_hidden' THEN 'soft_suppressed'
                   WHEN link_state = 'soft_suppressed' THEN 'active'
                   ELSE link_state
               END,
               rejection_count = MAX(0, rejection_count - 2)
               WHERE (target_id = ? OR source_id = ?)
               AND link_state != 'active'""",
            (summary_id, summary_id)
        )
        db.commit()


def _cleanup_expired_sessions():
    """Remove expired disclosure sessions."""
    now = time.time()
    expired = [sid for sid, s in _disclosure_sessions.items()
               if now - s.created_at > _SESSION_TTL_SECONDS]
    for sid in expired:
        del _disclosure_sessions[sid]


# ── Singleton ────────────────────────────────────────────────────────────────
_memory: UnifiedMemory | None = None


def get_memory() -> UnifiedMemory:
    global _memory
    if _memory is None:
        _memory = UnifiedMemory(DB_PATH)
    return _memory


# ── Pydantic Models ──────────────────────────────────────────────────────────
class IngestRequest(BaseModel):
    content: str
    source: str = "api"
    author: str = ""
    channel: str = ""
    metadata: dict | None = None
    summary: str | None = None


class IngestResponse(BaseModel):
    summary_id: int
    status: str
    annotations_count: int = 0


# ── WebSocket Manager ───────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


ws_manager = ConnectionManager()

# ── Service Start Time ──────────────────────────────────────────────────────
_service_start_time: float = time.time()

# ── Auto-Backup Task ────────────────────────────────────────────────────────
_backup_task: asyncio.Task | None = None


async def auto_backup_loop():
    """Periodically backup the RAMDisk database to disk. OPT-028: Non-blocking."""
    if BACKUP_INTERVAL <= 0:
        logger.info("Auto-backup disabled (interval_seconds=0)")
        return
    while True:
        await asyncio.sleep(BACKUP_INTERVAL)
        try:
            await asyncio.to_thread(do_backup)
        except Exception as e:
            logger.error(f"Auto-backup failed: {e}")


# ── OPT-024: Background stats updater loop ───────────────────────────────────

async def _background_stats_loop():
    """Periodically compute stats in background so /stats and /health are instant reads."""
    await asyncio.sleep(5)  # let service finish startup
    while True:
        try:
            mem = get_memory()
            data = await asyncio.to_thread(mem._compute_stats_fresh)
            _stats_cache["data"] = data
            _stats_cache["time"] = time.time()
        except Exception as e:
            logger.error(f"Background stats update failed: {e}")
        await asyncio.sleep(_BG_STATS_INTERVAL)


def compress_backup(db_path, output_path, level=3):
    """Stream-compress a DB file with zstd. Returns (compressed_size, sha256)."""
    hasher = hashlib.sha256()
    compressor = zstd.ZstdCompressor(level=level)
    with open(db_path, "rb") as fin, open(output_path, "wb") as fout:
        with compressor.stream_writer(fout) as writer:
            while True:
                chunk = fin.read(1 << 20)  # 1 MB chunks
                if not chunk:
                    break
                writer.write(chunk)
    with open(output_path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            hasher.update(chunk)
    compressed_size = os.path.getsize(output_path)
    return compressed_size, hasher.hexdigest()


def do_backup():
    """Create a consistent backup of the database using VACUUM INTO, then compress with zstd."""
    db_path = Path(DB_PATH)
    if not db_path.exists():
        return {"status": "skip", "reason": "db not found"}

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    dst = BACKUP_DIR / db_path.name

    try:
        conn = sqlite3.connect(str(db_path))
        conn.execute(f"VACUUM INTO '{str(dst)}'")
        conn.close()
    except Exception as e:
        logger.warning(f"VACUUM INTO failed ({e}), falling back to file copy")
        for suffix in ["", "-wal", "-shm"]:
            src = db_path.parent / (db_path.name + suffix)
            if src.exists():
                shutil.copy2(str(src), str(BACKUP_DIR / (db_path.name + suffix)))

    db_size = dst.stat().st_size if dst.exists() else 0
    logger.info(f"Backup saved: {dst.name} ({db_size / 1024:.1f} KB)")

    # Compressed backup (zstd)
    zst_path = BACKUP_DIR / (db_path.stem + ".zst")
    try:
        t0 = time.time()
        compressed_size, sha256 = compress_backup(str(dst), str(zst_path))
        elapsed = time.time() - t0
        logger.info(f"Compressed backup: {zst_path.name} ({compressed_size / (1024*1024):.1f} MB, sha256={sha256[:12]}…, {elapsed:.1f}s)")
    except Exception as e:
        logger.error(f"Compressed backup failed: {e}")
        compressed_size, sha256 = 0, ""

    return {
        "status": "ok",
        "files": [dst.name, zst_path.name] if compressed_size else [dst.name],
        "db_size_bytes": db_size,
        "compressed_size_bytes": compressed_size,
        "compressed_sha256": sha256,
    }


# ── Per-Segment Incremental Sync (B-TC-005β) ────────────────────────────────

# Logical segments: group tables by data affinity
SYNC_SEGMENTS = {
    "core": ["summaries", "verbatim", "document_links"],
    "annotations": ["annotations"],
    "graph": ["top_neighbors", "aspect_index"],
    "activity": ["agent_activity", "ambient_log", "selection_log", "presentation_log", "topic_heat"],
    "guardian": ["guardian_budget", "guardian_cache", "guardian_curations", "guardian_flags", "guardian_queries"],
}

SYNC_DIR = BACKUP_DIR / "segments"
MANIFEST_PATH = SYNC_DIR / "manifest.json"


def _load_sync_manifest() -> dict:
    """Load the previous sync manifest (segment checksums + metadata)."""
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"segments": {}, "created_at": None, "version": 1}


def _save_sync_manifest(manifest: dict):
    """Persist the sync manifest atomically."""
    tmp = MANIFEST_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    tmp.replace(MANIFEST_PATH)


def _export_segment(conn: sqlite3.Connection, segment_name: str, tables: list[str], output_dir: Path, level: int = 3) -> dict:
    """Export a logical segment (1+ tables) to a compressed .zst file with SHA256.

    Creates a temporary SQLite database containing only the segment's tables,
    then compresses the whole DB with zstd. This is orders of magnitude faster
    than SQL text export for large tables (2M+ rows).
    """
    seg_db_path = output_dir / f"{segment_name}.db"
    seg_path = output_dir / f"{segment_name}.db.zst"
    total_rows = 0

    # Remove stale temp DB
    for suffix in ["", "-wal", "-shm"]:
        p = seg_db_path.parent / (seg_db_path.name + suffix)
        if p.exists():
            p.unlink()

    # Create segment DB via ATTACH — uses SQLite's fast bulk copy
    seg_conn = sqlite3.connect(str(seg_db_path))
    seg_conn.execute("PRAGMA journal_mode=OFF")
    seg_conn.execute("PRAGMA synchronous=OFF")
    try:
        for table in tables:
            schema_row = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            if not schema_row or not schema_row[0]:
                continue

            # Create table with same schema in segment DB
            seg_conn.execute(schema_row[0])

            # Bulk copy via INSERT ... SELECT through ATTACH
            col_row = conn.execute(f"PRAGMA table_info([{table}])").fetchall()
            col_names = [c[1] for c in col_row]
            cols_str = ",".join([f"[{c}]" for c in col_names])

            # Stream in batches to avoid holding entire table in memory
            batch_size = 50000
            offset = 0
            while True:
                rows = conn.execute(
                    f"SELECT {cols_str} FROM [{table}] LIMIT {batch_size} OFFSET {offset}"
                ).fetchall()
                if not rows:
                    break
                placeholders = ",".join(["?"] * len(col_names))
                seg_conn.executemany(
                    f"INSERT INTO [{table}]({cols_str}) VALUES({placeholders})", rows
                )
                total_rows += len(rows)
                offset += batch_size
                if len(rows) < batch_size:
                    break

            # Copy indexes for this table
            indexes = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
                (table,)
            ).fetchall()
            for idx in indexes:
                try:
                    seg_conn.execute(idx[0])
                except sqlite3.OperationalError:
                    pass  # Skip if index already exists

        seg_conn.commit()
    finally:
        seg_conn.close()

    # Compress the segment DB with zstd
    hasher = hashlib.sha256()
    compressor = zstd.ZstdCompressor(level=level)
    with open(seg_db_path, "rb") as fin, open(seg_path, "wb") as fout:
        with compressor.stream_writer(fout) as writer:
            while True:
                chunk = fin.read(1 << 20)
                if not chunk:
                    break
                writer.write(chunk)

    # Checksum the compressed file
    with open(seg_path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            hasher.update(chunk)

    compressed_size = seg_path.stat().st_size

    # Clean up uncompressed DB
    seg_db_path.unlink(missing_ok=True)

    return {
        "file": seg_path.name,
        "tables": tables,
        "rows": total_rows,
        "compressed_bytes": compressed_size,
        "sha256": hasher.hexdigest(),
    }


def _compute_segment_checksum(conn: sqlite3.Connection, tables: list[str]) -> str:
    """Compute a fast checksum for a segment by hashing row counts + max rowids.

    This is a lightweight "change detector" — not a full content hash.
    Changes in row count or max rowid indicate the segment has changed.
    """
    hasher = hashlib.sha256()
    for table in sorted(tables):
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
            max_id = conn.execute(f"SELECT MAX(rowid) FROM [{table}]").fetchone()[0] or 0
            hasher.update(f"{table}:{count}:{max_id}\n".encode())
        except Exception:
            hasher.update(f"{table}:0:0\n".encode())
    return hasher.hexdigest()


def do_incremental_sync(force: bool = False) -> dict:
    """Per-segment incremental sync: only re-export segments whose data changed.

    Compares lightweight checksums (row count + max rowid) against the stored
    manifest.  Only segments with changes are re-exported and compressed.
    If force=True, all segments are re-exported regardless of checksums.

    Returns a summary dict with per-segment status and overall stats.
    """
    SYNC_DIR.mkdir(parents=True, exist_ok=True)
    db_path = Path(DB_PATH)
    if not db_path.exists():
        return {"status": "skip", "reason": "db not found"}

    t0 = time.time()
    manifest = _load_sync_manifest()
    prev_segments = manifest.get("segments", {})

    # Open read-only connection for consistent snapshot
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only = ON")

    results = {}
    changed_count = 0
    skipped_count = 0
    total_bytes = 0

    try:
        for seg_name, tables in SYNC_SEGMENTS.items():
            current_checksum = _compute_segment_checksum(conn, tables)
            prev_info = prev_segments.get(seg_name, {})
            prev_checksum = prev_info.get("quick_checksum", "")

            if not force and current_checksum == prev_checksum:
                # Segment unchanged — skip export
                results[seg_name] = {
                    "status": "unchanged",
                    "quick_checksum": current_checksum,
                    **{k: prev_info[k] for k in ("sha256", "compressed_bytes", "rows", "file") if k in prev_info},
                }
                skipped_count += 1
                total_bytes += prev_info.get("compressed_bytes", 0)
                continue

            # Segment changed — re-export
            seg_info = _export_segment(conn, seg_name, tables, SYNC_DIR)
            seg_info["status"] = "exported"
            seg_info["quick_checksum"] = current_checksum
            results[seg_name] = seg_info
            changed_count += 1
            total_bytes += seg_info["compressed_bytes"]

    finally:
        conn.close()

    elapsed = time.time() - t0

    # Build and save updated manifest
    new_manifest = {
        "version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "segments": results,
        "total_compressed_bytes": total_bytes,
        "sync_elapsed_seconds": round(elapsed, 2),
    }
    _save_sync_manifest(new_manifest)

    logger.info(
        f"Incremental sync: {changed_count} exported, {skipped_count} unchanged, "
        f"{total_bytes / (1024*1024):.1f} MB total, {elapsed:.1f}s"
    )
    return {
        "status": "ok",
        "changed": changed_count,
        "unchanged": skipped_count,
        "total_compressed_bytes": total_bytes,
        "elapsed_seconds": round(elapsed, 2),
        "segments": results,
    }


# ── FastAPI App ──────────────────────────────────────────────────────────────
_file_watcher: uaimc_watcher.UAIMCFileWatcher | None = None

REMINDERS_PATH = Path(__file__).parent / "data" / "scheduled_reminders.json"


def _check_scheduled_reminders(mem) -> None:
    """Check for due scheduled reminders and auto-ingest them into UAIMC.

    Reminders resurface on every startup after their due_date until dismissed.
    Uses resurface_interval_days to avoid spamming — only re-ingests if enough
    time has passed since last surfaced.
    """
    if not REMINDERS_PATH.exists():
        return
    try:
        with open(REMINDERS_PATH, "r", encoding="utf-8") as f:
            reminders = json.load(f)
    except Exception as e:
        logger.warning(f"Failed to read reminders: {e}")
        return

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    changed = False

    for r in reminders:
        if r.get("status") != "pending":
            continue
        due = r.get("due_date", "")
        if not due or today < due:
            continue

        # Check resurface interval
        interval = r.get("resurface_interval_days", 1)
        last = r.get("last_surfaced")
        if last:
            try:
                days_since = (datetime.strptime(today, "%Y-%m-%d") -
                              datetime.strptime(last, "%Y-%m-%d")).days
                if days_since < interval:
                    continue
            except ValueError:
                pass

        # Build reminder text for ingestion
        title = r.get("title", "Scheduled Reminder")
        criteria = r.get("criteria", [])
        context = r.get("context", "")
        tags = " ".join(f"#{t}" for t in r.get("tags", []))
        days_overdue = (datetime.strptime(today, "%Y-%m-%d") -
                        datetime.strptime(due, "%Y-%m-%d")).days

        urgency = "DUE TODAY" if days_overdue == 0 else f"OVERDUE by {days_overdue} days"
        criteria_text = "\n".join(f"  - [ ] {c}" for c in criteria) if criteria else "  (none specified)"

        text = (
            f"SCHEDULED REMINDER — {urgency}\n"
            f"{'=' * 60}\n"
            f"Title: {title}\n"
            f"Due: {due} | Priority: {r.get('priority', 'normal')}\n"
            f"Created by: {r.get('created_by', 'unknown')} on {r.get('created_date', '?')}\n\n"
            f"CRITERIA TO PROCEED:\n{criteria_text}\n\n"
            f"CONTEXT:\n{context}\n\n"
            f"Spec: {r.get('spec_reference', 'N/A')}\n"
            f"Tags: {tags} #reminder #scheduled\n\n"
            f"To dismiss: Set status to 'dismissed' in data/scheduled_reminders.json\n"
            f"To complete: Set status to 'completed' in data/scheduled_reminders.json"
        )

        sid = mem.store(
            text=text,
            source="scheduled_reminder",
            author=r.get("created_by", "SYSTEM"),
            channel="reminders",
            metadata={"reminder_id": r.get("id"), "due_date": due, "priority": r.get("priority")},
        )
        if sid > 0:
            logger.info(f"Reminder surfaced: {title} (summary_id={sid}, {urgency})")
        elif sid == -2:
            logger.info(f"Reminder already in DB (dedup): {title}")

        r["last_surfaced"] = today
        changed = True

    if changed:
        try:
            tmp = REMINDERS_PATH.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(reminders, f, indent=2, ensure_ascii=False)
            tmp.replace(REMINDERS_PATH)
        except Exception as e:
            logger.warning(f"Failed to update reminders file: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _backup_task, _file_watcher, _bg_stats_task
    logger.info(f"UAIMC Service starting on {HOST}:{PORT}")
    logger.info(f"Database: {DB_PATH}")
    mem = get_memory()  # Force init
    if _GPU_ENABLED:
        mem._cleanup_ppr_cache()  # Phase 2: clear stale PPR cache on startup
    if BACKUP_INTERVAL > 0:
        _backup_task = asyncio.create_task(auto_backup_loop())
    else:
        logger.info("Auto-backup task skipped (interval_seconds=0)")
    _bg_stats_task = asyncio.create_task(_background_stats_loop())  # OPT-024

    # Start file watcher (skip on Railway — read-only)
    if _GPU_ENABLED:
        watcher_config = CONFIG.get("file_watcher", {})
        if watcher_config.get("enabled", False):
            try:
                _file_watcher = uaimc_watcher.UAIMCFileWatcher(watcher_config, mem.store)
                _file_watcher.start()
            except Exception as e:
                logger.error(f"File watcher failed to start: {e}")

    # Check scheduled reminders (skip on Railway — writes to DB)
    if _GPU_ENABLED:
        _check_scheduled_reminders(mem)

    # Initialize Guardian AI (B-004) — own connection to avoid blocking event loop
    global _guardian
    if _GUARDIAN_AVAILABLE:
        try:
            _guardian_db = sqlite3.connect(mem.db_path, check_same_thread=False)
            _guardian_db.row_factory = sqlite3.Row
            _guardian_db.execute("PRAGMA journal_mode=WAL")
            _guardian_db.execute("PRAGMA synchronous=NORMAL")
            _guardian_db.execute("PRAGMA busy_timeout=10000")
            _guardian_db.execute("PRAGMA cache_size=-64000")
            _guardian_db.execute("PRAGMA temp_store=MEMORY")
            _guardian = GuardianAI(db=_guardian_db, config=CONFIG)
            logger.info(f"Guardian AI initialized — mode={_guardian.mode} (own db connection)")
            await _guardian.start_scheduler()
        except Exception as e:
            logger.error(f"Guardian AI init failed: {e}")
            _guardian = None

    # Phase 5: Cache warming — moved to background task to avoid blocking startup
    async def _deferred_cache_warming():
        """Run cache warming after server starts accepting requests."""
        await asyncio.sleep(2)  # Let health check pass first
        try:
            _warm_n = 3
            _warm_rows = mem.db.execute(
                "SELECT agent_name, COUNT(*) as cnt FROM agent_activity GROUP BY agent_name ORDER BY cnt DESC LIMIT ?",
                (_warm_n,)
            ).fetchall()
            _warm_agents = [r[0] for r in _warm_rows if r[0]]
            if _warm_agents:
                for _wa in _warm_agents:
                    try:
                        await asyncio.to_thread(mem.get_context_for_prompt, agent=_wa, topic="", max_chars=CONTEXT_LIMIT)
                    except Exception as _we:
                        logger.warning("Cache warming failed for agent %s: %s", _wa, _we)
                logger.info("Phase 5 cache warming: pre-warmed %d agents: %s", len(_warm_agents), _warm_agents)
            else:
                logger.info("Phase 5 cache warming: no agents found in agent_activity")
        except Exception as e:
            logger.warning("Phase 5 cache warming failed (non-fatal): %s", e)
        # Phase 6: Bootstrap agent topic priors
        try:
            priors = await asyncio.to_thread(mem.get_agent_topic_priors)
            logger.info("Phase 6 agent topic priors: loaded for %d agents", len(priors))
        except Exception as e:
            logger.warning("Phase 6 topic priors bootstrap failed (non-fatal): %s", e)

    asyncio.create_task(_deferred_cache_warming())

    yield
    logger.info("UAIMC Service shutting down...")

    # Shutdown Guardian
    if _guardian:
        try:
            await _guardian.stop_scheduler()
            await _guardian.close()
        except Exception:
            pass

    # Stop file watcher
    if _file_watcher:
        _file_watcher.stop()

    if _backup_task:
        _backup_task.cancel()
    if _bg_stats_task:
        _bg_stats_task.cancel()
    do_backup()
    mem = get_memory()
    mem.close()


app = FastAPI(
    title="UAIMC — Universal AI Memory Core",
    description="Shared memory service for ALL AI agents in Team Brain",
    version="1.0.0",
    lifespan=lifespan,
    default_response_class=SafeJSONResponse,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def pulse_auth_middleware(request, call_next):
    """Optional token auth for remote Pulse access.
    Skipped when no token configured or request is from localhost."""
    token = CONFIG.get("pulse", {}).get("auth_token")
    if token:
        client_ip = request.client.host if request.client else "127.0.0.1"
        if client_ip not in ("127.0.0.1", "::1"):
            skip_paths = ("/health", "/favicon.ico")
            if request.url.path not in skip_paths:
                auth = request.headers.get("authorization", "")
                qtoken = request.query_params.get("token", "")
                import hmac
                valid = (
                    hmac.compare_digest(auth, f"Bearer {token}")
                    or hmac.compare_digest(qtoken, token)
                )
                if not valid:
                    return JSONResponse({"error": "Unauthorized"}, status_code=401)
    return await call_next(request)


@app.middleware("http")
async def request_timeout_middleware(request, call_next):
    """Enforce 30s timeout on most HTTP requests to prevent queue buildup.
    Long-running endpoints (backup/sync) get 10 minutes."""
    import asyncio as _aio
    long_running = request.url.path in ("/backup/sync", "/backup", "/query/batch", "/context")
    timeout = 600.0 if long_running else 30.0
    try:
        return await _aio.wait_for(call_next(request), timeout=timeout)
    except _aio.TimeoutError:
        from starlette.responses import JSONResponse
        return JSONResponse({"error": f"Request timeout ({int(timeout)}s)"}, status_code=503)


# ── Endpoints ────────────────────────────────────────────────────────────────
@app.post("/ingest", response_model=IngestResponse)
async def ingest(req: IngestRequest):
    """Store content + auto-annotate into 3-tier memory."""
    mem = get_memory()
    summary_id = mem.store(
        text=req.content,
        source=req.source,
        author=req.author,
        channel=req.channel,
        metadata=req.metadata,
        summary_text=req.summary,
    )

    if summary_id == -2:
        return IngestResponse(summary_id=-2, status="duplicate", annotations_count=0)
    if summary_id < 0:
        raise HTTPException(status_code=500, detail="Failed to store content")

    # Phase B1-Step1 + Phase 5: Invalidate context cache on new data
    # Full clear because new content could affect any agent's context recall.
    # Per-agent invalidation available via invalidate_context_cache_for_agent() for targeted use.
    mem.invalidate_context_cache()
    _AMBIENT_CACHE.clear()
    _QUERY_CACHE.clear()  # OPT-029: invalidate query cache on ingest
    _stats_cache["data"] = None  # OPT-002: invalidate stats cache on ingest
    _health_cache["data"] = None  # OPT-014: invalidate health cache on ingest

    # Phase 2: Invalidate PPR cache on new data
    mem.db.execute("DELETE FROM ppr_cache")
    mem.db.commit()

    # Count annotations for this summary
    anno_count = mem.db.execute(
        "SELECT COUNT(*) FROM annotations WHERE summary_id = ?", (summary_id,)
    ).fetchone()[0]

    # B-011: Record ingest heat for topic tokens from this document's annotations
    if anno_count > 0:
        heat_rows = mem.db.execute(
            "SELECT DISTINCT token FROM annotations WHERE summary_id = ? LIMIT 10",
            (summary_id,),
        ).fetchall()
        heat_tokens = [r[0] for r in heat_rows]
        if heat_tokens:
            await asyncio.to_thread(mem.log_heat_event, heat_tokens, req.author or "*", "ingest")

    # Log agent activity
    if req.author:
        mem.log_agent_activity(req.author, "ingest", f"source={req.source}")

    # Broadcast to WebSocket clients
    await ws_manager.broadcast({
        "event": "new_entry",
        "summary_id": summary_id,
        "source": req.source,
        "author": req.author,
        "annotations_count": anno_count,
    })

    # RC: Ambient push — notify connected agents of related knowledge
    try:
        ambient_config = uaimc_ambient.load_ambient_config(CONFIG)
        if ambient_config.enabled and ws_manager.active:
            push = uaimc_ambient.build_ambient_push_payload(
                filepath=req.source, author=req.author or "",
                memory=mem, config=ambient_config,
            )
            if push:
                await ws_manager.broadcast(push)
    except Exception as e:
        logger.debug(f"Ambient push failed (non-fatal): {e}")

    # Token tracking (best-effort, non-blocking)
    uaimc_tools.track_api_usage(
        agent=req.author or req.source, action="ingest",
        input_chars=len(req.content), db_path=DB_PATH,
    )

    # KnowledgeSync bridge (best-effort)
    uaimc_tools.sync_to_knowledge_graph(
        text=req.content, source=req.source, author=req.author,
    )

    return IngestResponse(summary_id=summary_id, status="ok", annotations_count=anno_count)


@app.post("/ingest/curator-link")
async def add_curator_link(body: dict):
    """B-012: Curator explicit link creation between documents."""
    source_id = body.get("source_id")
    target_id = body.get("target_id")
    reason = body.get("reason", "")
    strength = min(1.0, max(0.0, body.get("strength", 0.9)))

    if not source_id or not target_id:
        raise HTTPException(status_code=400, detail="source_id and target_id required")

    mem = get_memory()
    s1 = mem.db.execute("SELECT id FROM summaries WHERE id = ?", (source_id,)).fetchone()
    s2 = mem.db.execute("SELECT id FROM summaries WHERE id = ?", (target_id,)).fetchone()

    if not s1 or not s2:
        raise HTTPException(status_code=404, detail="One or both document IDs not found")

    existing = mem.db.execute(
        "SELECT id FROM document_links WHERE source_id = ? AND target_id = ? AND link_type = 'curator_linked'",
        (source_id, target_id)
    ).fetchone()

    if existing:
        eid = existing["id"] if isinstance(existing, sqlite3.Row) else existing[0]
        mem.db.execute("UPDATE document_links SET strength = MAX(strength, ?) WHERE id = ?", (strength, eid))
    else:
        mem.db.execute(
            """INSERT INTO document_links (source_id, target_id, link_type, strength, discovered_by)
               VALUES (?, ?, 'curator_linked', ?, 'curator')""",
            (source_id, target_id, strength)
        )

    mem.db.commit()
    return {"success": True, "source_id": source_id, "target_id": target_id, "reason": reason}


# ── Phase 2 Task 3: Domain filter helper ─────────────────────────────────────

def _apply_domain_filter(results: list[dict], domain: str, mem) -> list[dict]:
    """Post-filter query results to only include entries matching the given domain.

    Resolution order per result:
      1. Check result['metadata']['domain'] (set by pipeline_bridge summaries)
      2. Check result['metadata']['filepath'] → look up kg_nodes by file:path → metadata.domain
    """
    domain_lower = domain.lower().strip()
    if not domain_lower:
        return results

    # Build a set of filepaths we need to resolve from kg_nodes
    need_lookup: dict[int, str] = {}  # index → normalized filepath
    resolved: list[tuple[int, bool]] = []  # (index, matches)

    for i, r in enumerate(results):
        meta = r.get("metadata", {})
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except (json.JSONDecodeError, TypeError):
                meta = {}

        # Fast path: domain already in summary metadata
        if meta.get("domain", "").lower() == domain_lower:
            resolved.append((i, True))
            continue

        # Need kg_nodes lookup via filepath
        fp = meta.get("filepath", "")
        if fp:
            need_lookup[i] = fp.replace("\\", "/")
        else:
            resolved.append((i, False))

    # Batch lookup: resolve filepaths → kg_nodes domain
    if need_lookup:
        conn = mem._pool.get()
        try:
            for idx, norm_fp in need_lookup.items():
                file_id = f"file:{norm_fp}"
                row = conn.execute(
                    "SELECT json_extract(metadata, '$.domain') as domain FROM kg_nodes WHERE id = ?",
                    (file_id,),
                ).fetchone()
                if row and row["domain"] and row["domain"].lower() == domain_lower:
                    resolved.append((idx, True))
                else:
                    resolved.append((idx, False))
        finally:
            mem._pool.put(conn)

    matching_indices = {idx for idx, matches in resolved if matches}
    return [r for i, r in enumerate(results) if i in matching_indices]


@app.get("/query")
async def query(
    q: str = Query(..., description="Search query"),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0, le=500, description="OPT-023: Skip first N results"),
    agent: str = Query("", description="Agent making the query (for activity log)"),
    domain: str = Query("", description="Filter results to files in this domain (e.g. research, protocol, config)"),
):
    """Search memory by keywords using FTS5."""
    # OPT-029: Check query result cache
    cache_key = (q, limit, offset, agent or "*", domain)
    now = time.time()
    cached = _QUERY_CACHE.get(cache_key)
    if cached and (now - cached[0]) < _QUERY_CACHE_TTL:
        return cached[1]

    mem = get_memory()
    # When domain filtering, fetch more results to ensure we have enough after filtering
    base_fetch = min(limit + offset, 100)
    fetch_count = min(base_fetch * 5, 500) if domain else base_fetch
    results = await asyncio.to_thread(mem.query_text, q, fetch_count, agent or "*")

    # Phase 2 Task 3: Domain filtering via summaries.metadata.domain + kg_nodes fallback
    if domain:
        results = await asyncio.to_thread(_apply_domain_filter, results, domain, mem)

    total_before_slice = len(results)
    results = results[offset:offset + limit]

    # B-010: Log ranking_promoted signals for all returned results
    if results:
        await asyncio.to_thread(mem.log_ranking_promoted, results, q, agent)

    # B-011: Record heat events for query keywords
    query_tokens = [kw.lower().strip() for kw in q.split() if len(kw.strip()) >= 2]
    if query_tokens:
        await asyncio.to_thread(mem.log_heat_event, query_tokens, agent or "*", "query")

    if agent:
        mem.log_agent_activity(agent, "query", f"q={q[:100]}")
        uaimc_tools.track_api_usage(agent=agent, action="query", input_chars=len(q), db_path=DB_PATH)

    response = {"query": q, "count": len(results), "offset": offset, "total_fetched": total_before_slice, "results": results}
    if domain:
        response["domain_filter"] = domain

    # OPT-029: Store in cache (evict oldest if full)
    if len(_QUERY_CACHE) >= _QUERY_CACHE_MAX:
        oldest_key = min(_QUERY_CACHE, key=lambda k: _QUERY_CACHE[k][0])
        del _QUERY_CACHE[oldest_key]
    _QUERY_CACHE[cache_key] = (time.time(), response)

    return response


# ── OPT-022: Batch Query API ──────────────────────────────────────────────

class BatchQueryItem(BaseModel):
    q: str
    limit: int = 10

class BatchQueryRequest(BaseModel):
    queries: list[BatchQueryItem]
    agent: str = ""

@app.post("/query/batch")
async def query_batch(req: BatchQueryRequest):
    """OPT-022: Execute multiple queries in a single request.
    Runs sequentially to avoid GPU-AM lock contention.
    Saves HTTP round-trip overhead for MCP tools.
    """
    mem = get_memory()
    if not req.queries:
        return {"count": 0, "results": []}
    if len(req.queries) > 20:
        return {"error": "Maximum 20 queries per batch", "count": 0, "results": []}

    batch_results = []
    for item in req.queries:
        results = await asyncio.to_thread(mem.query_text, item.q, min(item.limit, 100), req.agent or "*")
        batch_results.append({"query": item.q, "count": len(results), "results": results})

    if req.agent:
        mem.log_agent_activity(req.agent, "query_batch", f"n={len(req.queries)}")

    return {"count": len(batch_results), "results": batch_results}


@app.get("/context")
async def get_context(
    agent: str = Query("", description="Agent requesting context"),
    topic: str = Query("", description="Topic to recall"),
    max_chars: int = Query(CONTEXT_LIMIT, ge=100, le=MAX_CONTEXT, description="Maximum characters for context", alias="max_chars"),
    tokens: int = Query(None, ge=100, le=MAX_CONTEXT, description="(Deprecated) Alias for max_chars"),
    layers: str = Query(None, description="Phase 5: Disclosure layer — quick, detailed, or full (default: full)"),
    mode: str = Query("", description="Response format: '' (default text) or 'cans' (structured FACTS/REFLECTIONS/EPISODES sections)"),
):
    """Context Recycler: assembled memory block for prompt injection.
    
    Phase 5: ?layers= parameter selects progressive disclosure level:
      - full (default): all items (Layer 0)
      - detailed: top 70% — full + truncated tiers (Layer 1)
      - quick: top 30% — max 7 items (Layer 2)
    """
    reload_config_if_changed()  # Phase B1-FC2: hot-reload config if changed
    char_budget = tokens if tokens is not None else max_chars  # B11: backward compat
    mem = get_memory()
    context = await asyncio.to_thread(mem.get_context_for_prompt, agent=agent, topic=topic, max_chars=char_budget)

    # B-011: Record heat events for context topic tokens
    if topic:
        topic_tokens = [kw.lower().strip() for kw in topic.split() if len(kw.strip()) >= 2]
        if topic_tokens:
            await asyncio.to_thread(mem.log_heat_event, topic_tokens, agent or "*", "session_start")

    # Ambient Knowledge Enrichment (The Curator v1.0) — Phase B1: cached
    ambient_section = ""
    ambient_config = uaimc_ambient.load_ambient_config(CONFIG)
    if ambient_config.enabled and agent:
        _amb_key = (agent, topic)
        _amb_entry = _AMBIENT_CACHE.get(_amb_key)
        _amb_now = time.time()
        if _amb_entry and (_amb_now - _amb_entry[0]) < _AMBIENT_CACHE_TTL:
            ambient_section = _amb_entry[1]
        else:
            try:
                ambient_section = await asyncio.to_thread(
                    uaimc_ambient.enrich_context,
                    memory=mem, agent=agent, config=ambient_config,
                    trigger="context_request", topic=topic,
                )
                # OPT-034: Evict oldest if cache is full
                if len(_AMBIENT_CACHE) >= _AMBIENT_CACHE_MAX:
                    oldest_key = min(_AMBIENT_CACHE, key=lambda k: _AMBIENT_CACHE[k][0])
                    del _AMBIENT_CACHE[oldest_key]
                _AMBIENT_CACHE[_amb_key] = (_amb_now, ambient_section)
            except Exception as e:
                logger.warning(f"Ambient enrichment failed (non-fatal): {e}")

    full_context = context + ambient_section if ambient_section else context

    # Phase 5: Apply disclosure layer filtering if ?layers= specified
    layer_applied = None
    if layers and layers in ("quick", "detailed", "full"):
        cache_key = (agent, topic, char_budget)
        _layers_entry = mem._context_layers_cache.get(cache_key)
        if _layers_entry:
            _ts, _disc_layers = _layers_entry
            if (time.time() - _ts) < mem._context_cache_ttl and _disc_layers:
                if layers == "quick" and len(_disc_layers) >= 3:
                    layer_content = "\n\n".join(_disc_layers[2])
                    full_context = layer_content
                    layer_applied = "quick"
                elif layers == "detailed" and len(_disc_layers) >= 2:
                    layer_content = "\n\n".join(_disc_layers[1])
                    full_context = layer_content
                    layer_applied = "detailed"
                elif layers == "full" and len(_disc_layers) >= 1:
                    layer_content = "\n\n".join(_disc_layers[0])
                    full_context = layer_content
                    layer_applied = "full"

    if agent:
        mem.log_agent_activity(agent, "context", f"topic={topic[:100]}")
        uaimc_tools.track_api_usage(
            agent=agent, action="context",
            input_chars=len(topic), output_chars=len(full_context), db_path=DB_PATH,
        )

        # Phase 6: Predictive cache warming — pre-warm predicted next queries
        try:
            predicted = mem.predict_next_queries(agent, top_n=2)
            for pred_topic in predicted:
                pred_key = (agent, pred_topic, char_budget)
                if mem._context_cache_get(pred_key) is None:
                    # Pre-warm in background (best-effort)
                    mem.get_context_for_prompt(agent=agent, topic=pred_topic, max_chars=char_budget)
                    logger.debug("Phase 6 predictive pre-warm: agent=%s topic=%s", agent, pred_topic)
                    break  # Only pre-warm 1 per request to avoid latency
        except Exception as _pe:
            logger.debug("Phase 6 predictive pre-warm failed (non-fatal): %s", _pe)

    # Bible Sprint 1 Step 1.4: mode=cans returns structured FACTS/REFLECTIONS/EPISODES
    if mode == "cans":
        # Classify query results by source type into CANS sections
        _cans_facts_prefixes = ("research_", "document", "code_", "tool")
        _cans_reflections_sources = ("session_log", "bookmark", "memory_core")
        _cans_episodes_prefixes = ("synapse", "project_", "active_project")

        def _classify_source(src: str) -> str:
            src_l = src.lower() if src else ""
            if any(src_l.startswith(p) for p in _cans_facts_prefixes):
                return "facts"
            if src_l in _cans_reflections_sources:
                return "reflections"
            if any(src_l.startswith(p) for p in _cans_episodes_prefixes):
                return "episodes"
            # Default: classify by first letter heuristic
            if src_l.startswith(("research", "doc", "code", "tool")):
                return "facts"
            if src_l.startswith(("session", "book", "memory")):
                return "reflections"
            return "facts"  # default bucket

        # Get raw results to classify
        cans_results = await asyncio.to_thread(
            mem.query_text, topic or agent, limit=30, agent=agent
        )

        sections = {"facts": [], "reflections": [], "episodes": []}
        for r in (cans_results or []):
            bucket = _classify_source(r.get("source", ""))
            entry = f"[{r.get('source', 'unknown').upper()}] ({r.get('created_at', '')[:10]}) {r.get('summary', '')}"
            sections[bucket].append(entry)

        # Build structured context string
        section_lines = []
        for sec_name in ("facts", "reflections", "episodes"):
            items = sections[sec_name]
            if items:
                section_lines.append(f"=== {sec_name.upper()} ===")
                section_lines.extend(items)
                section_lines.append("")

        cans_context = "\n".join(section_lines) if section_lines else full_context

        return {
            "agent": agent,
            "mode": "cans",
            "topic": topic,
            "context": cans_context,
            "sections": {
                "facts": len(sections["facts"]),
                "reflections": len(sections["reflections"]),
                "episodes": len(sections["episodes"]),
            },
            "context_chars": len(cans_context),
        }

    return {
        "agent": agent,
        "topic": topic,
        "max_chars": char_budget,
        "context_chars": len(full_context),
        "context": full_context,
        "ambient_enabled": ambient_config.enabled,
        "ambient_chars": len(ambient_section),
        "layer": layer_applied,
    }


@app.get("/ambient")
async def ambient_query(
    agent: str = Query("", description="Agent name for context-aware ranking"),
    intent: str = Query("", description="What the agent is working on"),
    limit: int = Query(5, ge=1, le=20, description="Max results to return"),
):
    """Standalone ambient knowledge query (The Curator v1.0).

    Unlike /context (which injects ambient as a section), this returns
    structured JSON results for direct use by agents and MCP tools.

    Uses the full 4-tier pipeline: keywords → GPU-AM → quality filter → Haiku escalation.
    """
    mem = get_memory()
    ambient_config = uaimc_ambient.load_ambient_config(CONFIG)

    if not ambient_config.enabled:
        return {"error": "ambient layer disabled", "results": []}

    result = await asyncio.to_thread(
        uaimc_ambient.query_ambient,
        memory=mem, agent=agent, intent=intent,
        config=ambient_config, max_results=limit,
    )

    if agent:
        mem.log_agent_activity(agent, "ambient_query", f"intent={intent[:100]}")
        uaimc_tools.track_api_usage(
            agent=agent, action="ambient_query",
            input_chars=len(intent), output_chars=len(json.dumps(result)),
            db_path=DB_PATH,
        )

    return result


@app.get("/ambient/metrics")
async def ambient_metrics():
    """Dashboard metrics for The Curator v1.0 ambient layer.

    Returns daily/monthly cost, injections per agent, tier distribution,
    cache hit rate, latency percentiles, and budget status.
    """
    mem = get_memory()
    return uaimc_ambient.get_ambient_metrics(mem.db)


@app.get("/ambient/cross-pollinate")
async def ambient_cross_pollinate(
    agent: str = Query(..., description="Agent requesting cross-team intel"),
):
    """Get relevant work from OTHER agents for cross-pollination.

    Returns up to 3 knowledge items from other agents that match
    this agent's current working context.
    """
    mem = get_memory()
    ambient_config = uaimc_ambient.load_ambient_config(CONFIG)
    items = uaimc_ambient.cross_pollinate(mem.db, agent, ambient_config)
    return {"agent": agent, "items": items, "count": len(items)}


@app.get("/session-start")
async def session_start(
    agent: str = Query(..., description="Agent name (e.g. FORGE, CLIO, IRIS)"),
    max_chars: int = Query(6000, ge=500, le=12000, description="Max chars for assembled context"),
):
    """Session-start context assembler with priority tiering.

    Unlike /context (keyword search), this queries by source type directly
    so handoffs and session logs are never drowned out by BCH chat noise.

    Priority tiers:
      1. Recent agent_handoffs (to/from this agent)
      2. Recent session_logs (by/about this agent)
      3. Recent Synapse messages mentioning this agent
      4. Cross-team handoffs/logs (other agents, last 48h)
    """
    mem = get_memory()
    db = mem.db
    agent_upper = agent.upper()
    agent_lower = agent.lower()
    sections: list[str] = []
    char_count = 0

    header = f"\n--- UAIMC SESSION START [{agent_upper}] ---"
    sections.append(header)
    char_count += len(header)

    def _budget_left() -> int:
        return max_chars - char_count

    import re as _re
    import json as _json
    _DATE_PAT = _re.compile(r"(20\d{2})[-/]?(\d{2})[-/]?(\d{2})")

    def _extract_effective_date(row) -> str:
        """Extract the real content date from metadata/filepath/content/source.

        Priority: metadata date fields > filepath date > source path date
                  > content date > created_at (ingestion time, last resort).
        Returns ISO-ish string for sorting (YYYY-MM-DD...).
        """
        meta = {}
        if row["metadata"]:
            try:
                meta = _json.loads(row["metadata"])
            except (ValueError, TypeError):
                pass
        # 1. Explicit metadata date fields
        for key in ("handoff_date", "session_date", "modified_at"):
            val = meta.get(key)
            if val:
                return val
        # 2. Date from filepath in metadata
        fp = meta.get("filepath", "")
        if fp:
            m = _DATE_PAT.search(fp)
            if m:
                return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        # 3. Date from source column (mass-ingested uses filepath as source)
        src = row["source"] or ""
        m = _DATE_PAT.search(src)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        # 4. Date from first 300 chars of content
        content_head = (row["content"] or "")[:300]
        m = _DATE_PAT.search(content_head)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        # 5. Fallback: ingestion timestamp
        return row["created_at"] or "2000-01-01"

    def _sort_by_effective_date(rows, limit: int) -> list[tuple]:
        """Compute effective dates, sort descending, return top N as (row, eff_date) tuples."""
        dated = [(row, _extract_effective_date(row)) for row in rows]
        dated.sort(key=lambda x: x[1], reverse=True)
        return dated[:limit]

    def _add_section(title: str, dated_rows: list[tuple], label: str) -> int:
        nonlocal char_count
        if not dated_rows:
            return 0
        section_header = f"\n## {title}"
        if char_count + len(section_header) > max_chars:
            return 0
        sections.append(section_header)
        char_count += len(section_header)
        added = 0
        for row, eff_date in dated_rows:
            content = row["content"]
            eff = eff_date[:10] if eff_date else "unknown"
            # Trim content to fit budget, but give each entry at least 300 chars
            max_entry = min(800, _budget_left() - 20)
            if max_entry < 100:
                break
            preview = content[:max_entry]
            if len(content) > max_entry:
                preview += "..."
            entry = f"\n[{label}] ({eff}) {preview}"
            if char_count + len(entry) > max_chars:
                break
            sections.append(entry)
            char_count += len(entry)
            added += 1
        return added

    # ── Tier 1: Handoffs (to/from/about this agent) ─────────────────────
    # Fetch extra rows so Python-side date extraction + re-sort can pick the truly newest
    handoff_raw = db.execute("""
        SELECT s.id, s.content, s.source, s.created_at, s.metadata
        FROM summaries s
        WHERE (s.source = 'agent_handoff'
               OR s.source LIKE '%AGENT_HANDOFF%'
               OR s.source LIKE '%agent_handoff%')
          AND (s.content LIKE ? OR s.content LIKE ?)
        ORDER BY s.created_at DESC LIMIT 20
    """, (f"%{agent_upper}%", f"%{agent_lower}%")).fetchall()
    t1 = _add_section("HANDOFFS (Your Pickup Context)",
                       _sort_by_effective_date(handoff_raw, 3), "HANDOFF")

    # ── Tier 2: Session logs (by/about this agent) ──────────────────────
    session_raw = db.execute("""
        SELECT s.id, s.content, s.source, s.created_at, s.metadata
        FROM summaries s
        WHERE (s.source = 'session_log'
               OR s.source LIKE '%SESSION_LOG%'
               OR s.source LIKE '%session_log%')
          AND (s.content LIKE ? OR s.content LIKE ?)
        ORDER BY s.created_at DESC LIMIT 20
    """, (f"%{agent_upper}%", f"%{agent_lower}%")).fetchall()
    t2 = _add_section("SESSION LOGS (Your Recent Work)",
                       _sort_by_effective_date(session_raw, 3), "SESSION")

    # ── Tier 3: Synapse messages mentioning this agent ──────────────────
    if _budget_left() > 200:
        synapse_raw = db.execute("""
            SELECT s.id, s.content, s.source, s.created_at, s.metadata
            FROM summaries s
            WHERE (s.source = 'synapse' OR s.source LIKE '%SYNAPSE%')
              AND (s.content LIKE ? OR s.content LIKE ?)
            ORDER BY s.created_at DESC LIMIT 20
        """, (f"%{agent_upper}%", f"%{agent_lower}%")).fetchall()
        t3 = _add_section("SYNAPSE (Team Messages)",
                           _sort_by_effective_date(synapse_raw, 3), "SYNAPSE")
    else:
        t3 = 0

    # ── Tier 4: Cross-team handoffs (other agents, very recent) ─────────
    if _budget_left() > 200:
        from datetime import datetime, timedelta
        cutoff = (datetime.utcnow() - timedelta(hours=48)).strftime("%Y-%m-%d")
        cross_raw = db.execute("""
            SELECT s.id, s.content, s.source, s.created_at, s.metadata
            FROM summaries s
            WHERE (s.source = 'agent_handoff'
                   OR s.source LIKE '%AGENT_HANDOFF%')
              AND s.content NOT LIKE ?
              AND s.content NOT LIKE ?
            ORDER BY s.created_at DESC LIMIT 20
        """, (f"%{agent_upper}%", f"%{agent_lower}%")).fetchall()
        # Filter by effective date > 48h cutoff, then sort
        cross_dated = [(r, _extract_effective_date(r)) for r in cross_raw]
        cross_dated = [(r, d) for r, d in cross_dated if d[:10] >= cutoff]
        cross_dated.sort(key=lambda x: x[1], reverse=True)
        t4 = _add_section("CROSS-TEAM (Other Agents' Recent Handoffs)",
                           cross_dated[:2], "XTEAM")
    else:
        t4 = 0

    total = t1 + t2 + t3 + t4
    footer = f"\n\n[UAIMC: {total} entries | Agent: {agent_upper} | Tiers: handoffs={t1}, logs={t2}, synapse={t3}, cross-team={t4}]"
    sections.append(footer)
    sections.append("--- END SESSION START ---\n")

    context = "\n".join(sections)

    mem.log_agent_activity(agent, "session_start", f"tiers={t1},{t2},{t3},{t4}")
    uaimc_tools.track_api_usage(
        agent=agent, action="session_start",
        input_chars=len(agent), output_chars=len(context), db_path=DB_PATH,
    )

    return {
        "agent": agent,
        "context_chars": len(context),
        "max_chars": max_chars,
        "tiers": {"handoffs": t1, "session_logs": t2, "synapse": t3, "cross_team": t4},
        "total_entries": total,
        "context": context,
    }


@app.get("/health")
async def health():
    """Full system health check. Cached for 5 seconds (OPT-014)."""
    now = time.time()
    if _health_cache["data"] and (now - _health_cache["time"]) < _HEALTH_TTL:
        return _health_cache["data"]

    mem = get_memory()
    st = await asyncio.to_thread(mem.stats)

    ramdisk_ok = Path(DB_PATH).parent.exists()
    db_ok = Path(DB_PATH).exists()

    backup_db = BACKUP_DIR / Path(DB_PATH).name
    backup_age = None
    if backup_db.exists():
        backup_age = int(time.time() - backup_db.stat().st_mtime)

    result = {
        "status": "healthy" if (ramdisk_ok and db_ok) else "degraded",
        "version": "1.0.0-pp",
        "ramdisk_mounted": ramdisk_ok,
        "database_exists": db_ok,
        "db_size_bytes": st.get("db_file_size", 0),
        "annotations": st.get("annotations", 0),
        "summaries": st.get("summaries", 0),
        "verbatim": st.get("verbatim", 0),
        "backup_age_seconds": backup_age,
        "backup_integrity": uaimc_tools.verify_backup_integrity(str(BACKUP_DIR)),
        "ws_clients": len(ws_manager.active),
        "gpu_am": mem._gpu.stats() if mem._gpu else {"enabled": False},
        "gpu_triad": mem._gpu_triad.stats() if mem._gpu_triad and mem._gpu_triad.enabled else {"enabled": False},
        "bloom_cascade": {"enabled": True, "memory_kb": mem._bloom_cascade.memory_bytes // 1024} if mem._bloom_cascade else {"enabled": False},
        "aspect_index_size": mem._aspect_index.size,
        "uptime_seconds": int(time.time() - _service_start_time) if _service_start_time else 0,
        "tools_loaded": sum(1 for t in uaimc_tools.get_tools_status().values() if isinstance(t, dict) and t.get("loaded")),
        "context_cache": {
            "entries": len(mem._context_cache),
            "layers_entries": len(mem._context_layers_cache),
            "hits": mem._context_cache_hits,
            "misses": mem._context_cache_misses,
            "hit_rate": round(mem._context_cache_hits / max(mem._context_cache_hits + mem._context_cache_misses, 1), 4),
            "ttl_seconds": mem._context_cache_ttl,
            "max_entries": mem._context_cache_max,
        },
        "agent_topic_priors": {
            "agents_tracked": len(mem._agent_topic_priors),
            "agents": {a: [t[0] for t in topics[:3]] for a, topics in mem._agent_topic_priors.items()} if mem._agent_topic_priors else {},
            "last_refresh_age_seconds": round(time.time() - mem._agent_topic_priors_ts) if mem._agent_topic_priors_ts > 0 else None,
            "prediction_sequences": len(mem._agent_query_sequences),
        },
    }
    _health_cache["data"] = result
    _health_cache["time"] = time.time()
    return result


@app.get("/gpu")
async def gpu_stats():
    """GPU-AM detailed statistics."""
    mem = get_memory()
    if mem._gpu:
        return mem._gpu.stats()
    return {"enabled": False, "device": "disabled"}


@app.get("/stats")
async def stats():
    """Detailed database statistics."""
    return get_memory().stats()


@app.get("/recent")
async def recent(
    source: str = Query(None, description="Filter by source"),
    author: str = Query(None, description="Filter by author/agent"),
    limit: int = Query(20, ge=1, le=100),
):
    """Recent memory entries."""
    return get_memory().get_recent(source=source, author=author, limit=limit)


@app.post("/backup")
async def backup():
    """Trigger manual backup. OPT-028: Runs in background thread."""
    result = await asyncio.to_thread(do_backup)
    return result


@app.post("/backup/sync")
async def backup_sync(force: bool = Query(False, description="Force re-export all segments")):
    """Trigger per-segment incremental sync. Only changed segments are re-exported."""
    result = await asyncio.to_thread(do_incremental_sync, force)
    return result


@app.get("/backup/sync")
async def backup_sync_status():
    """Get the current sync manifest (segment checksums, sizes, timestamps)."""
    manifest = _load_sync_manifest()
    if not manifest.get("created_at"):
        return {"status": "no_sync_yet", "message": "Run POST /backup/sync first"}
    return manifest


@app.get("/watcher")
async def watcher_status():
    """File watcher status and statistics."""
    if _file_watcher:
        return _file_watcher.stats
    return {"running": False, "reason": "File watcher not initialized"}


@app.post("/watcher/sweep")
async def trigger_sweep():
    """Trigger an immediate file-system sweep (catches files missed by native observer)."""
    if _file_watcher and _file_watcher._sweep:
        threading.Thread(target=_file_watcher._sweep._run_sweep, daemon=True).start()
        return {"status": "sweep_triggered"}
    return JSONResponse(status_code=503, content={"status": "sweep_not_available"})


@app.post("/maintenance/prune-links")
async def prune_links(threshold: float = Query(0.1, ge=0.01, le=0.5)):
    """OPT-031: Prune weak document links below threshold."""
    mem = get_memory()
    deleted = await asyncio.to_thread(mem.prune_weak_links, threshold)
    return {"pruned": deleted, "threshold": threshold}


@app.get("/reminders")
async def get_reminders():
    """List all scheduled reminders and their status."""
    if not REMINDERS_PATH.exists():
        return {"reminders": [], "count": 0}
    try:
        with open(REMINDERS_PATH, "r", encoding="utf-8") as f:
            reminders = json.load(f)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for r in reminders:
            due = r.get("due_date", "")
            if due and today >= due and r.get("status") == "pending":
                r["_overdue"] = True
                r["_days_overdue"] = (datetime.strptime(today, "%Y-%m-%d") -
                                       datetime.strptime(due, "%Y-%m-%d")).days
            else:
                r["_overdue"] = False
                r["_days_overdue"] = 0
        return {"reminders": reminders, "count": len(reminders),
                "pending": sum(1 for r in reminders if r.get("status") == "pending"),
                "overdue": sum(1 for r in reminders if r.get("_overdue"))}
    except Exception as e:
        return {"error": str(e)}


@app.get("/agents")
async def get_agents():
    """List all known agents with activity timestamps."""
    mem = get_memory()
    try:
        rows = mem.db.execute("""
            SELECT agent_name,
                   COUNT(*) as activity_count,
                   MAX(timestamp) as last_active,
                   GROUP_CONCAT(DISTINCT action) as actions
            FROM agent_activity
            GROUP BY agent_name
            ORDER BY last_active DESC
        """).fetchall()
        return {
            "agents": [{
                "name": row["agent_name"],
                "activity_count": row["activity_count"],
                "last_active": row["last_active"],
                "actions": row["actions"].split(",") if row["actions"] else [],
            } for row in rows],
            "count": len(rows),
        }
    except Exception as e:
        logger.error(f"get_agents failed: {e}")
        return {"agents": [], "count": 0}


# ── Tool Integration Endpoints (v1.0 Production Polish) ─────────────────────

@app.get("/tools")
async def tools_status():
    """Status of all integrated AutoProject tools."""
    return uaimc_tools.get_tools_status()


@app.get("/decay")
async def context_decay(
    agent: str = Query("", description="Agent to check decay for"),
    window: int = Query(50, ge=10, le=500, description="Message window size"),
):
    """Context decay metrics — measures memory freshness and staleness."""
    mem = get_memory()
    return uaimc_tools.calculate_context_decay(mem.db, agent=agent, window_messages=window)


@app.get("/diagnostics")
async def diagnostics():
    """Expensive diagnostics moved out of /health (OPT-008)."""
    mem = get_memory()
    backup_db = BACKUP_DIR / Path(DB_PATH).name
    return {
        "schema_drift": uaimc_tools.check_schema_drift(DB_PATH, str(backup_db)),
        "context_decay": uaimc_tools.calculate_context_decay(mem.db),
    }


@app.get("/token-usage")
async def token_usage():
    """Token usage summary across all agents (via TokenTracker)."""
    return uaimc_tools.get_token_usage_summary(db_path=DB_PATH)


@app.get("/knowledge")
async def knowledge_query(
    q: str = Query("", description="Knowledge query"),
    agent: str = Query("", description="Filter by agent"),
):
    """Query the cross-agent KnowledgeSync graph."""
    if q:
        results = uaimc_tools.query_knowledge_graph(q, agent=agent)
        return {"query": q, "count": len(results), "results": results}
    return uaimc_tools.get_knowledge_stats()


@app.get("/knowledge/stats")
async def knowledge_stats():
    """KnowledgeSync statistics."""
    return uaimc_tools.get_knowledge_stats()


# ── Phase 2: Knowledge Graph API ────────────────────────────────────────────


def _normalize_path(path: str) -> str:
    """Normalize a file path to forward-slash format matching kg_nodes id convention."""
    return path.replace("\\", "/")


def _kg_node_to_dict(row: sqlite3.Row) -> dict:
    """Convert a kg_nodes row to a JSON-safe dict with parsed metadata."""
    d = {
        "id": row["id"],
        "type": row["type"],
        "name": row["name"],
        "summary": row["summary"],
        "tags": json.loads(row["tags"]) if row["tags"] else [],
        "complexity": row["complexity"],
        "source": row["source"],
        "created_at": row["created_at"],
    }
    if row["metadata"]:
        try:
            d["metadata"] = json.loads(row["metadata"])
        except (json.JSONDecodeError, TypeError):
            d["metadata"] = {}
    else:
        d["metadata"] = {}
    return d


def _kg_edge_to_dict(row: sqlite3.Row) -> dict:
    """Convert a kg_edges row to a JSON-safe dict."""
    d = {
        "id": row["id"],
        "source_id": row["source_id"],
        "target_id": row["target_id"],
        "type": row["type"],
        "weight": row["weight"],
        "description": row["description"],
        "created_at": row["created_at"],
    }
    if row["metadata"]:
        try:
            d["metadata"] = json.loads(row["metadata"])
        except (json.JSONDecodeError, TypeError):
            d["metadata"] = {}
    else:
        d["metadata"] = {}
    return d


@app.get("/graph/file")
async def graph_file(
    path: str = Query(..., description="File path (e.g. D:/BEACON_HQ/bch_check.py)"),
):
    """Return all kg_nodes for a file: functions, classes, imports, variables,
    plus the file summary, domain classification, and direct relationship edges.

    Use case: 'What's in this file?' answerable without reading the file.
    """
    mem = get_memory()
    conn = mem._pool.get()
    try:
        norm_path = _normalize_path(path)
        file_id = f"file:{norm_path}"

        # 1. Fetch the file node itself
        file_row = conn.execute(
            "SELECT * FROM kg_nodes WHERE id = ?", (file_id,)
        ).fetchone()
        if not file_row:
            raise HTTPException(status_code=404, detail=f"No kg_node found for path: {norm_path}")

        file_node = _kg_node_to_dict(file_row)
        domain = file_node["metadata"].get("domain")

        # 2. Fetch all child nodes whose source_id is this file via 'contains' edges
        child_ids_rows = conn.execute(
            "SELECT target_id FROM kg_edges WHERE source_id = ? AND type = 'contains'",
            (file_id,)
        ).fetchall()
        child_ids = [r["target_id"] for r in child_ids_rows]

        children = []
        if child_ids:
            placeholders = ",".join("?" for _ in child_ids)
            child_rows = conn.execute(
                f"SELECT * FROM kg_nodes WHERE id IN ({placeholders})", child_ids
            ).fetchall()
            children = [_kg_node_to_dict(r) for r in child_rows]

        # 3. Fetch all edges where this file (or its children) is source or target
        all_ids = [file_id] + child_ids
        placeholders = ",".join("?" for _ in all_ids)
        edge_rows = conn.execute(
            f"SELECT * FROM kg_edges WHERE source_id IN ({placeholders}) OR target_id IN ({placeholders})",
            all_ids + all_ids,
        ).fetchall()
        edges = [_kg_edge_to_dict(r) for r in edge_rows]

        # 4. Group children by type for convenience
        by_type: dict[str, list[dict]] = {}
        for child in children:
            by_type.setdefault(child["type"], []).append(child)

        return {
            "path": norm_path,
            "file_node": file_node,
            "domain": domain,
            "children": by_type,
            "children_count": len(children),
            "edges": edges,
            "edges_count": len(edges),
        }
    finally:
        mem._pool.put(conn)


@app.get("/graph/search")
async def graph_search(
    q: str = Query(..., description="Search query (name, summary text, or keyword)"),
    type: str = Query("", description="Filter by node type: file, function, class, import, variable, etc."),
    limit: int = Query(20, ge=1, le=200, description="Max results"),
):
    """Search kg_nodes by name or summary text, with optional type filtering.

    Returns matching nodes with their parent file and direct relationships.
    Use case: 'Find all functions related to authentication'
    """
    mem = get_memory()
    conn = mem._pool.get()
    try:
        # Ensure the FTS5 index exists (created once, reused)
        await asyncio.to_thread(_ensure_kg_fts, conn)

        # Build FTS5 query: quote each term for literal matching
        terms = [t.strip() for t in q.split() if t.strip()]
        if not terms:
            return {"query": q, "count": 0, "results": []}

        fts_parts = []
        for t in terms:
            escaped = t.replace('"', '""')
            fts_parts.append(f'"{escaped}"')
        fts_query = " OR ".join(fts_parts)

        # Query the FTS5 index, optionally filtered by type
        if type:
            rows = conn.execute(
                "SELECT * FROM kg_nodes WHERE rowid IN "
                "(SELECT rowid FROM kg_nodes_fts WHERE kg_nodes_fts MATCH ?) "
                "AND type = ? LIMIT ?",
                (fts_query, type, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM kg_nodes WHERE rowid IN "
                "(SELECT rowid FROM kg_nodes_fts WHERE kg_nodes_fts MATCH ?) "
                "LIMIT ?",
                (fts_query, limit),
            ).fetchall()

        results = []
        for row in rows:
            node = _kg_node_to_dict(row)

            # Find parent file for non-file nodes via 'contains' edge, with ID-stem fallback
            if node["type"] != "file":
                parent_row = conn.execute(
                    "SELECT source_id FROM kg_edges WHERE target_id = ? AND type = 'contains' LIMIT 1",
                    (node["id"],)
                ).fetchone()
                if parent_row:
                    node["parent_file"] = parent_row["source_id"]
                else:
                    # Fallback: extract stem from ID (e.g. function:bch_check_iris:func → bch_check_iris)
                    parts = node["id"].split(":")
                    if len(parts) >= 2:
                        stem = parts[1]
                        file_row = conn.execute(
                            "SELECT id FROM kg_nodes WHERE type='file' AND name LIKE ? LIMIT 1",
                            (stem + ".%",)
                        ).fetchone()
                        node["parent_file"] = file_row["id"] if file_row else None
                    else:
                        node["parent_file"] = None
            else:
                node["parent_file"] = node["id"]

            # Fetch direct edges for this node (capped to avoid huge responses)
            edge_rows = conn.execute(
                "SELECT * FROM kg_edges WHERE source_id = ? OR target_id = ? LIMIT 20",
                (node["id"], node["id"]),
            ).fetchall()
            node["edges"] = [_kg_edge_to_dict(e) for e in edge_rows]

            results.append(node)

        return {
            "query": q,
            "type_filter": type or None,
            "count": len(results),
            "results": results,
        }
    finally:
        mem._pool.put(conn)


# FTS5 index creation lock (one-time per process)
_kg_fts_ready = False
_kg_fts_lock = threading.Lock()


def _ensure_kg_fts(conn: sqlite3.Connection) -> None:
    """Create FTS5 virtual table on kg_nodes(name, summary) if it doesn't exist.

    Uses a content-sync (external content) FTS5 table so we don't duplicate data.
    Rebuilds the index if it exists but appears empty (content-sync can go stale).
    """
    global _kg_fts_ready
    if _kg_fts_ready:
        return
    with _kg_fts_lock:
        if _kg_fts_ready:
            return
        # Check if table already exists in schema
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='kg_nodes_fts'"
        ).fetchone()
        if not exists:
            logger.info("Creating FTS5 index kg_nodes_fts on kg_nodes(name, summary)...")
            conn.execute(
                "CREATE VIRTUAL TABLE kg_nodes_fts USING fts5("
                "  id, name, summary,"
                "  content=kg_nodes,"
                "  content_rowid=rowid"
                ")"
            )
            conn.execute(
                "INSERT INTO kg_nodes_fts(kg_nodes_fts) VALUES('rebuild')"
            )
            conn.connection.commit() if hasattr(conn, 'connection') else conn.commit()
            logger.info("kg_nodes_fts index created and populated.")
        else:
            # Verify index is actually populated (content-sync can go stale across restarts)
            # For content-sync FTS5, SELECT rowid returns content table rows even with empty index.
            # Use a MATCH query to verify the index itself works.
            try:
                # Pick a test word that's definitely non-empty from any node
                test_rows = conn.execute("SELECT name FROM kg_nodes WHERE length(name) > 3 LIMIT 5").fetchall()
                test_word = None
                for tr in test_rows:
                    for part in tr["name"].replace(".", " ").replace("_", " ").split():
                        if len(part) >= 3:
                            test_word = part
                            break
                    if test_word:
                        break
                if test_word:
                    test_match = conn.execute(
                        "SELECT rowid FROM kg_nodes_fts WHERE kg_nodes_fts MATCH ? LIMIT 1",
                        (f'"{test_word}"',)
                    ).fetchone()
                    if test_match is None:
                        logger.info("kg_nodes_fts MATCH test failed — rebuilding index...")
                        conn.execute(
                            "INSERT INTO kg_nodes_fts(kg_nodes_fts) VALUES('rebuild')"
                        )
                        conn.connection.commit() if hasattr(conn, 'connection') else conn.commit()
                        logger.info("kg_nodes_fts index rebuilt.")
            except Exception as e:
                logger.warning("kg_nodes_fts verify/rebuild failed: %s — dropping and recreating", e)
                try:
                    conn.execute("DROP TABLE IF EXISTS kg_nodes_fts")
                    conn.execute(
                        "CREATE VIRTUAL TABLE kg_nodes_fts USING fts5("
                        "  id, name, summary,"
                        "  content=kg_nodes,"
                        "  content_rowid=rowid"
                        ")"
                    )
                    conn.execute(
                        "INSERT INTO kg_nodes_fts(kg_nodes_fts) VALUES('rebuild')"
                    )
                    conn.connection.commit() if hasattr(conn, 'connection') else conn.commit()
                    logger.info("kg_nodes_fts recreated and rebuilt.")
                except Exception as e2:
                    logger.error("kg_nodes_fts recreation failed: %s", e2)
        _kg_fts_ready = True


@app.get("/graph/stats")
async def graph_stats():
    """Return comprehensive graph statistics: node/edge counts by type,
    domain distribution, orphan counts, FTS5 row count."""
    now = time.time()
    if _graph_stats_cache["data"] and (now - _graph_stats_cache["time"]) < _GRAPH_STATS_TTL:
        return _graph_stats_cache["data"]

    def _compute_graph_stats():
        mem = get_memory()
        conn = mem._pool.get()
        try:
            # Node counts by type
            node_rows = conn.execute(
                "SELECT type, COUNT(*) c FROM kg_nodes GROUP BY type ORDER BY c DESC"
            ).fetchall()
            nodes_by_type = {r["type"]: r["c"] for r in node_rows}
            total_nodes = sum(nodes_by_type.values())

            # Edge counts by type
            edge_rows = conn.execute(
                "SELECT type, COUNT(*) c FROM kg_edges GROUP BY type ORDER BY c DESC"
            ).fetchall()
            edges_by_type = {r["type"]: r["c"] for r in edge_rows}
            total_edges = sum(edges_by_type.values())

            # Domain distribution in summaries
            domain_rows = conn.execute(
                "SELECT json_extract(metadata, '$.domain') as domain, COUNT(*) c "
                "FROM summaries WHERE json_extract(metadata, '$.domain') IS NOT NULL "
                "GROUP BY domain ORDER BY c DESC"
            ).fetchall()
            domain_distribution = {r["domain"]: r["c"] for r in domain_rows}
            total_with_domain = sum(domain_distribution.values())
            total_summaries = conn.execute("SELECT COUNT(*) c FROM summaries").fetchone()["c"]

            # Orphan counts by type (nodes with zero edges)
            orphan_types = ["function", "class", "import", "decorator"]
            orphans = {}
            for ntype in orphan_types:
                r = conn.execute(
                    "SELECT COUNT(*) c FROM kg_nodes n "
                    "WHERE n.type = ? "
                    "AND NOT EXISTS (SELECT 1 FROM kg_edges e WHERE e.source_id = n.id OR e.target_id = n.id)",
                    (ntype,),
                ).fetchone()
                orphans[ntype] = r["c"]

            # FTS5 row count
            fts_count = 0
            try:
                fts_row = conn.execute(
                    "SELECT COUNT(*) c FROM kg_nodes_fts"
                ).fetchone()
                fts_count = fts_row["c"] if fts_row else 0
            except Exception:
                fts_count = 0  # FTS table may not exist yet

            # Annotation count
            anno_count = conn.execute("SELECT COUNT(*) c FROM annotations").fetchone()["c"]

            return {
                "nodes": {"total": total_nodes, "by_type": nodes_by_type},
                "edges": {"total": total_edges, "by_type": edges_by_type},
                "domains": {
                    "total_summaries": total_summaries,
                    "with_domain": total_with_domain,
                    "coverage_pct": round(100 * total_with_domain / max(total_summaries, 1), 1),
                    "distribution": domain_distribution,
                },
                "orphans": orphans,
                "fts5_rows": fts_count,
                "annotations_total": anno_count,
            }
        finally:
            mem._pool.put(conn)

    result = await asyncio.to_thread(_compute_graph_stats)
    _graph_stats_cache["data"] = result
    _graph_stats_cache["time"] = time.time()
    return result


# ── 3D-MAP / CANS Bridge Endpoints (Bible Sprint 1) ─────────────────────────

# Type mapping: kg_nodes type → neurolux visualization type
_3D_MAP_TYPE_MAPPING = {
    "file": "episode",
    "function": "fact",
    "class": "concept",
    "agent_reference": "agent",
    "header": "reflection",
    "config_key": "session",
    "import": "project",
    "reference": "birth_event",
}

# Cluster centers for deterministic layout (spread on sphere, radius ~100)
_3D_MAP_CLUSTER_CENTERS = {
    "episode":     (80, 0, 40),
    "fact":        (-40, 70, 50),
    "concept":     (-40, -70, 50),
    "agent":       (0, 0, -100),
    "reflection":  (60, 60, -40),
    "session":     (-60, 60, -40),
    "project":     (60, -60, -40),
    "birth_event": (-60, -60, -40),
}

_3D_MAP_MAX_OVERVIEW_NODES = 300
_3d_map_overview_cache: dict = {"data": None, "time": 0}
_3D_MAP_OVERVIEW_TTL = 60  # seconds


def _3d_map_position(node_id: str, vis_type: str) -> tuple[float, float, float]:
    """Deterministic x/y/z position from node ID and visual type."""
    cx, cy, cz = _3D_MAP_CLUSTER_CENTERS.get(vis_type, (0, 0, 0))
    h = hash(node_id) & 0xFFFFFFFF
    # Spread within ±30 of cluster center using hash bits
    ox = ((h & 0xFFFF) / 0xFFFF - 0.5) * 60
    oy = (((h >> 8) & 0xFFFF) / 0xFFFF - 0.5) * 60
    oz = (((h >> 16) & 0xFFFF) / 0xFFFF - 0.5) * 60
    return (round(cx + ox, 2), round(cy + oy, 2), round(cz + oz, 2))


def _kg_node_to_3d(row: sqlite3.Row) -> dict:
    """Convert a kg_nodes row to a 3D-map compatible node dict."""
    kg_type = row["type"]
    vis_type = _3D_MAP_TYPE_MAPPING.get(kg_type, "fact")
    x, y, z = _3d_map_position(row["id"], vis_type)
    return {
        "id": row["id"],
        "label": row["name"],
        "type": vis_type,
        "summary": (row["summary"] or "")[:200],
        "importance": row["complexity"] if row["complexity"] is not None else 0.5,
        "timestamp": row["created_at"],
        "x": x, "y": y, "z": z,
    }


@app.get("/api/v1/3d-map/overview")
async def threed_map_overview():
    """Return a sampled graph overview for 3D neural map visualization.

    Samples top nodes by type diversity (up to 300), computes deterministic
    x/y/z positions, fetches edges between visible nodes only.
    Bible: Sprint 1, Step 1.1
    """
    now = time.time()
    if _3d_map_overview_cache["data"] and (now - _3d_map_overview_cache["time"]) < _3D_MAP_OVERVIEW_TTL:
        return _3d_map_overview_cache["data"]

    mem = get_memory()
    conn = mem._pool.get()
    try:
        def _compute():
            # Sample top-N per type for diversity (top 20 each by complexity)
            sampled_types = ["file", "function", "class", "agent_reference", "config_key", "header"]
            node_rows = []
            seen_ids = set()
            for ntype in sampled_types:
                rows = conn.execute(
                    "SELECT * FROM kg_nodes WHERE type = ? ORDER BY complexity DESC LIMIT 20",
                    (ntype,),
                ).fetchall()
                for r in rows:
                    if r["id"] not in seen_ids:
                        node_rows.append(r)
                        seen_ids.add(r["id"])

            # Fill remaining slots with top-complexity nodes across all types
            remaining = _3D_MAP_MAX_OVERVIEW_NODES - len(node_rows)
            if remaining > 0:
                placeholders = ",".join("?" * len(seen_ids)) if seen_ids else "''"
                q = f"SELECT * FROM kg_nodes WHERE id NOT IN ({placeholders}) ORDER BY complexity DESC LIMIT ?"
                params = list(seen_ids) + [remaining]
                fill_rows = conn.execute(q, params).fetchall()
                for r in fill_rows:
                    if r["id"] not in seen_ids:
                        node_rows.append(r)
                        seen_ids.add(r["id"])

            # Build 3D nodes
            nodes_3d = [_kg_node_to_3d(r) for r in node_rows]

            # Fetch edges between visible nodes only
            if seen_ids:
                id_list = list(seen_ids)
                # SQLite has a variable limit; batch if needed
                batch_size = 500
                edges_3d = []
                for i in range(0, len(id_list), batch_size):
                    batch = id_list[i:i + batch_size]
                    ph = ",".join("?" * len(batch))
                    edge_rows = conn.execute(
                        f"SELECT * FROM kg_edges WHERE source_id IN ({ph}) AND target_id IN ({ph}) LIMIT 2000",
                        batch + batch,
                    ).fetchall()
                    for er in edge_rows:
                        edges_3d.append({
                            "source": er["source_id"],
                            "target": er["target_id"],
                            "type": er["type"],
                            "weight": er["weight"],
                        })
                        if len(edges_3d) >= 2000:
                            break
                    if len(edges_3d) >= 2000:
                        break
            else:
                edges_3d = []

            # Stats from cache or quick count
            total_nodes = conn.execute("SELECT COUNT(*) c FROM kg_nodes").fetchone()["c"]
            total_edges = conn.execute("SELECT COUNT(*) c FROM kg_edges").fetchone()["c"]

            return {
                "nodes": nodes_3d,
                "edges": edges_3d,
                "stats": {
                    "total_nodes": total_nodes,
                    "total_edges": total_edges,
                    "visible_nodes": len(nodes_3d),
                    "visible_edges": len(edges_3d),
                },
            }

        result = await asyncio.to_thread(_compute)
        _3d_map_overview_cache["data"] = result
        _3d_map_overview_cache["time"] = time.time()
        return result
    finally:
        mem._pool.put(conn)


@app.get("/api/v1/3d-map/search")
async def threed_map_search(
    q: str = Query(..., description="Search query for nodes"),
    limit: int = Query(30, ge=1, le=100, description="Max results"),
    type: str = Query("", description="Optional node type filter"),
):
    """Search kg_nodes and return results in 3D-map compatible shape.

    Uses FTS5 for text matching, maps node types to neurolux visual types,
    computes deterministic x/y/z positions.
    Bible: Sprint 1, Step 1.2
    """
    mem = get_memory()
    conn = mem._pool.get()
    try:
        await asyncio.to_thread(_ensure_kg_fts, conn)

        terms = [t.strip() for t in q.split() if t.strip()]
        if not terms:
            return {"query": q, "nodes": [], "count": 0}

        fts_parts = []
        for t in terms:
            escaped = t.replace('"', '""')
            fts_parts.append(f'"{escaped}"')
        fts_query = " OR ".join(fts_parts)

        def _search():
            if type:
                rows = conn.execute(
                    "SELECT * FROM kg_nodes WHERE rowid IN "
                    "(SELECT rowid FROM kg_nodes_fts WHERE kg_nodes_fts MATCH ?) "
                    "AND type = ? LIMIT ?",
                    (fts_query, type, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM kg_nodes WHERE rowid IN "
                    "(SELECT rowid FROM kg_nodes_fts WHERE kg_nodes_fts MATCH ?) "
                    "LIMIT ?",
                    (fts_query, limit),
                ).fetchall()
            return [_kg_node_to_3d(r) for r in rows]

        nodes = await asyncio.to_thread(_search)
        return {"query": q, "nodes": nodes, "count": len(nodes)}
    finally:
        mem._pool.put(conn)


@app.get("/api/v1/3d-map/expand/{node_id:path}")
async def threed_map_expand(node_id: str):
    """Return the 1-hop neighborhood of a given node for graph expansion.

    Fetches all edges connected to the node, collects neighbor node data,
    arranges neighbors in a ring layout around the center node.
    Bible: Sprint 1, Step 1.3
    """
    mem = get_memory()
    conn = mem._pool.get()
    try:
        def _expand():
            # Fetch center node
            center_row = conn.execute(
                "SELECT * FROM kg_nodes WHERE id = ?", (node_id,)
            ).fetchone()
            if not center_row:
                return None

            # Fetch 1-hop edges
            edge_rows = conn.execute(
                "SELECT * FROM kg_edges WHERE source_id = ? OR target_id = ? LIMIT 50",
                (node_id, node_id),
            ).fetchall()

            # Collect neighbor IDs
            neighbor_ids = set()
            for er in edge_rows:
                if er["source_id"] != node_id:
                    neighbor_ids.add(er["source_id"])
                if er["target_id"] != node_id:
                    neighbor_ids.add(er["target_id"])

            # Fetch neighbor nodes
            neighbor_nodes = []
            if neighbor_ids:
                ph = ",".join("?" * len(neighbor_ids))
                n_rows = conn.execute(
                    f"SELECT * FROM kg_nodes WHERE id IN ({ph})",
                    list(neighbor_ids),
                ).fetchall()

                # Ring layout: place neighbors around center
                center_3d = _kg_node_to_3d(center_row)
                cx, cy, cz = center_3d["x"], center_3d["y"], center_3d["z"]
                ring_radius = 30
                n_count = len(n_rows)
                for idx, nr in enumerate(n_rows):
                    node_3d = _kg_node_to_3d(nr)
                    # Override position to ring layout
                    angle = (2 * 3.14159265 * idx) / max(n_count, 1)
                    node_3d["x"] = round(cx + ring_radius * math.cos(angle), 2)
                    node_3d["y"] = round(cy + ring_radius * math.sin(angle), 2)
                    node_3d["z"] = round(cz + (hash(nr["id"]) % 20 - 10), 2)
                    neighbor_nodes.append(node_3d)

            # Build edges in 3d-map shape
            edges_3d = []
            for er in edge_rows:
                edges_3d.append({
                    "source": er["source_id"],
                    "target": er["target_id"],
                    "type": er["type"],
                    "weight": er["weight"],
                })

            # Include center node in response
            all_nodes = [_kg_node_to_3d(center_row)] + neighbor_nodes

            return {
                "center_node": node_id,
                "nodes": all_nodes,
                "edges": edges_3d,
                "count": {"nodes": len(all_nodes), "edges": len(edges_3d)},
            }

        result = await asyncio.to_thread(_expand)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")
        return result
    finally:
        mem._pool.put(conn)


@app.get("/schema-drift")
async def schema_drift():
    """Check schema drift between live DB and backup DB."""
    backup_db = str(BACKUP_DIR / Path(DB_PATH).name)
    return uaimc_tools.check_schema_drift(DB_PATH, backup_db)


@app.get("/backup-integrity")
async def backup_integrity():
    """Verify backup file integrity via HashGuard."""
    return uaimc_tools.verify_backup_integrity(str(BACKUP_DIR))


# ── Guardian AI Endpoints (B-004) ────────────────────────────────────────────

@app.post("/guardian/query")
async def guardian_query(
    q: str = Query(..., description="Knowledge query for Guardian"),
    agent: str = Query("anonymous", description="Agent making the query"),
    tier: str = Query("", description="Force query tier (LITE/STANDARD/DEEP/EXHAUSTIVE)"),
):
    """Submit a knowledge query to Guardian AI (Opus-enhanced semantic search)."""
    guardian = get_guardian()
    if not guardian:
        raise HTTPException(status_code=503, detail="Guardian AI is not available")
    tier_override = tier.upper() if tier else None
    result = await guardian.query(q, agent=agent, tier_override=tier_override)
    return result


@app.get("/guardian/query")
async def guardian_query_get(
    q: str = Query(..., description="Knowledge query for Guardian"),
    agent: str = Query("anonymous", description="Agent making the query"),
    tier: str = Query("", description="Force query tier"),
):
    """GET variant of Guardian query (for curl convenience)."""
    guardian = get_guardian()
    if not guardian:
        raise HTTPException(status_code=503, detail="Guardian AI is not available")
    tier_override = tier.upper() if tier else None
    result = await guardian.query(q, agent=agent, tier_override=tier_override)
    return result


@app.get("/guardian/status")
async def guardian_status():
    """Guardian health, mode, model, budget, and tool status."""
    guardian = get_guardian()
    if not guardian:
        return {
            "status": "unavailable",
            "mode": "offline",
            "model": None,
            "opus_api_available": False,
            "message": "Guardian module not loaded",
        }
    return guardian.get_status()


@app.get("/guardian/budget")
async def guardian_budget():
    """Guardian API budget usage report."""
    guardian = get_guardian()
    if not guardian:
        raise HTTPException(status_code=503, detail="Guardian AI is not available")
    return guardian.get_budget_report()


@app.get("/guardian/cache")
async def guardian_cache():
    """Guardian cache statistics and management."""
    guardian = get_guardian()
    if not guardian:
        raise HTTPException(status_code=503, detail="Guardian AI is not available")
    return guardian._cache_stats()


@app.post("/guardian/cache/clear")
async def guardian_cache_clear():
    """Clear expired Guardian cache entries."""
    guardian = get_guardian()
    if not guardian:
        raise HTTPException(status_code=503, detail="Guardian AI is not available")
    removed = guardian._cache_cleanup()
    return {"removed": removed, "message": f"Removed {removed} expired cache entries"}


@app.post("/guardian/curate")
async def guardian_curate(
    scan_type: str = Query("quick", description="Curation type: quick or standard"),
):
    """Trigger a Guardian curation scan (RC — The Librarian Curates)."""
    guardian = get_guardian()
    if not guardian:
        raise HTTPException(status_code=503, detail="Guardian AI is not available")
    if scan_type not in ("quick", "standard"):
        raise HTTPException(status_code=400, detail="scan_type must be 'quick' or 'standard'")
    result = await guardian.curate(scan_type=scan_type)
    return result


@app.post("/guardian/synthesize")
async def guardian_synthesize(
    concept_id: str = Query(None, description="Concept ID to synthesize reflections for (auto-selects if omitted)"),
    min_facts: int = Query(5, ge=1, description="Minimum facts required for synthesis"),
    max_facts: int = Query(20, ge=1, le=100, description="Max facts to include in prompt"),
    min_confidence: float = Query(0.7, ge=0.0, le=1.0, description="Minimum reflection confidence to store"),
):
    """Trigger Guardian reflection synthesis for a concept (Phase 3).

    Runs PPR-ranked fact selection → Opus reflection synthesis → stores reflection_nodes + edges.
    If concept_id is omitted, auto-selects the highest-degree concept with enough facts.
    """
    guardian = get_guardian()
    if not guardian:
        raise HTTPException(status_code=503, detail="Guardian AI is not available")
    result = await guardian.synthesize_reflections(
        concept_id=concept_id,
        min_facts=min_facts,
        max_facts_for_prompt=max_facts,
        min_confidence=min_confidence,
    )
    return result


@app.get("/guardian/flags")
async def guardian_flags(
    status: str = Query("", description="Filter by status: OPEN, RESOLVED, DISMISSED"),
    flag_type: str = Query("", description="Filter by type: DUPLICATE, CONTRADICTION, STALE, QUALITY, ORPHAN"),
    limit: int = Query(50, ge=1, le=500, description="Max flags to return"),
):
    """List Guardian curation flags with optional filters."""
    guardian = get_guardian()
    if not guardian:
        raise HTTPException(status_code=503, detail="Guardian AI is not available")
    flags = guardian.get_flags(
        status=status if status else None,
        flag_type=flag_type if flag_type else None,
        limit=limit,
    )
    return {"flags": flags, "count": len(flags)}


@app.post("/guardian/flags/{flag_id}/resolve")
async def guardian_flag_resolve(
    flag_id: int,
    action: str = Query("RESOLVED", description="RESOLVED or DISMISSED"),
    resolved_by: str = Query("MANUAL", description="Who resolved the flag"),
):
    """Resolve or dismiss a Guardian curation flag."""
    guardian = get_guardian()
    if not guardian:
        raise HTTPException(status_code=503, detail="Guardian AI is not available")
    if action.upper() not in ("RESOLVED", "DISMISSED"):
        raise HTTPException(status_code=400, detail="action must be RESOLVED or DISMISSED")
    result = guardian.resolve_flag(flag_id, resolved_by=resolved_by, action=action.upper())
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


# ── v1.0 ENDPOINTS ──────────────────────────────────────────────────────────

@app.get("/guardian/performance")
async def guardian_performance():
    """Query latency percentiles (p50/p95/p99) from recent queries."""
    guardian = get_guardian()
    if not guardian:
        raise HTTPException(status_code=503, detail="Guardian AI is not available")
    return guardian.get_performance()


@app.get("/guardian/errors")
async def guardian_errors(limit: int = Query(50, ge=1, le=200)):
    """Recent structured error log entries."""
    guardian = get_guardian()
    if not guardian:
        raise HTTPException(status_code=503, detail="Guardian AI is not available")
    return guardian.get_error_log(limit=limit)


@app.get("/guardian/welcome")
async def guardian_welcome(agent: str = Query(..., description="Agent name requesting onboarding")):
    """Welcome packet for a new or returning agent — status, topics, tips."""
    guardian = get_guardian()
    if not guardian:
        raise HTTPException(status_code=503, detail="Guardian AI is not available")
    return guardian.welcome_packet(agent)


@app.post("/guardian/resolve-model")
async def guardian_resolve_model():
    """Check Anthropic API for latest Opus model and auto-bump if newer."""
    guardian = get_guardian()
    if not guardian:
        raise HTTPException(status_code=503, detail="Guardian AI is not available")
    resolved = await guardian.resolve_opus_model()
    return {"model": resolved}


@app.get("/mcp-tools")
async def mcp_tools():
    """MCP tool definitions for UAIMC (for MCPBridge registration)."""
    return {
        "tools": uaimc_tools.get_mcp_tool_definitions(),
        "registration": uaimc_tools.register_with_mcp_bridge(),
    }


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """Real-time feed of new memory entries."""
    await ws_manager.connect(ws)
    logger.info(f"WebSocket client connected ({len(ws_manager.active)} total)")
    try:
        while True:
            data = await ws.receive_text()
            # Clients can send queries via WS
            try:
                msg = json.loads(data)
                if msg.get("action") == "query":
                    mem = get_memory()
                    ws_agent = msg.get("agent", "*")
                    results = mem.query_text(msg.get("q", ""), limit=msg.get("limit", 10), agent=ws_agent)
                    await ws.send_json({"event": "query_result", "results": results})
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)
        logger.info(f"WebSocket client disconnected ({len(ws_manager.active)} remaining)")


# ── Phase A4: Progressive Disclosure Endpoints (B-013) ───────────────────────

class DiscoverRequest(BaseModel):
    q: str
    agent: str = ""
    limit: int = 20


class DiscoverRespondRequest(BaseModel):
    summary_id: int
    response: str  # 'explore' | 'ignore' | 'reject'


@app.post("/query/discover")
async def query_discover(req: DiscoverRequest):
    """B-013: Initiate a progressive disclosure session.
    Returns Layer 0 annotations + session_id. NOT full results."""
    _cleanup_expired_sessions()
    mem = get_memory()
    keywords = [a.token for a in uaimc_anno.annotate(req.q)[:8]]
    if not keywords:
        raise HTTPException(status_code=400, detail="No keywords extracted from query")

    # Run standard query pipeline to get seed results
    seed_results = mem.query(req.q, limit=req.limit, agent=req.agent or "*")

    # Create session
    session_id = str(uuid.uuid4())
    query_hash = hashlib.sha256(
        " ".join(sorted(kw.lower() for kw in keywords)).encode()
    ).hexdigest()[:16]

    session = DisclosureSession(
        session_id=session_id,
        query_hash=query_hash,
        seed_results=seed_results,
        agent=req.agent,
        db=mem.db,
    )
    _disclosure_sessions[session_id] = session

    # Get Layer 0 candidates and group into cohorts
    candidates = session.get_layer_candidates(layer=0)
    cohort_size = CONFIG.get("scoring", {}).get("disclosure_cohort_size", 3)
    cohorts = DisclosureSession.group_into_cohorts(candidates, cohort_size)

    return {
        "session_id": session_id,
        "query_hash": query_hash,
        "layer": 0,
        "cohorts": cohorts,
        "candidate_count": len(candidates),
        "seed_result_count": len(seed_results),
    }


@app.post("/query/discover/{session_id}/respond")
async def query_discover_respond(session_id: str, req: DiscoverRespondRequest):
    """B-013: Record user response (explore/ignore/reject) and return next layer if explored."""
    session = _disclosure_sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or expired")

    if req.response not in ("explore", "ignore", "reject"):
        raise HTTPException(status_code=400, detail="response must be 'explore', 'ignore', or 'reject'")

    # Record response (this handles B-010/B-011 integration, link degradation, etc.)
    session.record_response(req.summary_id, session.current_layer, req.response)

    result = {
        "session_id": session_id,
        "summary_id": req.summary_id,
        "response": req.response,
        "layer": session.current_layer,
    }

    if req.response == "explore":
        # Open next layer for this item
        next_layer = session.current_layer + 1
        if next_layer <= 3:
            session.current_layer = next_layer
            candidates = session.get_layer_candidates(layer=next_layer, parent_id=req.summary_id)
            result["next_layer"] = next_layer
            result["candidates"] = candidates
            result["candidate_count"] = len(candidates)
        else:
            result["next_layer"] = None
            result["candidates"] = []
            result["message"] = "Maximum depth reached"
    elif req.response == "reject":
        result["link_state_update"] = "degradation_evaluated"

    return result


@app.get("/query/discover/{session_id}/status")
async def query_discover_status(session_id: str):
    """B-013: Get current disclosure session state."""
    session = _disclosure_sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    return session.get_status()


# ── Dashboard ────────────────────────────────────────────────────────────────
WWW_DIR = Path(__file__).parent / "www"

@app.get("/favicon.ico")
async def favicon():
    """Serve favicon."""
    ico = WWW_DIR / "UAIMC.ico"
    if ico.exists():
        return FileResponse(str(ico), media_type="image/x-icon")
    return HTMLResponse(status_code=404)

@app.get("/www/{filename}")
async def serve_www(filename: str):
    """Serve static files from www/ directory."""
    safe_name = Path(filename).name  # prevent path traversal
    file_path = WWW_DIR / safe_name
    if file_path.exists() and file_path.is_file():
        media_types = {".png": "image/png", ".ico": "image/x-icon", ".jpg": "image/jpeg", ".svg": "image/svg+xml", ".css": "text/css", ".js": "application/javascript"}
        mt = media_types.get(file_path.suffix.lower(), "application/octet-stream")
        return FileResponse(str(file_path), media_type=mt)
    return HTMLResponse(status_code=404)

@app.get("/pulse", response_class=HTMLResponse)
async def pulse_dashboard():
    """Serve UAIMC Pulse — the live dashboard with search + chat."""
    html_path = WWW_DIR / "pulse.html"
    if html_path.exists():
        return FileResponse(str(html_path), media_type="text/html")
    return HTMLResponse(status_code=404)


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the UAIMC dashboard."""
    html_path = WWW_DIR / "dashboard.html"
    if html_path.exists():
        return FileResponse(str(html_path), media_type="text/html")
    return HTMLResponse(
        "<html><body><h1>UAIMC — Universal AI Memory Core</h1>"
        "<p>Alpha v1.0.0 | Dashboard not found. Place dashboard.html in www/</p>"
        f"<p>DB: {DB_PATH}</p>"
        f"<p><a href='/docs'>API Docs</a> | <a href='/health'>Health</a> | <a href='/stats'>Stats</a></p>"
        "</body></html>"
    )


# ── Entry Point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT, log_level="info",
                limit_concurrency=30, backlog=128)
