r"""
UAIMC GPU-AM (GPU Annotation Map) v1.0-rc
==========================================
GPU-resident tensor hash map for parallel annotation recall.
Ported from server gpu_memory.py, adapted for UAIMC local config.

Loads FNV-1a hashes + summary_ids from UAIMC SQLite into CUDA tensors.
Falls back to CPU tensors or disables if GPU unavailable.

Architecture:
  - Three parallel tensors: hashes (int64) + summary_ids (int64) + weights (float32)
  - Query = vectorized isin() -> gather summary_ids -> aggregate weights
  - LRU eviction when cap reached (oldest by insertion order)
  - Periodic stats logging (SQLite is authoritative)

Config driven by UAIMC config.json:
  gpu.enabled     (default: false)
  gpu.max_mb      (default: 200)
  gpu.sync_interval_seconds (default: 30)

PRIVATE -- Not for publication
COPILOT_VSCODE (Team Brain) | March 15, 2026
"""

import json
import logging
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger("uaimc.gpu")

# ── Configuration ────────────────────────────────────────────────────────────
_CONFIG_PATH = Path(__file__).parent / "config" / "config.json"


def _load_gpu_config() -> dict:
    """Load GPU config from UAIMC config.json."""
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, "r") as f:
            cfg = json.load(f)
        return cfg.get("gpu", {})
    return {}


_GPU_CFG = _load_gpu_config()

GPU_ENABLED = _GPU_CFG.get("enabled", False)
GPU_MAX_MB = int(_GPU_CFG.get("max_mb", 200))
SYNC_INTERVAL = int(_GPU_CFG.get("sync_interval_seconds", 30))

# Env vars can override config (same as server)
if os.getenv("GPU_MEMORY_ENABLED", "").lower() in ("true", "1", "yes"):
    GPU_ENABLED = True
if os.getenv("GPU_MEMORY_MAX_MB"):
    GPU_MAX_MB = int(os.getenv("GPU_MEMORY_MAX_MB"))
if os.getenv("GPU_SYNC_INTERVAL_SEC"):
    SYNC_INTERVAL = int(os.getenv("GPU_SYNC_INTERVAL_SEC"))

# 8 (hash) + 8 (summary_id) + 4 (weight float32) = 20 bytes per entry
BYTES_PER_ENTRY = 20
MAX_ENTRIES = (GPU_MAX_MB * 1024 * 1024) // BYTES_PER_ENTRY

# ── Lazy torch import ────────────────────────────────────────────────────────
_torch = None
_device = None


def _init_torch():
    """Lazy init: import torch, select device. Returns True if GPU available."""
    global _torch, _device
    if _torch is not None:
        return _device is not None and _device.type == "cuda"
    try:
        import torch
        _torch = torch
        if torch.cuda.is_available():
            _device = torch.device("cuda")
            free_mb = torch.cuda.mem_get_info()[0] / (1024 * 1024)
            needed = GPU_MAX_MB + 200  # Safety margin
            if free_mb < needed:
                logger.warning(
                    f"GPU-AM: Only {free_mb:.0f}MB free, need {needed}MB. Falling back to CPU."
                )
                _device = torch.device("cpu")
            else:
                logger.info(f"GPU-AM: CUDA available. {free_mb:.0f}MB free. Cap: {GPU_MAX_MB}MB")
        else:
            _device = torch.device("cpu")
            logger.info("GPU-AM: No CUDA — running on CPU tensors (still faster than FTS5 at scale)")
        return _device.type == "cuda"
    except ImportError:
        logger.warning("GPU-AM: PyTorch not installed. GPU-AM disabled.")
        _torch = False
        _device = None
        return False


class GPUAnnotationMap:
    """GPU-resident (or CPU fallback) tensor hash map for annotation recall."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.enabled = GPU_ENABLED
        self._lock = threading.Lock()

        # Tensors
        self._hashes = None       # int64 tensor of FNV-1a hashes
        self._summary_ids = None  # int64 tensor of summary_ids
        self._weights = None      # float32 tensor of weights
        self._count = 0
        self._allocated = 0  # OPT-020: Track allocated tensor size
        self._capacity = MAX_ENTRIES

        # Sync daemon
        self._sync_thread = None
        self._stop_event = threading.Event()

        # Stats
        self._gpu_queries = 0
        self._gpu_hits = 0
        self._load_time_ms = 0.0
        self._last_sync = 0.0

        if self.enabled:
            is_gpu = _init_torch()
            if _torch is False:
                self.enabled = False
                logger.warning("GPU-AM: Disabled (PyTorch not available)")
                return
            self.device_name = "cuda" if is_gpu else "cpu"
            self._load_from_disk()
            self._start_sync_daemon()
        else:
            self.device_name = "disabled"
            logger.info("GPU-AM: Disabled by config (gpu.enabled=false)")

    def _load_from_disk(self):
        """B-TC-007α: Load HOT-TIER annotations (last 30 days) from SQLite into tensors.

        Cold annotations (>30 days) remain queryable via FTS5 in SQLite on demand.
        Uses annotations.created_at for recency (not summary_id, which reflects
        when the *file* was first ingested, not when the annotation was derived).
        OPT-020: Pre-allocates tensors with growth headroom to avoid torch.cat copies.
        """
        t0 = time.perf_counter()
        try:
            db = sqlite3.connect(self.db_path)

            # Ensure index exists for efficient created_at filtering
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_annotations_created "
                "ON annotations(created_at)"
            )

            # Load annotations created in the last 30 days
            rows = db.execute(
                "SELECT token_hash, summary_id, weight FROM annotations "
                "WHERE created_at >= datetime('now', '-30 days') LIMIT ?",
                (self._capacity,)
            ).fetchall()
            db.close()

            if not rows:
                logger.info("GPU-AM: No annotations to load")
                return

            n = len(rows)
            hashes = [r[0] for r in rows]
            sids = [r[1] for r in rows]
            weights = [r[2] for r in rows]

            # OPT-020: Allocate exact size for initial load (no waste)
            with self._lock:
                self._hashes = _torch.tensor(hashes, dtype=_torch.int64, device=_device)
                self._summary_ids = _torch.tensor(sids, dtype=_torch.int64, device=_device)
                self._weights = _torch.tensor(weights, dtype=_torch.float16, device=_device)
                self._count = n
                self._allocated = n  # OPT-020: Track allocated size

            elapsed = (time.perf_counter() - t0) * 1000
            self._load_time_ms = elapsed

            mem_mb = (self._count * BYTES_PER_ENTRY) / (1024 * 1024)
            logger.info(
                f"GPU-AM: Loaded {self._count:,} annotations -> {self.device_name} "
                f"({mem_mb:.2f}MB, {elapsed:.1f}ms)"
            )
        except Exception as e:
            logger.error(f"GPU-AM: Load failed: {e}")
            self.enabled = False

    def query(self, token_hashes: list[int], limit: int = 10) -> list[dict]:
        """Parallel hash match: find all annotations matching any given hashes.

        Returns list of {summary_id, weight} sorted by weight descending.
        """
        if not self.enabled or self._hashes is None or not token_hashes:
            return []

        t0 = time.perf_counter()
        self._gpu_queries += 1

        try:
            query_tensor = _torch.tensor(token_hashes, dtype=_torch.int64, device=_device)

            with self._lock:
                # OPT-020: Only scan [0:_count] valid entries
                n = self._count
                if n == 0:
                    return []
                active_hashes = self._hashes[:n]
                mask = _torch.isin(active_hashes, query_tensor)
                if not mask.any():
                    return []

                matched_sids = self._summary_ids[:n][mask]
                matched_weights = self._weights[:n][mask]

            # Vectorized aggregation: O(n) GPU scatter instead of O(n*k) Python loop
            unique_sids, inverse = _torch.unique(matched_sids, return_inverse=True)
            agg_weights = _torch.zeros(unique_sids.shape[0], dtype=_torch.float32, device=_device)
            agg_weights.scatter_add_(0, inverse, matched_weights.float())

            # Get top-k by weight (avoid full sort when limit << n)
            k = min(limit, unique_sids.shape[0])
            top_weights, top_idx = agg_weights.topk(k)

            # Transfer only the top-k results to CPU
            top_sids_cpu = unique_sids[top_idx].cpu().tolist()
            top_weights_cpu = top_weights.cpu().tolist()

            results = [
                {"summary_id": sid, "weight": round(w, 3)}
                for sid, w in zip(top_sids_cpu, top_weights_cpu)
            ]

            self._gpu_hits += unique_sids.shape[0]

            elapsed_us = (time.perf_counter() - t0) * 1_000_000
            if elapsed_us > 1000:
                logger.debug(
                    f"GPU-AM query: {len(token_hashes)} hashes -> {unique_sids.shape[0]} unique sids, "
                    f"top {k} returned in {elapsed_us:.0f}us"
                )

            return results

        except Exception as e:
            logger.error(f"GPU-AM query failed: {e}")
            return []

    def _ensure_tensors(self, min_size: int = 1024):
        """OPT-020: Lazily allocate tensors if not yet created."""
        if self._hashes is None:
            alloc = min(min_size, self._capacity)
            self._hashes = _torch.zeros(alloc, dtype=_torch.int64, device=_device)
            self._summary_ids = _torch.zeros(alloc, dtype=_torch.int64, device=_device)
            self._weights = _torch.zeros(alloc, dtype=_torch.float16, device=_device)
            self._count = 0
            self._allocated = alloc

    def _grow_if_needed(self, needed: int):
        """OPT-020: Double tensor allocation when space runs out (amortized O(1) appends)."""
        if self._count + needed <= self._allocated:
            return  # Enough room
        new_alloc = min(max(self._allocated * 2, self._count + needed), self._capacity)
        if new_alloc == self._allocated:
            return  # At hard cap
        new_h = _torch.zeros(new_alloc, dtype=_torch.int64, device=_device)
        new_s = _torch.zeros(new_alloc, dtype=_torch.int64, device=_device)
        new_w = _torch.zeros(new_alloc, dtype=_torch.float16, device=_device)
        new_h[:self._count] = self._hashes[:self._count]
        new_s[:self._count] = self._summary_ids[:self._count]
        new_w[:self._count] = self._weights[:self._count]
        self._hashes = new_h
        self._summary_ids = new_s
        self._weights = new_w
        self._allocated = new_alloc
        logger.debug(f"GPU-AM: Grew tensors to {new_alloc:,} slots")

    def _evict_oldest(self, needed: int):
        """OPT-020: Shift tensor data left to free `needed` slots at the end."""
        keep = self._count - needed
        if keep <= 0:
            self._count = 0
            return
        self._hashes[:keep] = self._hashes[needed:self._count].clone()
        self._summary_ids[:keep] = self._summary_ids[needed:self._count].clone()
        self._weights[:keep] = self._weights[needed:self._count].clone()
        self._count = keep
        logger.debug(f"GPU-AM: LRU evicted {needed} oldest entries")

    def add(self, token_hash: int, summary_id: int, weight: float = 1.0):
        """Add a single annotation to the GPU map (called after disk write).
        OPT-020: Writes at index instead of torch.cat — O(1) amortized per call."""
        if not self.enabled or _torch is False:
            return

        try:
            with self._lock:
                self._ensure_tensors()

                if self._count >= self._capacity:
                    self._evict_oldest(1)
                else:
                    self._grow_if_needed(1)

                idx = self._count
                self._hashes[idx] = token_hash
                self._summary_ids[idx] = summary_id
                self._weights[idx] = weight
                self._count += 1

        except Exception as e:
            logger.error(f"GPU-AM add failed: {e}")

    def add_batch(self, entries: list[tuple[int, int, float]]):
        """Add multiple annotations at once.
        OPT-020: Writes slice at index instead of torch.cat — O(batch) amortized."""
        if not self.enabled or _torch is False or not entries:
            return

        try:
            hashes = [e[0] for e in entries]
            sids = [e[1] for e in entries]
            weights = [e[2] for e in entries]
            batch_size = len(entries)

            with self._lock:
                self._ensure_tensors(batch_size)

                # If batch exceeds capacity, only keep the last _capacity entries
                if batch_size >= self._capacity:
                    offset = batch_size - self._capacity
                    hashes = hashes[offset:]
                    sids = sids[offset:]
                    weights = weights[offset:]
                    batch_size = self._capacity
                    self._count = 0  # overwrite everything

                # Evict if at hard cap
                if self._count + batch_size > self._capacity:
                    self._evict_oldest(self._count + batch_size - self._capacity)

                # Grow allocation if needed (but not past capacity)
                self._grow_if_needed(batch_size)

                idx = self._count
                self._hashes[idx:idx + batch_size] = _torch.tensor(hashes, dtype=_torch.int64, device=_device)
                self._summary_ids[idx:idx + batch_size] = _torch.tensor(sids, dtype=_torch.int64, device=_device)
                self._weights[idx:idx + batch_size] = _torch.tensor(weights, dtype=_torch.float16, device=_device)
                self._count += batch_size

        except Exception as e:
            logger.error(f"GPU-AM add_batch failed: {e}")

    def _start_sync_daemon(self):
        """Start background thread that periodically logs GPU state."""
        def _sync_loop():
            while not self._stop_event.wait(SYNC_INTERVAL):
                self._last_sync = time.time()
                if self._count > 0:
                    logger.debug(
                        f"GPU-AM sync: {self._count:,} entries, "
                        f"{self._gpu_queries} queries, {self._gpu_hits} hits"
                    )

        self._sync_thread = threading.Thread(target=_sync_loop, daemon=True, name="uaimc-gpu-sync")
        self._sync_thread.start()
        logger.info(f"GPU-AM: Sync daemon started (interval={SYNC_INTERVAL}s)")

    def stats(self) -> dict:
        """Return GPU-AM statistics."""
        mem_mb = (self._count * BYTES_PER_ENTRY) / (1024 * 1024) if self._count else 0

        result = {
            "enabled": self.enabled,
            "device": self.device_name,
            "entries": self._count,
            "capacity": self._capacity,
            "fill_pct": round(self._count / self._capacity * 100, 2) if self._capacity else 0,
            "memory_mb": round(mem_mb, 2),
            "max_mb": GPU_MAX_MB,
            "queries": self._gpu_queries,
            "hits": self._gpu_hits,
            "load_time_ms": round(self._load_time_ms, 1),
        }

        if self.enabled and _torch and _torch is not False and _device and _device.type == "cuda":
            try:
                free, total = _torch.cuda.mem_get_info()
                result["vram_free_mb"] = round(free / (1024 * 1024))
                result["vram_total_mb"] = round(total / (1024 * 1024))
            except Exception:
                pass

        return result

    def shutdown(self):
        """Stop sync daemon."""
        self._stop_event.set()
        if self._sync_thread:
            self._sync_thread.join(timeout=5)
        logger.info("GPU-AM: Shutdown complete")


# ── Phase B4.4: GPU Triad Index ──────────────────────────────────────────────

class GPUTriadIndex:
    """GPU-resident 5-aspect tensor index for parallel triangulation.

    Mirrors AspectInvertedIndex (CPU) but runs vectorized on CUDA tensors.
    Exact triangulate() + fuzzy hamming_triangulate() for semantic similarity.

    VRAM budget: ~3.84MB for 80K docs (trivial on RTX 5070 Ti).
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.enabled = False
        self._summary_ids = None   # int64 tensor, shape (N,)
        self._triads = None        # int64 tensor, shape (N, 5)
        self._lock = threading.Lock()
        self._count = 0

    def load_from_db(self, db: "sqlite3.Connection"):
        """Load aspect_index table into CUDA tensors."""
        if not _init_torch() and (_device is None or _device.type != "cuda"):
            # Still try CPU fallback
            if _torch is False:
                logger.info("GPU-Triad: Disabled (PyTorch not available)")
                return
            # CPU fallback is still useful for vectorized ops
        if _torch is False:
            return

        try:
            rows = db.execute(
                "SELECT summary_id, sem_hash, source_hash, agent_hash, "
                "intent_hash, project_hash FROM aspect_index"
            ).fetchall()

            if not rows:
                logger.info("GPU-Triad: No aspect data to load")
                self.enabled = True  # Enable but empty
                device = _device or _torch.device("cpu")
                self._summary_ids = _torch.tensor([], dtype=_torch.int64, device=device)
                self._triads = _torch.zeros((0, 5), dtype=_torch.int64, device=device)
                return

            device = _device or _torch.device("cpu")
            sids = [r[0] for r in rows]
            triads = [[r[1], r[2], r[3], r[4], r[5]] for r in rows]

            with self._lock:
                self._summary_ids = _torch.tensor(sids, dtype=_torch.int64, device=device)
                self._triads = _torch.tensor(triads, dtype=_torch.int64, device=device)
                self._count = len(rows)
                self.enabled = True

            mem_mb = (self._triads.nelement() * 8 + self._summary_ids.nelement() * 8) / (1024 * 1024)
            logger.info(f"GPU-Triad: Loaded {len(rows):,} documents ({mem_mb:.2f}MB on {device})")
        except Exception as e:
            logger.error(f"GPU-Triad: Load failed: {e}")
            self.enabled = False

    def triangulate(self, query_aspects: dict[str, int],
                    min_convergence: int = 2,
                    max_results: int | None = None) -> list[tuple[int, int]]:
        """GPU-parallel exact triangulation. Returns (summary_id, match_count) pairs."""
        if not self.enabled or self._triads is None or self._count == 0:
            return []

        try:
            query_vec = _torch.tensor(
                [[query_aspects.get("sem_hash", 0), query_aspects.get("source_hash", 0),
                  query_aspects.get("agent_hash", 0), query_aspects.get("intent_hash", 0),
                  query_aspects.get("project_hash", 0)]],
                dtype=_torch.int64, device=self._triads.device
            )

            with self._lock:
                matches_per_doc = (self._triads == query_vec).sum(dim=1)
                mask = matches_per_doc >= min_convergence
                result_sids = self._summary_ids[mask]
                result_counts = matches_per_doc[mask]

            if result_sids.numel() == 0:
                return []

            if max_results and max_results > 0:
                k = min(int(max_results), int(result_sids.numel()))
                top_counts, top_idx = result_counts.topk(k)
                top_sids = result_sids[top_idx]
                return [(top_sids[i].item(), top_counts[i].item()) for i in range(k)]

            sorted_idx = result_counts.argsort(descending=True)
            return [(result_sids[i].item(), result_counts[i].item()) for i in sorted_idx]
        except Exception as e:
            logger.error(f"GPU-Triad triangulate failed: {e}")
            return []

    def hamming_triangulate(self, query_sem_hash: int, exact_aspects: dict[str, int],
                            max_hamming: int = 6,
                            min_convergence: int = 2,
                            max_results: int | None = None) -> list[tuple[int, int]]:
        """Fuzzy semantic + exact categorical triangulation on GPU.

        Computes Hamming distance for sem_hash (fuzzy) and exact match for
        the other 4 categorical aspects. A sem_hash within max_hamming bits
        counts as 1 match toward convergence.
        """
        if not self.enabled or self._triads is None or self._count == 0:
            return []

        try:
            device = self._triads.device

            cat_query = _torch.tensor(
                [[exact_aspects.get("source_hash", 0), exact_aspects.get("agent_hash", 0),
                  exact_aspects.get("intent_hash", 0), exact_aspects.get("project_hash", 0)]],
                dtype=_torch.int64, device=device
            )

            with self._lock:
                # Exact match for categorical aspects (columns 1-4)
                cat_matches = (self._triads[:, 1:] == cat_query).sum(dim=1)

                # Fuzzy match for semantic aspect (column 0) via XOR + popcount
                sem_col = self._triads[:, 0]
                query_sem = _torch.tensor(query_sem_hash, dtype=_torch.int64, device=device)
                xor_result = sem_col ^ query_sem

                # Vectorized popcount: count set bits via bit-parallel iteration
                hamming = _torch.zeros(len(sem_col), dtype=_torch.int32, device=device)
                temp = xor_result.clone()
                for _ in range(64):
                    hamming += (temp & 1).int()
                    temp >>= 1

                sem_match = (hamming <= max_hamming).int()
                total_convergence = cat_matches + sem_match

                mask = total_convergence >= min_convergence
                result_sids = self._summary_ids[mask]
                result_counts = total_convergence[mask]

            if result_sids.numel() == 0:
                return []

            if max_results and max_results > 0:
                k = min(int(max_results), int(result_sids.numel()))
                top_counts, top_idx = result_counts.topk(k)
                top_sids = result_sids[top_idx]
                return [(top_sids[i].item(), top_counts[i].item()) for i in range(k)]

            sorted_idx = result_counts.argsort(descending=True)
            return [(result_sids[i].item(), result_counts[i].item()) for i in sorted_idx]
        except Exception as e:
            logger.error(f"GPU-Triad hamming_triangulate failed: {e}")
            return []

    def add(self, summary_id: int, aspects: dict[str, int]):
        """Add a single document to GPU-Triad index. Called after DB insert."""
        if not self.enabled or _torch is False:
            return

        try:
            device = self._triads.device if self._triads is not None else _device
            new_sid = _torch.tensor([summary_id], dtype=_torch.int64, device=device)
            new_triad = _torch.tensor(
                [[aspects["sem_hash"], aspects["source_hash"], aspects["agent_hash"],
                  aspects["intent_hash"], aspects["project_hash"]]],
                dtype=_torch.int64, device=device
            )
            with self._lock:
                if self._triads is not None and self._triads.numel() > 0:
                    self._summary_ids = _torch.cat([self._summary_ids, new_sid])
                    self._triads = _torch.cat([self._triads, new_triad])
                else:
                    self._summary_ids = new_sid
                    self._triads = new_triad
                self._count += 1
        except Exception as e:
            logger.error(f"GPU-Triad add failed: {e}")

    def remove(self, summary_id: int):
        """Remove a document from GPU-Triad index. Called on delete."""
        if not self.enabled or self._summary_ids is None or self._count == 0:
            return

        try:
            with self._lock:
                mask = self._summary_ids != summary_id
                if mask.all():
                    return  # Not found
                self._summary_ids = self._summary_ids[mask]
                self._triads = self._triads[mask]
                self._count = self._summary_ids.numel()
        except Exception as e:
            logger.error(f"GPU-Triad remove failed: {e}")

    @property
    def count(self) -> int:
        return self._count

    @property
    def memory_bytes(self) -> int:
        if self._triads is None:
            return 0
        return (self._triads.nelement() + self._summary_ids.nelement()) * 8

    def stats(self) -> dict:
        """Return GPU-Triad statistics."""
        mem_mb = self.memory_bytes / (1024 * 1024) if self._count else 0
        return {
            "enabled": self.enabled,
            "device": str(self._triads.device) if self._triads is not None else "none",
            "entries": self._count,
            "memory_mb": round(mem_mb, 4),
        }


# ── Singletons ───────────────────────────────────────────────────────────────
_instance: Optional[GPUAnnotationMap] = None
_triad_instance: Optional[GPUTriadIndex] = None


def get_instance(db_path: str) -> GPUAnnotationMap:
    """Get or create the global GPUAnnotationMap singleton."""
    global _instance
    if _instance is None:
        _instance = GPUAnnotationMap(db_path)
    return _instance


def get_triad_instance(db_path: str) -> GPUTriadIndex:
    """Get or create the global GPUTriadIndex singleton."""
    global _triad_instance
    if _triad_instance is None:
        _triad_instance = GPUTriadIndex(db_path)
    return _triad_instance
