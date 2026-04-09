r"""
UAIMC File Watcher — Auto-Ingest Module (Beta)
=================================================
Watches configured directories for new/modified files and auto-ingests
them into UAIMC's 3-tier memory pipeline (verbatim → summary → annotations).

Beta features:
  - Processed file tracking (survives restarts without re-ingestion)
  - Retry queue for failed ingestions (UAIMC down/locked)
  - Polling fallback when OS-level events unavailable

PRIVATE -- Not for publication
COPILOT_VSCODE (Team Brain) | March 14, 2026
"""

import hashlib
import json
import logging
import os
import platform
import re
import threading
import time
from pathlib import Path
from typing import Callable

logger = logging.getLogger("uaimc.watcher")

# ── Imports with polling fallback ────────────────────────────────────────────
try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer

    _WATCHDOG_AVAILABLE = True
except ImportError:
    _WATCHDOG_AVAILABLE = False
    Observer = None  # type: ignore[assignment,misc]

try:
    from watchdog.observers.polling import PollingObserver
except ImportError:
    PollingObserver = None  # type: ignore[assignment,misc]


# Agent name pattern for filename-based author detection
_AGENT_RE = re.compile(
    r"^(CLIO|FORGE|ATLAS|IRIS|PORTER|BOLT|SONNET|NEXUS|OPUS|COPILOT|AEGIS|GROK|GEMINI|LAIA|AXIOM|CODEX|VISOR|LOGAN)",
    re.IGNORECASE,
)

MAX_FILE_BYTES = 5 * 1024 * 1024  # 5 MB — store() handles chunking for large content
MAX_PENDING_QUEUE = 100
DRAIN_INTERVAL = 30  # seconds between retry queue drain attempts


def _content_hash(text: str) -> str:
    """Fast content hash for tracking processed files."""
    return hashlib.md5(text.strip().lower().encode("utf-8")).hexdigest()


# ── DebouncedHandler ─────────────────────────────────────────────────────────
if _WATCHDOG_AVAILABLE:

    class DebouncedHandler(FileSystemEventHandler):
        """Debounces rapid file write events and dispatches to a callback."""

        def __init__(self, callback: Callable[[Path], None], debounce_seconds: float = 3.0,
                     patterns: tuple[str, ...] = ("*.md", "*.json", "*.txt"),
                     ignore_patterns: tuple[str, ...] = ("*.tmp", "*.swp"),
                     exclude_dirs: tuple[str, ...] = ()):
            super().__init__()
            self._callback = callback
            self._debounce = debounce_seconds
            self._patterns = patterns
            self._ignore_patterns = ignore_patterns
            self._exclude_dirs = tuple(d.lower() for d in exclude_dirs)
            self._timers: dict[str, threading.Timer] = {}
            self._lock = threading.Lock()
            # Buffer overflow tracking (ReadDirectoryChangesW)
            self._overflow_count = 0
            self._last_overflow: float = 0

        def _matches(self, path: str) -> bool:
            # Exclude entire directory trees first (O(1) per event)
            path_lower = path.lower().replace("/", "\\")
            for excl in self._exclude_dirs:
                if f"\\{excl}\\" in path_lower:
                    return False
            name = os.path.basename(path)
            for ip in self._ignore_patterns:
                if self._glob_match(name, ip):
                    return False
            for p in self._patterns:
                if self._glob_match(name, p):
                    return True
            return False

        def on_any_event(self, event):
            """Track buffer overflow events from ReadDirectoryChangesW."""
            if hasattr(event, 'event_type') and event.event_type == 'overflow':
                self._overflow_count += 1
                self._last_overflow = time.time()
                logger.warning(
                    f"Watcher: ReadDirectoryChangesW buffer overflow #{self._overflow_count} "
                    f"— periodic sweep will recover any missed events"
                )
                return
            # Delegate to specific handlers (on_created, on_modified)
            super().on_any_event(event)

        @staticmethod
        def _glob_match(name: str, pattern: str) -> bool:
            if pattern.startswith("*."):
                return name.lower().endswith(pattern[1:].lower())
            if pattern.startswith("."):
                return name.lower().startswith(pattern.lower())
            return name.lower() == pattern.lower()

        def _schedule(self, path: str):
            with self._lock:
                if path in self._timers:
                    self._timers[path].cancel()
                timer = threading.Timer(self._debounce, self._fire, args=(path,))
                timer.daemon = True
                timer.start()
                self._timers[path] = timer

        def _fire(self, path: str):
            with self._lock:
                self._timers.pop(path, None)
            try:
                self._callback(Path(path))
            except Exception as e:
                logger.error(f"Watcher callback failed for {path}: {e}")

        def on_created(self, event):
            if not event.is_directory and self._matches(event.src_path):
                self._schedule(event.src_path)

        def on_modified(self, event):
            if not event.is_directory and self._matches(event.src_path):
                self._schedule(event.src_path)

else:
    # Stub so the class can still be referenced when watchdog is missing
    class DebouncedHandler:  # type: ignore[no-redef]
        def __init__(self, *a, **kw):
            pass


# ── SweepEngine ──────────────────────────────────────────────────────────────

class SweepEngine:
    """Periodic full-tree sweep to catch files missed by native observer."""

    def __init__(self, directories: list[dict], exclude_dirs: tuple[str, ...],
                 file_patterns: tuple[str, ...], callback: Callable[[Path], None],
                 interval_minutes: float = 10.0):
        self._dirs = directories
        self._exclude_dirs = tuple(d.lower() for d in exclude_dirs)
        self._patterns = file_patterns
        self._callback = callback
        self._interval = interval_minutes * 60  # convert to seconds
        self._timer: threading.Timer | None = None
        self._running = False
        self._last_sweep_time: float = 0
        self._last_sweep_files: int = 0
        self._last_sweep_duration: float = 0
        self._total_sweeps: int = 0

    def start(self):
        self._running = True
        self._schedule_next()
        logger.info(f"SweepEngine started (interval={self._interval / 60:.0f}min)")

    def stop(self):
        self._running = False
        if self._timer:
            self._timer.cancel()
            self._timer = None

    def _schedule_next(self):
        if self._running:
            self._timer = threading.Timer(self._interval, self._run_sweep)
            self._timer.daemon = True
            self._timer.start()

    def _run_sweep(self):
        """Walk all directories, submit any unprocessed files to callback."""
        start = time.time()
        files_found = 0

        try:
            for d in self._dirs:
                path = Path(d.get("path", ""))
                if not path.is_dir():
                    continue
                for item in self._walk_filtered(path):
                    try:
                        self._callback(item)
                    except Exception as e:
                        logger.warning(f"SweepEngine: callback failed for {item}: {e}")
                    files_found += 1
        except Exception as e:
            logger.error(f"SweepEngine: sweep aborted after {files_found} files: {e}")

        elapsed = time.time() - start
        self._last_sweep_time = time.time()
        self._last_sweep_files = files_found
        self._last_sweep_duration = elapsed
        self._total_sweeps += 1

        logger.info(f"Sweep #{self._total_sweeps} complete: {files_found} files checked in {elapsed:.1f}s")

        # Schedule next sweep
        self._schedule_next()

    def _walk_filtered(self, root: Path):
        """Walk directory tree, skipping exclude_dirs, yielding matching files."""
        for dirpath, dirnames, filenames in os.walk(root):
            # Filter directories IN PLACE to prevent os.walk from entering them
            dirnames[:] = [
                d for d in dirnames
                if d.lower() not in self._exclude_dirs
            ]

            for fname in filenames:
                if self._matches_pattern(fname):
                    yield Path(dirpath) / fname

    def _matches_pattern(self, name: str) -> bool:
        name_lower = name.lower()
        for p in self._patterns:
            if p.startswith("*.") and name_lower.endswith(p[1:].lower()):
                return True
        return False

    def status(self) -> dict:
        return {
            "enabled": self._running,
            "interval_minutes": self._interval / 60,
            "total_sweeps": self._total_sweeps,
            "last_sweep_time": self._last_sweep_time,
            "last_sweep_files": self._last_sweep_files,
            "last_sweep_duration_sec": round(self._last_sweep_duration, 2),
        }


# ── Metadata detection ───────────────────────────────────────────────────────

def _detect_author(filepath: Path) -> str:
    """Extract agent name from filename (e.g. FORGE_SESSION_2026.md → FORGE)."""
    m = _AGENT_RE.match(filepath.stem)
    return m.group(1).upper() if m else ""


def _detect_source(filepath: Path, dir_configs: list[dict], source_routes: dict | None = None) -> str:
    """Determine source tag from path components, with fallback to watch root."""
    fpath = str(filepath).replace("/", "\\")

    # Layer 1: Check source_routes (deepest match in path wins)
    if source_routes:
        best_tag = None
        best_pos = -1
        for route_key, tag in source_routes.items():
            normalized_key = route_key.replace("/", "\\")
            pos = fpath.find(normalized_key)
            if pos != -1 and pos > best_pos:
                best_pos = pos
                best_tag = tag
        if best_tag:
            return best_tag

    # Layer 2: Fallback to watch root's default source tag
    fpath_lower = fpath.lower()
    for dc in dir_configs:
        dpath = dc["path"].replace("/", "\\").lower()
        if fpath_lower.startswith(dpath):
            return dc.get("source", "file_watcher")

    return "file_watcher"


# ── Persistent state helpers ─────────────────────────────────────────────────

def _load_json(path: Path, default):
    """Load JSON file, returning default if missing or corrupt."""
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Could not load {path.name}: {e}")
    return default


def _save_json_atomic(path: Path, data):
    """Atomic write: write to .tmp then rename to avoid corruption."""
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
        tmp.replace(path)
    except OSError as e:
        logger.error(f"Failed to save {path.name}: {e}")


# ── UAIMCFileWatcher ─────────────────────────────────────────────────────────

class UAIMCFileWatcher:
    """Watches directories and auto-ingests files into UAIMC memory."""

    def __init__(self, config: dict, store_fn: Callable):
        self._config = config
        self._store = store_fn
        self._observer = None
        self._sweep: SweepEngine | None = None
        self._files_processed = 0
        self._errors = 0
        self._last_processed: str = ""
        self._running = False
        self._using_polling = False
        self._source_routes = config.get("source_routes", {})

        # Persistent state paths (next to config.json)
        data_dir = Path(__file__).parent / "data"
        data_dir.mkdir(exist_ok=True)
        self._processed_path = data_dir / "processed_files.json"
        self._pending_path = data_dir / "pending_ingest.json"

        # Load persisted state
        self._processed: dict[str, str] = _load_json(self._processed_path, {})
        self._pending: list[dict] = _load_json(self._pending_path, [])
        self._lock = threading.Lock()
        self._drain_timer: threading.Timer | None = None

        # Dedup Gate Layer 3: per-path ingestion cooldown
        self._last_ingest_time: dict[str, float] = {}
        self._ingest_cooldown = config.get("ingest_cooldown_seconds", 30)

        if self._processed:
            logger.info(f"Watcher: loaded {len(self._processed)} processed file records")
        if self._pending:
            logger.info(f"Watcher: loaded {len(self._pending)} pending retry entries")

    @property
    def stats(self) -> dict:
        handler = getattr(self, "_handler", None)
        return {
            "running": self._running,
            "files_processed": self._files_processed,
            "errors": self._errors,
            "last_processed": self._last_processed,
            "directories_watched": len(self._config.get("directories", [])),
            "queue_depth": len(self._pending),
            "tracked_files": len(self._processed),
            "using_polling": self._using_polling,
            "overflow_count": handler._overflow_count if handler else 0,
            "last_overflow": handler._last_overflow if handler else 0,
            "exclude_dirs_count": len(self._config.get("exclude_dirs", [])),
            "sweep": self._sweep.status() if self._sweep else {"enabled": False},
        }

    def start(self):
        """Start watching all configured directories."""
        if not self._config.get("enabled", True):
            logger.info("File watcher disabled in config")
            return

        dirs = self._config.get("directories", [])
        if not dirs:
            logger.warning("No directories configured for file watcher")
            return

        if not _WATCHDOG_AVAILABLE:
            logger.warning("Watcher: watchdog not installed — file watcher cannot start")
            return

        debounce = self._config.get("debounce_seconds", 3.0)
        patterns = tuple(self._config.get("file_patterns", ["*.md", "*.json", "*.txt"]))
        ignore = tuple(self._config.get("ignore_patterns", ["*.tmp", "*.swp"]))

        exclude_dirs = tuple(self._config.get("exclude_dirs", []))

        handler = DebouncedHandler(
            callback=self._on_file,
            debounce_seconds=debounce,
            patterns=patterns,
            ignore_patterns=ignore,
            exclude_dirs=exclude_dirs,
        )
        self._handler = handler  # keep ref for stats access

        # Use native Observer (ReadDirectoryChangesW on Windows) by default
        # Fall back to PollingObserver only if native fails or config disables it
        use_native = self._config.get("use_native_observer", True)
        try:
            if use_native:
                self._observer = Observer()
                self._observer.daemon = True
                watched_count = self._schedule_dirs(handler, dirs)
                if watched_count > 0:
                    self._observer.start()
                    self._using_polling = False
                    logger.info(
                        f"UAIMC File Watcher started (native/ReadDirectoryChangesW) "
                        f"— {watched_count} directories, {len(exclude_dirs)} excluded dir patterns"
                    )
            else:
                self._observer = PollingObserver(timeout=2) if PollingObserver else Observer()
                self._observer.daemon = True
                watched_count = self._schedule_dirs(handler, dirs)
                if watched_count > 0:
                    self._observer.start()
                    self._using_polling = PollingObserver is not None
                    mode = "polling" if self._using_polling else "native"
                    logger.info(f"UAIMC File Watcher started ({mode}) — {watched_count} directories")
        except Exception as e:
            logger.warning(f"Watcher: native Observer failed ({e}), falling back to PollingObserver")
            if self._observer:
                try:
                    self._observer.stop()
                except Exception:
                    pass
            if PollingObserver is not None:
                self._observer = PollingObserver(timeout=5)
                self._observer.daemon = True
                watched_count = self._schedule_dirs(handler, dirs)
                if watched_count > 0:
                    self._observer.start()
                    self._using_polling = True
                    logger.info(f"UAIMC File Watcher started (polling/fallback) — {watched_count} directories")
            else:
                logger.error("Watcher: PollingObserver not available, file watcher disabled")
                return

        self._running = True

        # Start periodic sweep engine
        if self._config.get("sweep_enabled", False):
            sweep_interval = self._config.get("sweep_interval_minutes", 10)
            self._sweep = SweepEngine(
                directories=dirs,
                exclude_dirs=tuple(self._config.get("exclude_dirs", [])),
                file_patterns=tuple(patterns),
                callback=self._on_file,
                interval_minutes=sweep_interval,
            )
            self._sweep.start()

        # Drain any pending retries from last session
        if self._pending:
            self._schedule_drain()

    def _schedule_dirs(self, handler, dirs: list[dict]) -> int:
        """Schedule all valid directories with the observer. Returns count."""
        watched = 0
        for d in dirs:
            path = d.get("path", "")
            if not path or not Path(path).is_dir():
                logger.warning(f"Watcher: skipping invalid directory: {path}")
                continue
            recursive = d.get("recursive", False)
            self._observer.schedule(handler, path, recursive=recursive)
            logger.info(f"Watcher: watching {path} (source={d.get('source', '?')}, recursive={recursive})")
            watched += 1
        return watched

    def stop(self):
        """Stop the file watcher and persist state."""
        if self._sweep:
            self._sweep.stop()
        if self._drain_timer:
            self._drain_timer.cancel()
            self._drain_timer = None
        if self._observer and self._running:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._running = False
            self._save_state()
            logger.info(f"UAIMC File Watcher stopped (processed {self._files_processed} files, {len(self._pending)} pending)")

    def _save_state(self):
        """Persist processed files and pending queue to disk."""
        with self._lock:
            _save_json_atomic(self._processed_path, self._processed)
            _save_json_atomic(self._pending_path, self._pending)

    def _schedule_drain(self):
        """Schedule the next retry queue drain."""
        if self._drain_timer:
            self._drain_timer.cancel()
        self._drain_timer = threading.Timer(DRAIN_INTERVAL, self._drain_pending)
        self._drain_timer.daemon = True
        self._drain_timer.start()

    def _drain_pending(self):
        """Retry all pending queue entries."""
        if not self._pending:
            return

        logger.info(f"Watcher: draining {len(self._pending)} pending entries")
        remaining = []
        for entry in list(self._pending):
            filepath = Path(entry.get("filepath", ""))
            if not filepath.exists():
                logger.debug(f"Watcher: pending file gone, dropping: {filepath.name}")
                continue
            try:
                raw = filepath.read_text(encoding="utf-8", errors="replace")
                if not raw.strip():
                    continue
                if len(raw) > MAX_FILE_BYTES:
                    logger.warning(f"Watcher: file too large ({len(raw)} bytes), skipping: {filepath.name}")
                    continue

                result = self._store(
                    text=raw,
                    source=entry.get("source", "file_watcher"),
                    author=entry.get("author", ""),
                    metadata=entry.get("metadata", {}),
                )
                if result > 0:
                    self._files_processed += 1
                    self._last_processed = filepath.name
                    with self._lock:
                        self._processed[str(filepath)] = _content_hash(raw)
                    logger.info(f"Watcher: retry succeeded for {filepath.name} (summary_id={result})")
                elif result == -2:
                    # Already stored (dedup), mark as processed
                    with self._lock:
                        self._processed[str(filepath)] = _content_hash(raw)
                else:
                    remaining.append(entry)
            except Exception as e:
                logger.warning(f"Watcher: retry failed for {filepath.name}: {e}")
                remaining.append(entry)

        with self._lock:
            self._pending = remaining
        self._save_state()

        # Reschedule if items remain
        if self._pending and self._running:
            self._schedule_drain()

    def _on_file(self, filepath: Path):
        """Process a new/modified file."""
        try:
            if not filepath.exists():
                return

            size = filepath.stat().st_size
            if size == 0:
                return
            if size > MAX_FILE_BYTES:
                logger.warning(f"Watcher: file exceeds 5MB ({size} bytes), skipping: {filepath}")
                return

            try:
                raw = filepath.read_text(encoding="utf-8", errors="replace")
            except (OSError, PermissionError) as e:
                logger.warning(f"Watcher: cannot read {filepath}: {e}")
                self._errors += 1
                return

            if not raw.strip():
                return

            # Dedup Gate Layer 3: per-path ingestion cooldown
            fkey = str(filepath)
            now = time.time()
            with self._lock:
                last_time = self._last_ingest_time.get(fkey, 0)
                if now - last_time < self._ingest_cooldown:
                    logger.debug(f"Watcher: cooldown active for {filepath.name} ({now - last_time:.0f}s < {self._ingest_cooldown}s)")
                    return  # Too soon since last ingest of this path

            # Check if already processed with same content
            chash = _content_hash(raw)
            with self._lock:
                if self._processed.get(fkey) == chash:
                    return  # Already processed, content unchanged

            # Detect metadata
            author = _detect_author(filepath)
            source = _detect_source(filepath, self._config.get("directories", []), self._source_routes)
            metadata = {
                "filename": filepath.name,
                "filepath": fkey,
                "ingested_by": "file_watcher",
            }

            # Attempt store
            try:
                result = self._store(
                    text=raw,
                    source=source,
                    author=author,
                    metadata=metadata,
                )
            except Exception as e:
                logger.error(f"Watcher: store() raised for {filepath.name}: {e}")
                self._queue_pending(filepath, source, author, metadata)
                return

            if result == -2:
                # Already in DB (content_hash or summary_hash dedup) — mark processed + record time
                with self._lock:
                    self._processed[fkey] = chash
                    self._last_ingest_time[fkey] = time.time()
                logger.debug(f"Watcher: duplicate skipped: {filepath.name}")
            elif result > 0:
                with self._lock:
                    self._processed[fkey] = chash
                    self._last_ingest_time[fkey] = time.time()
                self._files_processed += 1
                self._last_processed = filepath.name
                logger.info(f"Watcher: ingested {filepath.name} (summary_id={result}, source={source}, author={author or 'unknown'})")
            else:
                logger.error(f"Watcher: store failed for {filepath.name} (code={result})")
                self._queue_pending(filepath, source, author, metadata)
                self._errors += 1

            # Save state after each successful ingest
            self._save_state()

        except Exception as e:
            logger.error(f"Watcher: unexpected error processing {filepath}: {e}")
            self._errors += 1

    def _queue_pending(self, filepath: Path, source: str, author: str, metadata: dict):
        """Add a failed ingestion to the retry queue."""
        with self._lock:
            if len(self._pending) >= MAX_PENDING_QUEUE:
                logger.warning("Watcher: pending queue full, dropping oldest entry")
                self._pending.pop(0)
            self._pending.append({
                "filepath": str(filepath),
                "source": source,
                "author": author,
                "metadata": metadata,
                "queued_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            })
        self._save_state()
        # Start drain cycle if not already running
        if not self._drain_timer or not self._drain_timer.is_alive():
            self._schedule_drain()
        logger.info(f"Watcher: queued for retry: {filepath.name} (queue depth: {len(self._pending)})")
