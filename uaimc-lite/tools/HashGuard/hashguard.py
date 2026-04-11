#!/usr/bin/env python3
"""
HashGuard - File Integrity Monitor & Verification Tool

Monitor files for unauthorized changes, verify integrity, detect tampering,
and maintain cryptographic manifests of your directories. HashGuard creates
SHA-256 hash manifests that let you know instantly if any file has been
modified, added, or deleted.

Whether you're verifying downloads, monitoring config files for drift,
ensuring release artifacts are untampered, or auditing directory states
before and after deployments, HashGuard gives you confidence that your
files are exactly what they should be.

Author: ATLAS (Team Brain)
For: Logan Smith / Metaphy LLC
Version: 1.0.0
Date: February 7, 2026
License: MIT
"""

import argparse
import hashlib
import json
import os
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ============== CONSTANTS ==============
VERSION = "1.0.0"
TOOL_NAME = "HashGuard"
DEFAULT_ALGORITHM = "sha256"
SUPPORTED_ALGORITHMS = ["md5", "sha1", "sha256", "sha512"]
MANIFEST_FILENAME = ".hashguard.json"
DEFAULT_BUFFER_SIZE = 65536  # 64KB read buffer
MAX_FILE_SIZE_MB = 8192  # 8GB max per file
DEFAULT_IGNORE_PATTERNS = [
    "__pycache__",
    ".git",
    ".svn",
    ".hg",
    "node_modules",
    ".DS_Store",
    "Thumbs.db",
    "*.pyc",
    "*.pyo",
    ".hashguard.json",
]


# ============== UTILITY FUNCTIONS ==============


def _format_size(size_bytes: int) -> str:
    """Format byte count to human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def _format_timestamp(ts: Optional[float] = None) -> str:
    """Format a timestamp to ISO 8601 string."""
    if ts is None:
        ts = time.time()
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _matches_pattern(filename: str, pattern: str) -> bool:
    """Check if a filename matches a simple glob pattern."""
    if pattern.startswith("*."):
        return filename.endswith(pattern[1:])
    return filename == pattern


def _should_ignore(path: Path, ignore_patterns: List[str]) -> bool:
    """Check if a path should be ignored based on patterns."""
    parts = path.parts
    for pattern in ignore_patterns:
        for part in parts:
            if _matches_pattern(part, pattern):
                return True
    return False


# ============== CORE CLASS ==============


class HashGuard:
    """
    File Integrity Monitor & Verification Tool.

    HashGuard creates, manages, and verifies cryptographic hash manifests
    for directories and files. It can detect modifications, additions,
    deletions, and corruption.

    Example:
        >>> guard = HashGuard()
        >>> manifest = guard.init_manifest(Path("./my_project"))
        >>> print(f"Tracked {manifest['stats']['total_files']} files")
        Tracked 42 files

        >>> result = guard.verify(Path("./my_project"))
        >>> print(f"Status: {'OK' if result['clean'] else 'CHANGED'}")
        Status: OK
    """

    def __init__(
        self,
        algorithm: str = DEFAULT_ALGORITHM,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        ignore_patterns: Optional[List[str]] = None,
    ):
        """
        Initialize HashGuard.

        Args:
            algorithm: Hash algorithm to use (md5, sha1, sha256, sha512)
            buffer_size: Read buffer size in bytes
            ignore_patterns: List of filename/directory patterns to ignore

        Raises:
            ValueError: If algorithm is not supported
        """
        algorithm = algorithm.lower()
        if algorithm not in SUPPORTED_ALGORITHMS:
            raise ValueError(
                f"Unsupported algorithm: {algorithm}. "
                f"Supported: {', '.join(SUPPORTED_ALGORITHMS)}"
            )
        self.algorithm = algorithm
        self.buffer_size = buffer_size
        self.ignore_patterns = ignore_patterns if ignore_patterns is not None else list(DEFAULT_IGNORE_PATTERNS)

    def hash_file(self, filepath: Path) -> str:
        """
        Compute hash of a single file.

        Args:
            filepath: Path to the file to hash

        Returns:
            Hex digest string of the file hash

        Raises:
            FileNotFoundError: If file does not exist
            ValueError: If path is not a file or file is too large
            PermissionError: If file cannot be read
        """
        filepath = Path(filepath)

        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        if not filepath.is_file():
            raise ValueError(f"Path is not a file: {filepath}")

        size_mb = filepath.stat().st_size / (1024 * 1024)
        if size_mb > MAX_FILE_SIZE_MB:
            raise ValueError(
                f"File too large: {size_mb:.1f} MB (max {MAX_FILE_SIZE_MB} MB)"
            )

        h = hashlib.new(self.algorithm)
        try:
            with open(filepath, "rb") as f:
                while True:
                    chunk = f.read(self.buffer_size)
                    if not chunk:
                        break
                    h.update(chunk)
        except PermissionError:
            raise PermissionError(f"Cannot read file: {filepath}")

        return h.hexdigest()

    def hash_data(self, data: bytes) -> str:
        """
        Compute hash of raw bytes.

        Args:
            data: Bytes to hash

        Returns:
            Hex digest string

        Raises:
            TypeError: If data is not bytes
        """
        if not isinstance(data, bytes):
            raise TypeError(f"Expected bytes, got {type(data).__name__}")

        h = hashlib.new(self.algorithm)
        h.update(data)
        return h.hexdigest()

    def scan_directory(
        self,
        directory: Path,
        include_hidden: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Scan a directory and compute hashes for all files.

        Args:
            directory: Path to directory to scan
            include_hidden: Whether to include hidden files/dirs

        Returns:
            Dictionary mapping relative paths to file info dicts

        Raises:
            FileNotFoundError: If directory does not exist
            ValueError: If path is not a directory
        """
        directory = Path(directory).resolve()

        if not directory.exists():
            raise FileNotFoundError(f"Directory not found: {directory}")

        if not directory.is_dir():
            raise ValueError(f"Path is not a directory: {directory}")

        files: Dict[str, Dict[str, Any]] = {}
        errors: List[str] = []

        for root, dirs, filenames in os.walk(directory):
            root_path = Path(root)
            rel_root = root_path.relative_to(directory)

            # Filter ignored directories in-place
            if not include_hidden:
                dirs[:] = [
                    d for d in dirs
                    if not d.startswith(".")
                    and not _should_ignore(rel_root / d, self.ignore_patterns)
                ]
            else:
                dirs[:] = [
                    d for d in dirs
                    if not _should_ignore(rel_root / d, self.ignore_patterns)
                ]

            for filename in filenames:
                if not include_hidden and filename.startswith("."):
                    if filename != MANIFEST_FILENAME:
                        continue

                filepath = root_path / filename
                rel_path = filepath.relative_to(directory)

                if _should_ignore(rel_path, self.ignore_patterns):
                    continue

                # Use forward slashes for cross-platform consistency
                rel_key = str(rel_path).replace("\\", "/")

                try:
                    stat = filepath.stat()
                    file_hash = self.hash_file(filepath)
                    files[rel_key] = {
                        "hash": file_hash,
                        "size": stat.st_size,
                        "modified": stat.st_mtime,
                    }
                except (PermissionError, ValueError, OSError) as e:
                    errors.append(f"{rel_key}: {e}")

        if errors:
            files["__errors__"] = {"messages": errors}

        return files

    def init_manifest(
        self,
        directory: Path,
        include_hidden: bool = False,
        save: bool = True,
    ) -> Dict[str, Any]:
        """
        Create a new hash manifest for a directory.

        Args:
            directory: Path to directory to create manifest for
            include_hidden: Whether to include hidden files
            save: Whether to save the manifest to disk

        Returns:
            Complete manifest dictionary

        Raises:
            FileNotFoundError: If directory does not exist
            ValueError: If path is not a directory
        """
        directory = Path(directory).resolve()
        files = self.scan_directory(directory, include_hidden)

        # Extract errors if any
        errors = []
        if "__errors__" in files:
            errors = files.pop("__errors__")["messages"]

        total_size = sum(info["size"] for info in files.values())

        manifest = {
            "hashguard_version": VERSION,
            "algorithm": self.algorithm,
            "created": _format_timestamp(),
            "directory": str(directory),
            "platform": platform.system(),
            "include_hidden": include_hidden,
            "ignore_patterns": self.ignore_patterns,
            "stats": {
                "total_files": len(files),
                "total_size": total_size,
                "total_size_human": _format_size(total_size),
                "scan_errors": len(errors),
            },
            "files": files,
        }

        if errors:
            manifest["errors"] = errors

        if save:
            manifest_path = directory / MANIFEST_FILENAME
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2, sort_keys=False)

        return manifest

    def verify(
        self,
        directory: Path,
        manifest_path: Optional[Path] = None,
    ) -> Dict[str, Any]:
        """
        Verify directory integrity against a saved manifest.

        Args:
            directory: Path to directory to verify
            manifest_path: Path to manifest file (default: .hashguard.json in dir)

        Returns:
            Verification result dictionary with changes detected

        Raises:
            FileNotFoundError: If directory or manifest not found
        """
        directory = Path(directory).resolve()

        if not directory.exists():
            raise FileNotFoundError(f"Directory not found: {directory}")

        if manifest_path is None:
            manifest_path = directory / MANIFEST_FILENAME

        manifest_path = Path(manifest_path)
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"Manifest not found: {manifest_path}. "
                f"Run 'hashguard init' first."
            )

        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        # Verify manifest structure
        if "files" not in manifest or "algorithm" not in manifest:
            raise ValueError("Invalid manifest format")

        # Use manifest's algorithm for verification
        old_algo = self.algorithm
        self.algorithm = manifest["algorithm"]

        try:
            current_files = self.scan_directory(
                directory,
                include_hidden=manifest.get("include_hidden", False),
            )
        finally:
            self.algorithm = old_algo

        # Remove error entries for comparison
        current_errors = []
        if "__errors__" in current_files:
            current_errors = current_files.pop("__errors__")["messages"]

        saved_files = manifest["files"]

        # Find changes
        modified: List[Dict[str, Any]] = []
        added: List[str] = []
        deleted: List[str] = []
        unchanged: List[str] = []

        saved_keys: Set[str] = set(saved_files.keys())
        current_keys: Set[str] = set(current_files.keys())

        # Added files
        for key in sorted(current_keys - saved_keys):
            added.append(key)

        # Deleted files
        for key in sorted(saved_keys - current_keys):
            deleted.append(key)

        # Check existing files for modifications
        for key in sorted(saved_keys & current_keys):
            old_hash = saved_files[key]["hash"]
            new_hash = current_files[key]["hash"]
            if old_hash != new_hash:
                modified.append({
                    "file": key,
                    "old_hash": old_hash,
                    "new_hash": new_hash,
                    "old_size": saved_files[key]["size"],
                    "new_size": current_files[key]["size"],
                })
            else:
                unchanged.append(key)

        is_clean = len(modified) == 0 and len(added) == 0 and len(deleted) == 0

        result = {
            "clean": is_clean,
            "verified_at": _format_timestamp(),
            "manifest_created": manifest.get("created", "unknown"),
            "algorithm": manifest["algorithm"],
            "directory": str(directory),
            "summary": {
                "total_checked": len(saved_keys | current_keys),
                "unchanged": len(unchanged),
                "modified": len(modified),
                "added": len(added),
                "deleted": len(deleted),
                "errors": len(current_errors),
            },
            "modified": modified,
            "added": added,
            "deleted": deleted,
        }

        if current_errors:
            result["errors"] = current_errors

        return result

    def check_file(self, filepath: Path, expected_hash: str) -> Dict[str, Any]:
        """
        Verify a single file against an expected hash.

        Args:
            filepath: Path to the file
            expected_hash: Expected hash hex string

        Returns:
            Verification result dictionary

        Raises:
            FileNotFoundError: If file does not exist
        """
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        actual_hash = self.hash_file(filepath)
        matches = actual_hash.lower() == expected_hash.lower()
        size = filepath.stat().st_size

        return {
            "file": str(filepath),
            "algorithm": self.algorithm,
            "expected": expected_hash.lower(),
            "actual": actual_hash,
            "match": matches,
            "size": size,
            "size_human": _format_size(size),
            "verified_at": _format_timestamp(),
        }

    def compare_directories(
        self,
        dir1: Path,
        dir2: Path,
        include_hidden: bool = False,
    ) -> Dict[str, Any]:
        """
        Compare two directories and find differences.

        Args:
            dir1: First directory path
            dir2: Second directory path
            include_hidden: Whether to include hidden files

        Returns:
            Comparison result dictionary

        Raises:
            FileNotFoundError: If either directory not found
        """
        dir1 = Path(dir1).resolve()
        dir2 = Path(dir2).resolve()

        if not dir1.exists():
            raise FileNotFoundError(f"Directory not found: {dir1}")
        if not dir2.exists():
            raise FileNotFoundError(f"Directory not found: {dir2}")

        files1 = self.scan_directory(dir1, include_hidden)
        files2 = self.scan_directory(dir2, include_hidden)

        # Remove errors for comparison
        errors1 = files1.pop("__errors__", {}).get("messages", [])
        errors2 = files2.pop("__errors__", {}).get("messages", [])

        keys1: Set[str] = set(files1.keys())
        keys2: Set[str] = set(files2.keys())

        only_in_1: List[str] = sorted(keys1 - keys2)
        only_in_2: List[str] = sorted(keys2 - keys1)
        in_both: Set[str] = keys1 & keys2

        identical: List[str] = []
        different: List[Dict[str, Any]] = []

        for key in sorted(in_both):
            if files1[key]["hash"] == files2[key]["hash"]:
                identical.append(key)
            else:
                different.append({
                    "file": key,
                    "hash_1": files1[key]["hash"],
                    "hash_2": files2[key]["hash"],
                    "size_1": files1[key]["size"],
                    "size_2": files2[key]["size"],
                })

        total_1 = sum(info["size"] for info in files1.values())
        total_2 = sum(info["size"] for info in files2.values())

        return {
            "directory_1": str(dir1),
            "directory_2": str(dir2),
            "algorithm": self.algorithm,
            "compared_at": _format_timestamp(),
            "summary": {
                "files_in_1": len(keys1),
                "files_in_2": len(keys2),
                "identical": len(identical),
                "different": len(different),
                "only_in_1": len(only_in_1),
                "only_in_2": len(only_in_2),
                "total_size_1": _format_size(total_1),
                "total_size_2": _format_size(total_2),
            },
            "identical_files": identical,
            "different_files": different,
            "only_in_1": only_in_1,
            "only_in_2": only_in_2,
        }

    def diff_manifests(
        self,
        manifest1_path: Path,
        manifest2_path: Path,
    ) -> Dict[str, Any]:
        """
        Compare two manifest files and show differences.

        Args:
            manifest1_path: Path to first manifest
            manifest2_path: Path to second manifest

        Returns:
            Diff result dictionary

        Raises:
            FileNotFoundError: If either manifest not found
        """
        manifest1_path = Path(manifest1_path)
        manifest2_path = Path(manifest2_path)

        if not manifest1_path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest1_path}")
        if not manifest2_path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest2_path}")

        with open(manifest1_path, "r", encoding="utf-8") as f:
            m1 = json.load(f)
        with open(manifest2_path, "r", encoding="utf-8") as f:
            m2 = json.load(f)

        files1 = m1.get("files", {})
        files2 = m2.get("files", {})

        keys1: Set[str] = set(files1.keys())
        keys2: Set[str] = set(files2.keys())

        added: List[str] = sorted(keys2 - keys1)
        deleted: List[str] = sorted(keys1 - keys2)
        modified: List[Dict[str, Any]] = []
        unchanged: List[str] = []

        for key in sorted(keys1 & keys2):
            if files1[key]["hash"] != files2[key]["hash"]:
                modified.append({
                    "file": key,
                    "old_hash": files1[key]["hash"],
                    "new_hash": files2[key]["hash"],
                    "old_size": files1[key]["size"],
                    "new_size": files2[key]["size"],
                })
            else:
                unchanged.append(key)

        return {
            "manifest_1": str(manifest1_path),
            "manifest_2": str(manifest2_path),
            "manifest_1_created": m1.get("created", "unknown"),
            "manifest_2_created": m2.get("created", "unknown"),
            "algorithm_1": m1.get("algorithm", "unknown"),
            "algorithm_2": m2.get("algorithm", "unknown"),
            "diffed_at": _format_timestamp(),
            "summary": {
                "unchanged": len(unchanged),
                "modified": len(modified),
                "added": len(added),
                "deleted": len(deleted),
            },
            "modified": modified,
            "added": added,
            "deleted": deleted,
        }

    def watch(
        self,
        directory: Path,
        interval: float = 5.0,
        callback: Optional[Any] = None,
    ) -> None:
        """
        Monitor a directory for changes by polling.

        Args:
            directory: Path to directory to watch
            interval: Polling interval in seconds
            callback: Optional callback function(changes_dict)

        Raises:
            FileNotFoundError: If directory not found
            KeyboardInterrupt: When user stops monitoring
        """
        directory = Path(directory).resolve()

        if not directory.exists():
            raise FileNotFoundError(f"Directory not found: {directory}")

        manifest_path = directory / MANIFEST_FILENAME
        if not manifest_path.exists():
            print(f"[!] No manifest found. Creating initial manifest...")
            self.init_manifest(directory)
            print(f"[OK] Manifest created. Watching for changes...")
        else:
            print(f"[OK] Manifest loaded. Watching for changes...")

        print(f"[*] Monitoring: {directory}")
        print(f"[*] Interval: {interval}s")
        print(f"[*] Press Ctrl+C to stop")
        print()

        try:
            while True:
                time.sleep(interval)
                result = self.verify(directory)

                if not result["clean"]:
                    summary = result["summary"]
                    print(
                        f"[!] CHANGES DETECTED at {result['verified_at']}: "
                        f"{summary['modified']} modified, "
                        f"{summary['added']} added, "
                        f"{summary['deleted']} deleted"
                    )

                    if result["modified"]:
                        for m in result["modified"]:
                            print(f"    [MOD] {m['file']}")
                    if result["added"]:
                        for a in result["added"]:
                            print(f"    [ADD] {a}")
                    if result["deleted"]:
                        for d in result["deleted"]:
                            print(f"    [DEL] {d}")

                    print()

                    if callback:
                        callback(result)

                    # Update manifest after reporting changes
                    self.init_manifest(directory)

        except KeyboardInterrupt:
            print("\n[OK] Watch stopped.")

    def generate_report(
        self,
        directory: Path,
        output_format: str = "text",
    ) -> str:
        """
        Generate a comprehensive integrity report.

        Args:
            directory: Path to directory to report on
            output_format: Output format ('text', 'json', 'markdown')

        Returns:
            Formatted report string

        Raises:
            FileNotFoundError: If directory not found
            ValueError: If format is not supported
        """
        directory = Path(directory).resolve()

        if output_format not in ("text", "json", "markdown"):
            raise ValueError(
                f"Unsupported format: {output_format}. "
                f"Use 'text', 'json', or 'markdown'."
            )

        # Try to verify against existing manifest
        manifest_path = directory / MANIFEST_FILENAME
        has_manifest = manifest_path.exists()

        if has_manifest:
            verification = self.verify(directory)
        else:
            verification = None

        # Scan current state
        files = self.scan_directory(directory)
        errors = files.pop("__errors__", {}).get("messages", [])

        total_size = sum(info["size"] for info in files.values())

        # Organize by extension
        by_extension: Dict[str, List[str]] = {}
        for filepath in files:
            ext = Path(filepath).suffix.lower() or "(no extension)"
            by_extension.setdefault(ext, []).append(filepath)

        if output_format == "json":
            report_data = {
                "report": "HashGuard Integrity Report",
                "generated_at": _format_timestamp(),
                "directory": str(directory),
                "algorithm": self.algorithm,
                "has_manifest": has_manifest,
                "stats": {
                    "total_files": len(files),
                    "total_size": total_size,
                    "total_size_human": _format_size(total_size),
                    "file_types": len(by_extension),
                    "errors": len(errors),
                },
                "file_types": {
                    ext: len(paths) for ext, paths in sorted(by_extension.items())
                },
                "verification": verification,
                "files": files,
            }
            return json.dumps(report_data, indent=2)

        elif output_format == "markdown":
            lines = [
                f"# HashGuard Integrity Report",
                f"",
                f"**Directory:** `{directory}`  ",
                f"**Generated:** {_format_timestamp()}  ",
                f"**Algorithm:** {self.algorithm}  ",
                f"**Manifest:** {'Yes' if has_manifest else 'No (run hashguard init)'}  ",
                f"",
                f"## Summary",
                f"",
                f"| Metric | Value |",
                f"|--------|-------|",
                f"| Total Files | {len(files)} |",
                f"| Total Size | {_format_size(total_size)} |",
                f"| File Types | {len(by_extension)} |",
                f"| Scan Errors | {len(errors)} |",
                f"",
            ]

            if verification:
                s = verification["summary"]
                status = "[OK] Clean" if verification["clean"] else "[!] Changes Detected"
                lines.extend([
                    f"## Verification Status: {status}",
                    f"",
                    f"| Check | Count |",
                    f"|-------|-------|",
                    f"| Unchanged | {s['unchanged']} |",
                    f"| Modified | {s['modified']} |",
                    f"| Added | {s['added']} |",
                    f"| Deleted | {s['deleted']} |",
                    f"",
                ])

                if verification["modified"]:
                    lines.append("### Modified Files")
                    lines.append("")
                    for m in verification["modified"]:
                        lines.append(f"- `{m['file']}` ({_format_size(m['old_size'])} -> {_format_size(m['new_size'])})")
                    lines.append("")

                if verification["added"]:
                    lines.append("### Added Files")
                    lines.append("")
                    for a in verification["added"]:
                        lines.append(f"- `{a}`")
                    lines.append("")

                if verification["deleted"]:
                    lines.append("### Deleted Files")
                    lines.append("")
                    for d in verification["deleted"]:
                        lines.append(f"- `{d}`")
                    lines.append("")

            lines.extend([
                f"## File Types",
                f"",
                f"| Extension | Count |",
                f"|-----------|-------|",
            ])
            for ext, paths in sorted(by_extension.items(), key=lambda x: -len(x[1])):
                lines.append(f"| {ext} | {len(paths)} |")
            lines.append("")

            lines.extend([
                f"## All Files",
                f"",
                f"| File | Hash | Size |",
                f"|------|------|------|",
            ])
            for filepath in sorted(files.keys()):
                info = files[filepath]
                short_hash = info["hash"][:16] + "..."
                lines.append(f"| `{filepath}` | `{short_hash}` | {_format_size(info['size'])} |")
            lines.append("")

            lines.append(f"---")
            lines.append(f"*Generated by HashGuard v{VERSION}*")

            return "\n".join(lines)

        else:  # text
            separator = "=" * 70
            lines = [
                separator,
                f"  HASHGUARD INTEGRITY REPORT",
                separator,
                f"",
                f"  Directory:  {directory}",
                f"  Generated:  {_format_timestamp()}",
                f"  Algorithm:  {self.algorithm}",
                f"  Manifest:   {'Yes' if has_manifest else 'No (run hashguard init)'}",
                f"",
                f"  SUMMARY",
                f"  {'-' * 40}",
                f"  Total Files:  {len(files)}",
                f"  Total Size:   {_format_size(total_size)}",
                f"  File Types:   {len(by_extension)}",
                f"  Scan Errors:  {len(errors)}",
                f"",
            ]

            if verification:
                s = verification["summary"]
                status = "[OK] Clean" if verification["clean"] else "[!] CHANGES DETECTED"
                lines.extend([
                    f"  VERIFICATION: {status}",
                    f"  {'-' * 40}",
                    f"  Unchanged:  {s['unchanged']}",
                    f"  Modified:   {s['modified']}",
                    f"  Added:      {s['added']}",
                    f"  Deleted:    {s['deleted']}",
                    f"",
                ])

                if verification["modified"]:
                    lines.append("  MODIFIED FILES:")
                    for m in verification["modified"]:
                        lines.append(f"    [MOD] {m['file']}")
                    lines.append("")

                if verification["added"]:
                    lines.append("  ADDED FILES:")
                    for a in verification["added"]:
                        lines.append(f"    [ADD] {a}")
                    lines.append("")

                if verification["deleted"]:
                    lines.append("  DELETED FILES:")
                    for d in verification["deleted"]:
                        lines.append(f"    [DEL] {d}")
                    lines.append("")

            lines.extend([
                f"  FILE TYPES:",
                f"  {'-' * 40}",
            ])
            for ext, paths in sorted(by_extension.items(), key=lambda x: -len(x[1])):
                lines.append(f"    {ext:20s} {len(paths):5d} files")
            lines.append("")

            lines.extend([
                f"  ALL FILES:",
                f"  {'-' * 40}",
            ])
            for filepath in sorted(files.keys()):
                info = files[filepath]
                short_hash = info["hash"][:12]
                lines.append(
                    f"    {short_hash}  {_format_size(info['size']):>10s}  {filepath}"
                )
            lines.append("")

            lines.append(separator)
            lines.append(f"  Generated by HashGuard v{VERSION}")
            lines.append(separator)

            return "\n".join(lines)


# ============== CLI INTERFACE ==============


def _print_verify_result(result: Dict[str, Any]) -> None:
    """Print verification results to console."""
    summary = result["summary"]

    if result["clean"]:
        print(f"[OK] Directory is CLEAN - all {summary['unchanged']} files match manifest")
    else:
        print(f"[!] INTEGRITY CHANGES DETECTED")
        print()
        print(f"    Unchanged: {summary['unchanged']}")
        print(f"    Modified:  {summary['modified']}")
        print(f"    Added:     {summary['added']}")
        print(f"    Deleted:   {summary['deleted']}")
        print()

        if result["modified"]:
            print("  Modified files:")
            for m in result["modified"]:
                old_s = _format_size(m["old_size"])
                new_s = _format_size(m["new_size"])
                print(f"    [MOD] {m['file']} ({old_s} -> {new_s})")
            print()

        if result["added"]:
            print("  New files:")
            for a in result["added"]:
                print(f"    [ADD] {a}")
            print()

        if result["deleted"]:
            print("  Deleted files:")
            for d in result["deleted"]:
                print(f"    [DEL] {d}")
            print()


def main():
    """CLI entry point for HashGuard."""
    # Fix Windows console encoding
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    parser = argparse.ArgumentParser(
        prog="hashguard",
        description="HashGuard - File Integrity Monitor & Verification Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  hashguard init ./project              Create manifest for directory
  hashguard verify ./project            Check integrity against manifest
  hashguard hash myfile.zip             Get hash of a single file
  hashguard check file.zip abc123...    Verify file matches expected hash
  hashguard compare dir1/ dir2/         Compare two directories
  hashguard diff m1.json m2.json        Compare two manifests
  hashguard watch ./project             Monitor for real-time changes
  hashguard report ./project            Generate integrity report

For more info: https://github.com/DonkRonk17/HashGuard
        """,
    )

    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {VERSION}"
    )
    parser.add_argument(
        "--algorithm", "-a",
        choices=SUPPORTED_ALGORITHMS,
        default=DEFAULT_ALGORITHM,
        help=f"Hash algorithm (default: {DEFAULT_ALGORITHM})",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output results as JSON",
    )
    parser.add_argument(
        "--hidden", action="store_true",
        help="Include hidden files and directories",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # init command
    init_parser = subparsers.add_parser(
        "init", help="Create hash manifest for a directory"
    )
    init_parser.add_argument("directory", help="Directory to create manifest for")

    # verify command
    verify_parser = subparsers.add_parser(
        "verify", help="Verify directory integrity against manifest"
    )
    verify_parser.add_argument("directory", help="Directory to verify")
    verify_parser.add_argument(
        "--manifest", "-m", help="Path to manifest file (default: .hashguard.json)"
    )

    # hash command
    hash_parser = subparsers.add_parser("hash", help="Compute hash of a file")
    hash_parser.add_argument("file", help="File to hash")

    # check command
    check_parser = subparsers.add_parser(
        "check", help="Verify file matches expected hash"
    )
    check_parser.add_argument("file", help="File to verify")
    check_parser.add_argument("expected_hash", help="Expected hash value")

    # compare command
    compare_parser = subparsers.add_parser(
        "compare", help="Compare two directories"
    )
    compare_parser.add_argument("dir1", help="First directory")
    compare_parser.add_argument("dir2", help="Second directory")

    # diff command
    diff_parser = subparsers.add_parser(
        "diff", help="Compare two manifest files"
    )
    diff_parser.add_argument("manifest1", help="First manifest file")
    diff_parser.add_argument("manifest2", help="Second manifest file")

    # watch command
    watch_parser = subparsers.add_parser(
        "watch", help="Monitor directory for changes"
    )
    watch_parser.add_argument("directory", help="Directory to monitor")
    watch_parser.add_argument(
        "--interval", "-i",
        type=float, default=5.0,
        help="Polling interval in seconds (default: 5)",
    )

    # report command
    report_parser = subparsers.add_parser(
        "report", help="Generate integrity report"
    )
    report_parser.add_argument("directory", help="Directory to report on")
    report_parser.add_argument(
        "--format", "-f",
        choices=["text", "json", "markdown"],
        default="text",
        help="Report format (default: text)",
    )
    report_parser.add_argument(
        "--output", "-o",
        help="Output file (default: stdout)",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    guard = HashGuard(
        algorithm=args.algorithm,
        ignore_patterns=list(DEFAULT_IGNORE_PATTERNS),
    )

    try:
        if args.command == "init":
            directory = Path(args.directory)
            manifest = guard.init_manifest(directory, include_hidden=args.hidden)
            stats = manifest["stats"]

            if args.json:
                print(json.dumps(manifest, indent=2))
            else:
                print(f"[OK] Manifest created: {directory / MANIFEST_FILENAME}")
                print(f"     Files tracked: {stats['total_files']}")
                print(f"     Total size: {stats['total_size_human']}")
                print(f"     Algorithm: {guard.algorithm}")
                if stats["scan_errors"] > 0:
                    print(f"     [!] Scan errors: {stats['scan_errors']}")

        elif args.command == "verify":
            directory = Path(args.directory)
            manifest_path = Path(args.manifest) if args.manifest else None
            result = guard.verify(directory, manifest_path)

            if args.json:
                print(json.dumps(result, indent=2))
            else:
                _print_verify_result(result)

            # Return non-zero exit code if changes detected
            if not result["clean"]:
                return 1

        elif args.command == "hash":
            filepath = Path(args.file)
            file_hash = guard.hash_file(filepath)
            size = filepath.stat().st_size

            if args.json:
                print(json.dumps({
                    "file": str(filepath),
                    "algorithm": guard.algorithm,
                    "hash": file_hash,
                    "size": size,
                    "size_human": _format_size(size),
                }, indent=2))
            else:
                print(f"{file_hash}  {filepath}")

        elif args.command == "check":
            filepath = Path(args.file)
            result = guard.check_file(filepath, args.expected_hash)

            if args.json:
                print(json.dumps(result, indent=2))
            else:
                if result["match"]:
                    print(f"[OK] Hash matches: {filepath}")
                    print(f"     {guard.algorithm}: {result['actual']}")
                else:
                    print(f"[X] Hash MISMATCH: {filepath}")
                    print(f"     Expected: {result['expected']}")
                    print(f"     Actual:   {result['actual']}")
                    return 1

        elif args.command == "compare":
            dir1 = Path(args.dir1)
            dir2 = Path(args.dir2)
            result = guard.compare_directories(dir1, dir2, include_hidden=args.hidden)

            if args.json:
                print(json.dumps(result, indent=2))
            else:
                s = result["summary"]
                print(f"Directory Comparison")
                print(f"  Dir 1: {dir1} ({s['files_in_1']} files, {s['total_size_1']})")
                print(f"  Dir 2: {dir2} ({s['files_in_2']} files, {s['total_size_2']})")
                print()
                print(f"  Identical: {s['identical']}")
                print(f"  Different: {s['different']}")
                print(f"  Only in 1: {s['only_in_1']}")
                print(f"  Only in 2: {s['only_in_2']}")

                if result["different_files"]:
                    print()
                    print("  Different files:")
                    for d in result["different_files"]:
                        print(f"    [DIFF] {d['file']}")

                if result["only_in_1"]:
                    print()
                    print(f"  Only in {dir1.name}/:")
                    for f in result["only_in_1"]:
                        print(f"    {f}")

                if result["only_in_2"]:
                    print()
                    print(f"  Only in {dir2.name}/:")
                    for f in result["only_in_2"]:
                        print(f"    {f}")

                if s["identical"] == s["files_in_1"] == s["files_in_2"]:
                    print()
                    print("[OK] Directories are identical")

        elif args.command == "diff":
            m1 = Path(args.manifest1)
            m2 = Path(args.manifest2)
            result = guard.diff_manifests(m1, m2)

            if args.json:
                print(json.dumps(result, indent=2))
            else:
                s = result["summary"]
                print(f"Manifest Diff")
                print(f"  Manifest 1: {m1} (created: {result['manifest_1_created']})")
                print(f"  Manifest 2: {m2} (created: {result['manifest_2_created']})")
                print()
                print(f"  Unchanged: {s['unchanged']}")
                print(f"  Modified:  {s['modified']}")
                print(f"  Added:     {s['added']}")
                print(f"  Deleted:   {s['deleted']}")

                if result["modified"]:
                    print()
                    print("  Modified files:")
                    for m in result["modified"]:
                        print(f"    [MOD] {m['file']}")

                if result["added"]:
                    print()
                    print("  Added files:")
                    for a in result["added"]:
                        print(f"    [ADD] {a}")

                if result["deleted"]:
                    print()
                    print("  Deleted files:")
                    for d in result["deleted"]:
                        print(f"    [DEL] {d}")

        elif args.command == "watch":
            directory = Path(args.directory)
            guard.watch(directory, interval=args.interval)

        elif args.command == "report":
            directory = Path(args.directory)
            fmt = args.format
            report = guard.generate_report(directory, output_format=fmt)

            if args.output:
                output_path = Path(args.output)
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(report)
                print(f"[OK] Report saved to: {output_path}")
            else:
                print(report)

        return 0

    except FileNotFoundError as e:
        print(f"[X] Error: {e}")
        return 1
    except ValueError as e:
        print(f"[X] Error: {e}")
        return 1
    except PermissionError as e:
        print(f"[X] Permission denied: {e}")
        return 1
    except KeyboardInterrupt:
        print("\n[OK] Interrupted.")
        return 0
    except Exception as e:
        print(f"[X] Unexpected error: {e}")
        print(f"[*] Please report this issue with the error message above")
        return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
