#!/usr/bin/env python3
"""
SQLiteExplorer - Smart SQLite Database Explorer & Management Tool

Instantly inspect, query, analyze, and export SQLite databases from the
command line or Python. Beautiful formatted output, zero dependencies,
cross-platform. Browse schemas, run queries, get column statistics,
search across tables, export to CSV/JSON/Markdown, compare databases,
and optimize storage -- all with a single tool.

Author: ATLAS (Team Brain)
For: Logan Smith / Metaphy LLC
Version: 1.0.0
Date: February 14, 2026
License: MIT
"""

import argparse
import csv
import io
import json
import os
import shutil
import sqlite3
import sys
import textwrap
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union


__version__ = "1.0.0"
__author__ = "ATLAS (Team Brain)"


# =============================================================================
# TABLE FORMATTER
# =============================================================================

class TableFormatter:
    """Format tabular data for terminal, JSON, CSV, and Markdown output."""

    @staticmethod
    def format_table(
        headers: List[str],
        rows: List[List[Any]],
        max_width: int = 40,
        title: str = None,
    ) -> str:
        """
        Format data as an aligned text table.

        Args:
            headers: Column header names.
            rows: List of row data (each row is a list).
            max_width: Maximum column width before truncation.
            title: Optional title displayed above the table.

        Returns:
            Formatted table string.
        """
        if not headers:
            return "(no columns)"
        if not rows:
            rows_display = []
        else:
            rows_display = rows

        # Stringify all values
        str_headers = [str(h) for h in headers]
        str_rows = []
        for row in rows_display:
            str_row = []
            for val in row:
                if val is None:
                    str_row.append("NULL")
                elif isinstance(val, bytes):
                    str_row.append("<BLOB %d bytes>" % len(val))
                elif isinstance(val, float):
                    str_row.append("%.6g" % val)
                else:
                    s = str(val)
                    # Replace newlines for display
                    s = s.replace("\n", "\\n").replace("\r", "\\r")
                    str_row.append(s)
            str_rows.append(str_row)

        # Calculate column widths
        col_widths = [len(h) for h in str_headers]
        for row in str_rows:
            for i, val in enumerate(row):
                if i < len(col_widths):
                    col_widths[i] = max(col_widths[i], len(val))

        # Apply max width
        col_widths = [min(w, max_width) for w in col_widths]

        # Truncate values
        def truncate(s: str, width: int) -> str:
            if len(s) <= width:
                return s
            return s[: width - 3] + "..."

        # Build separator
        sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"

        lines = []
        if title:
            lines.append("")
            lines.append(title)

        # Header
        lines.append(sep)
        header_line = "| " + " | ".join(
            truncate(h, w).ljust(w) for h, w in zip(str_headers, col_widths)
        ) + " |"
        lines.append(header_line)
        lines.append(sep)

        # Rows
        for row in str_rows:
            padded = []
            for i, w in enumerate(col_widths):
                val = row[i] if i < len(row) else ""
                val = truncate(val, w)
                # Right-align if it looks numeric
                try:
                    float(row[i] if i < len(row) else "x")
                    padded.append(val.rjust(w))
                except (ValueError, IndexError):
                    padded.append(val.ljust(w))
            lines.append("| " + " | ".join(padded) + " |")

        lines.append(sep)

        if str_rows:
            lines.append("(%d row%s)" % (len(str_rows), "s" if len(str_rows) != 1 else ""))
        else:
            lines.append("(0 rows)")

        return "\n".join(lines)

    @staticmethod
    def format_json(data: Any, indent: int = 2) -> str:
        """
        Format data as JSON string.

        Args:
            data: Data to serialize.
            indent: JSON indentation level.

        Returns:
            JSON string.
        """
        def default_serializer(obj):
            if isinstance(obj, bytes):
                return "<BLOB %d bytes>" % len(obj)
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, Path):
                return str(obj)
            return str(obj)

        return json.dumps(data, indent=indent, default=default_serializer,
                          ensure_ascii=False)

    @staticmethod
    def format_csv_str(headers: List[str], rows: List[List[Any]]) -> str:
        """
        Format data as CSV string.

        Args:
            headers: Column headers.
            rows: Row data.

        Returns:
            CSV formatted string.
        """
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)
        for row in rows:
            csv_row = []
            for val in row:
                if val is None:
                    csv_row.append("")
                elif isinstance(val, bytes):
                    csv_row.append("<BLOB %d bytes>" % len(val))
                else:
                    csv_row.append(str(val))
            writer.writerow(csv_row)
        return output.getvalue()

    @staticmethod
    def format_markdown(headers: List[str], rows: List[List[Any]]) -> str:
        """
        Format data as a Markdown table.

        Args:
            headers: Column headers.
            rows: Row data.

        Returns:
            Markdown table string.
        """
        if not headers:
            return "(no columns)"

        # Stringify
        str_headers = [str(h) for h in headers]
        str_rows = []
        for row in rows:
            str_row = []
            for val in row:
                if val is None:
                    str_row.append("NULL")
                elif isinstance(val, bytes):
                    str_row.append("`<BLOB %d bytes>`" % len(val))
                else:
                    s = str(val).replace("|", "\\|")
                    str_row.append(s)
            str_rows.append(str_row)

        lines = []
        lines.append("| " + " | ".join(str_headers) + " |")
        lines.append("| " + " | ".join("---" for _ in str_headers) + " |")
        for row in str_rows:
            # Pad row if shorter than headers
            while len(row) < len(str_headers):
                row.append("")
            lines.append("| " + " | ".join(row) + " |")

        return "\n".join(lines)


# =============================================================================
# SQLITE EXPLORER
# =============================================================================

class SQLiteExplorer:
    """
    Smart SQLite Database Explorer.

    Provides read-only database inspection with beautiful formatted output.
    Supports info, tables, schema, browse, query, export, stats, search,
    size analysis, diff, and vacuum operations.

    Example:
        >>> db = SQLiteExplorer("mydata.db")
        >>> tables = db.get_tables()
        >>> schema = db.get_schema("users")
        >>> results = db.query("SELECT * FROM users LIMIT 5")
        >>> db.close()
    """

    def __init__(self, db_path: str):
        """
        Initialize SQLiteExplorer with a database path.

        Args:
            db_path: Path to the SQLite database file.

        Raises:
            FileNotFoundError: If the database file does not exist.
            ValueError: If the path is empty or None.
        """
        if not db_path:
            raise ValueError("Database path cannot be empty")

        self.db_path = Path(db_path).resolve()

        if not self.db_path.exists():
            raise FileNotFoundError("Database not found: %s" % self.db_path)

        if not self.db_path.is_file():
            raise ValueError("Path is not a file: %s" % self.db_path)

        self._conn = None

    def _connect(self, readonly: bool = True) -> sqlite3.Connection:
        """
        Get or create a database connection.

        Args:
            readonly: If True, open in read-only mode.

        Returns:
            sqlite3.Connection object.
        """
        if self._conn is not None:
            return self._conn

        if readonly:
            # Use URI mode for read-only
            uri = "file:%s?mode=ro" % str(self.db_path).replace("\\", "/")
            try:
                conn = sqlite3.connect(uri, uri=True)
            except sqlite3.OperationalError:
                # Fallback if URI mode not supported
                conn = sqlite3.connect(str(self.db_path))
        else:
            conn = sqlite3.connect(str(self.db_path))

        conn.row_factory = sqlite3.Row
        self._conn = conn
        return conn

    def close(self):
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    # -------------------------------------------------------------------------
    # INFO
    # -------------------------------------------------------------------------

    def get_info(self) -> Dict[str, Any]:
        """
        Get database metadata and overview.

        Returns:
            Dict with database info including path, size, tables, version.
        """
        conn = self._connect()
        cursor = conn.cursor()

        # File info
        file_size = self.db_path.stat().st_size
        modified = datetime.fromtimestamp(self.db_path.stat().st_mtime)

        # SQLite version
        cursor.execute("SELECT sqlite_version()")
        sqlite_version = cursor.fetchone()[0]

        # Page info
        cursor.execute("PRAGMA page_size")
        page_size = cursor.fetchone()[0]
        cursor.execute("PRAGMA page_count")
        page_count = cursor.fetchone()[0]
        cursor.execute("PRAGMA freelist_count")
        freelist_count = cursor.fetchone()[0]

        # Journal mode
        cursor.execute("PRAGMA journal_mode")
        journal_mode = cursor.fetchone()[0]

        # Encoding
        cursor.execute("PRAGMA encoding")
        encoding = cursor.fetchone()[0]

        # Table count
        cursor.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        )
        table_count = cursor.fetchone()[0]

        # Index count
        cursor.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' "
            "AND name NOT LIKE 'sqlite_%'"
        )
        index_count = cursor.fetchone()[0]

        # View count
        cursor.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='view'"
        )
        view_count = cursor.fetchone()[0]

        # Trigger count
        cursor.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'"
        )
        trigger_count = cursor.fetchone()[0]

        return {
            "path": str(self.db_path),
            "file_size": file_size,
            "file_size_display": _format_size(file_size),
            "modified": modified.isoformat(),
            "sqlite_version": sqlite_version,
            "page_size": page_size,
            "page_count": page_count,
            "freelist_count": freelist_count,
            "journal_mode": journal_mode,
            "encoding": encoding,
            "table_count": table_count,
            "index_count": index_count,
            "view_count": view_count,
            "trigger_count": trigger_count,
        }

    # -------------------------------------------------------------------------
    # TABLES
    # -------------------------------------------------------------------------

    def get_tables(self) -> List[Dict[str, Any]]:
        """
        List all tables with row counts and column counts.

        Returns:
            List of dicts with table name, row_count, column_count.
        """
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        table_names = [row[0] for row in cursor.fetchall()]

        tables = []
        for name in table_names:
            # Row count
            try:
                cursor.execute('SELECT COUNT(*) FROM "%s"' % name)
                row_count = cursor.fetchone()[0]
            except sqlite3.OperationalError:
                row_count = -1

            # Column count
            cursor.execute('PRAGMA table_info("%s")' % name)
            columns = cursor.fetchall()
            col_count = len(columns)

            tables.append({
                "name": name,
                "row_count": row_count,
                "column_count": col_count,
            })

        return tables

    # -------------------------------------------------------------------------
    # SCHEMA
    # -------------------------------------------------------------------------

    def get_schema(self, table: str = None) -> List[Dict[str, Any]]:
        """
        Get table schema information.

        Args:
            table: Specific table name, or None for all tables.

        Returns:
            List of dicts with table schemas.

        Raises:
            ValueError: If the specified table does not exist.
        """
        conn = self._connect()
        cursor = conn.cursor()

        if table:
            # Verify table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,)
            )
            if not cursor.fetchone():
                available = [t["name"] for t in self.get_tables()]
                raise ValueError(
                    "Table '%s' not found. Available tables: %s"
                    % (table, ", ".join(available) if available else "(none)")
                )
            table_names = [table]
        else:
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
            table_names = [row[0] for row in cursor.fetchall()]

        schemas = []
        for tbl in table_names:
            # Column info
            cursor.execute('PRAGMA table_info("%s")' % tbl)
            columns = []
            for col in cursor.fetchall():
                columns.append({
                    "cid": col[0],
                    "name": col[1],
                    "type": col[2] if col[2] else "ANY",
                    "notnull": bool(col[3]),
                    "default": col[4],
                    "pk": bool(col[5]),
                })

            # Indexes
            cursor.execute('PRAGMA index_list("%s")' % tbl)
            indexes = []
            for idx in cursor.fetchall():
                idx_name = idx[1]
                idx_unique = bool(idx[2])
                cursor.execute('PRAGMA index_info("%s")' % idx_name)
                idx_columns = [ic[2] for ic in cursor.fetchall()]
                indexes.append({
                    "name": idx_name,
                    "unique": idx_unique,
                    "columns": idx_columns,
                })

            # Foreign keys
            cursor.execute('PRAGMA foreign_key_list("%s")' % tbl)
            fks = []
            for fk in cursor.fetchall():
                fks.append({
                    "id": fk[0],
                    "table": fk[2],
                    "from": fk[3],
                    "to": fk[4],
                })

            # CREATE statement
            cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (tbl,)
            )
            create_sql = cursor.fetchone()[0]

            schemas.append({
                "table": tbl,
                "columns": columns,
                "indexes": indexes,
                "foreign_keys": fks,
                "create_sql": create_sql,
            })

        return schemas

    # -------------------------------------------------------------------------
    # BROWSE
    # -------------------------------------------------------------------------

    def browse(
        self,
        table: str,
        limit: int = 50,
        offset: int = 0,
        where: str = None,
        order_by: str = None,
    ) -> Dict[str, Any]:
        """
        Browse table data with pagination.

        Args:
            table: Table name to browse.
            limit: Maximum rows to return.
            offset: Starting row offset.
            where: Optional WHERE clause (without WHERE keyword).
            order_by: Optional ORDER BY clause (without ORDER BY keyword).

        Returns:
            Dict with headers, rows, total_rows, limit, offset.

        Raises:
            ValueError: If table does not exist.
        """
        conn = self._connect()
        cursor = conn.cursor()

        # Verify table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        )
        if not cursor.fetchone():
            available = [t["name"] for t in self.get_tables()]
            raise ValueError(
                "Table '%s' not found. Available tables: %s"
                % (table, ", ".join(available) if available else "(none)")
            )

        # Total row count
        count_sql = 'SELECT COUNT(*) FROM "%s"' % table
        if where:
            count_sql += " WHERE %s" % where
        cursor.execute(count_sql)
        total_rows = cursor.fetchone()[0]

        # Get column names
        cursor.execute('PRAGMA table_info("%s")' % table)
        headers = [col[1] for col in cursor.fetchall()]

        # Build query
        query_sql = 'SELECT * FROM "%s"' % table
        if where:
            query_sql += " WHERE %s" % where
        if order_by:
            query_sql += " ORDER BY %s" % order_by
        query_sql += " LIMIT %d OFFSET %d" % (limit, offset)

        cursor.execute(query_sql)
        rows = [list(row) for row in cursor.fetchall()]

        return {
            "table": table,
            "headers": headers,
            "rows": rows,
            "total_rows": total_rows,
            "limit": limit,
            "offset": offset,
            "showing": "%d-%d of %d" % (
                offset + 1 if rows else 0,
                offset + len(rows),
                total_rows,
            ),
        }

    # -------------------------------------------------------------------------
    # QUERY
    # -------------------------------------------------------------------------

    def query(self, sql: str) -> Dict[str, Any]:
        """
        Execute a raw SQL query and return results.

        Args:
            sql: SQL query string.

        Returns:
            Dict with headers, rows, row_count, and the executed sql.

        Raises:
            sqlite3.Error: On SQL errors.
        """
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute(sql)

        if cursor.description:
            headers = [desc[0] for desc in cursor.description]
            rows = [list(row) for row in cursor.fetchall()]
        else:
            headers = []
            rows = []

        return {
            "sql": sql,
            "headers": headers,
            "rows": rows,
            "row_count": len(rows),
        }

    # -------------------------------------------------------------------------
    # EXPORT
    # -------------------------------------------------------------------------

    def export_table(
        self,
        table: str,
        fmt: str = "csv",
        output: str = None,
        query_sql: str = None,
    ) -> str:
        """
        Export table or query results to CSV, JSON, or Markdown.

        Args:
            table: Table name to export (ignored if query_sql provided).
            fmt: Output format ('csv', 'json', 'md').
            output: Output file path, or None for stdout.
            query_sql: Optional SQL query to export instead of full table.

        Returns:
            Formatted string of the export data.
        """
        if query_sql:
            result = self.query(query_sql)
            headers = result["headers"]
            rows = result["rows"]
        else:
            # Verify table exists
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,)
            )
            if not cursor.fetchone():
                available = [t["name"] for t in self.get_tables()]
                raise ValueError(
                    "Table '%s' not found. Available tables: %s"
                    % (table, ", ".join(available) if available else "(none)")
                )

            cursor.execute('PRAGMA table_info("%s")' % table)
            headers = [col[1] for col in cursor.fetchall()]

            cursor.execute('SELECT * FROM "%s"' % table)
            rows = [list(row) for row in cursor.fetchall()]

        # Format
        if fmt == "json":
            data = []
            for row in rows:
                record = {}
                for i, h in enumerate(headers):
                    val = row[i] if i < len(row) else None
                    if isinstance(val, bytes):
                        val = "<BLOB %d bytes>" % len(val)
                    record[h] = val
                data.append(record)
            content = TableFormatter.format_json(data)
        elif fmt == "md":
            content = TableFormatter.format_markdown(headers, rows)
        else:  # csv
            content = TableFormatter.format_csv_str(headers, rows)

        # Write to file or return
        if output:
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(content, encoding="utf-8")

        return content

    # -------------------------------------------------------------------------
    # STATS
    # -------------------------------------------------------------------------

    def get_stats(self, table: str) -> List[Dict[str, Any]]:
        """
        Get column statistics for a table.

        Args:
            table: Table name to analyze.

        Returns:
            List of dicts with column statistics (min, max, avg, etc.).

        Raises:
            ValueError: If table does not exist.
        """
        conn = self._connect()
        cursor = conn.cursor()

        # Verify table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        )
        if not cursor.fetchone():
            available = [t["name"] for t in self.get_tables()]
            raise ValueError(
                "Table '%s' not found. Available tables: %s"
                % (table, ", ".join(available) if available else "(none)")
            )

        # Get columns
        cursor.execute('PRAGMA table_info("%s")' % table)
        columns_info = cursor.fetchall()

        # Total rows
        cursor.execute('SELECT COUNT(*) FROM "%s"' % table)
        total_rows = cursor.fetchone()[0]

        stats = []
        for col_info in columns_info:
            col_name = col_info[1]
            col_type = col_info[2] if col_info[2] else "ANY"

            # Basic stats via SQL
            cursor.execute('''
                SELECT
                    COUNT("%s") as non_null,
                    COUNT(DISTINCT "%s") as distinct_count
                FROM "%s"
            ''' % (col_name, col_name, table))
            row = cursor.fetchone()
            non_null = row[0]
            distinct_count = row[1]
            null_count = total_rows - non_null

            stat = {
                "column": col_name,
                "type": col_type,
                "total_rows": total_rows,
                "non_null": non_null,
                "null_count": null_count,
                "null_pct": ("%.1f%%" % (null_count / total_rows * 100)) if total_rows > 0 else "N/A",
                "distinct": distinct_count,
            }

            # Numeric stats (try, may fail for non-numeric)
            try:
                cursor.execute('''
                    SELECT
                        MIN("%s"),
                        MAX("%s"),
                        AVG("%s"),
                        SUM("%s")
                    FROM "%s"
                ''' % (col_name, col_name, col_name, col_name, table))
                num_row = cursor.fetchone()
                stat["min"] = num_row[0]
                stat["max"] = num_row[1]
                stat["avg"] = round(num_row[2], 4) if num_row[2] is not None else None
                stat["sum"] = num_row[3]
            except (sqlite3.OperationalError, TypeError):
                stat["min"] = None
                stat["max"] = None
                stat["avg"] = None
                stat["sum"] = None

            stats.append(stat)

        return stats

    # -------------------------------------------------------------------------
    # SEARCH
    # -------------------------------------------------------------------------

    def search(
        self,
        term: str,
        tables: List[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Search for a term across all text columns in all tables.

        Args:
            term: Search term (case-insensitive LIKE match).
            tables: Optional list of table names to search. None = all tables.
            limit: Maximum total results.

        Returns:
            List of match dicts with table, column, row data.
        """
        conn = self._connect()
        cursor = conn.cursor()

        if tables:
            search_tables = tables
        else:
            search_tables = [t["name"] for t in self.get_tables()]

        matches = []
        remaining = limit

        for tbl in search_tables:
            if remaining <= 0:
                break

            # Get text-like columns
            cursor.execute('PRAGMA table_info("%s")' % tbl)
            columns = cursor.fetchall()
            text_cols = []
            all_col_names = []
            for col in columns:
                all_col_names.append(col[1])
                col_type = (col[2] or "").upper()
                if col_type in ("", "TEXT", "VARCHAR", "CHAR", "CLOB",
                                "ANY", "STRING", "NVARCHAR"):
                    text_cols.append(col[1])
                elif not col_type.startswith(("INT", "REAL", "FLOAT",
                                              "DOUBLE", "NUMERIC", "BLOB",
                                              "BOOLEAN")):
                    # Include columns with unknown types
                    text_cols.append(col[1])

            if not text_cols:
                continue

            # Build search query
            conditions = ' OR '.join(
                '"%s" LIKE ?' % col for col in text_cols
            )
            sql = 'SELECT * FROM "%s" WHERE %s LIMIT %d' % (
                tbl, conditions, remaining
            )
            params = ["%%%s%%" % term] * len(text_cols)

            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                for row in rows:
                    # Find which column(s) matched
                    matched_cols = []
                    for col in text_cols:
                        idx = all_col_names.index(col)
                        val = row[idx]
                        if val is not None and term.lower() in str(val).lower():
                            matched_cols.append(col)

                    match_data = {}
                    for i, col_name in enumerate(all_col_names):
                        match_data[col_name] = row[i]

                    matches.append({
                        "table": tbl,
                        "matched_columns": matched_cols,
                        "data": match_data,
                    })
                    remaining -= 1
                    if remaining <= 0:
                        break
            except sqlite3.OperationalError:
                continue

        return matches

    # -------------------------------------------------------------------------
    # SIZE
    # -------------------------------------------------------------------------

    def get_size(self) -> Dict[str, Any]:
        """
        Get detailed size analysis of the database.

        Returns:
            Dict with file size, table sizes, index sizes, free space.
        """
        conn = self._connect()
        cursor = conn.cursor()

        file_size = self.db_path.stat().st_size

        cursor.execute("PRAGMA page_size")
        page_size = cursor.fetchone()[0]
        cursor.execute("PRAGMA page_count")
        page_count = cursor.fetchone()[0]
        cursor.execute("PRAGMA freelist_count")
        freelist_count = cursor.fetchone()[0]

        used_pages = page_count - freelist_count
        free_space = freelist_count * page_size
        used_space = used_pages * page_size

        # Table sizes (approximate via row count * avg row size)
        table_sizes = []
        tables = self.get_tables()
        for tbl in tables:
            tbl_name = tbl["name"]
            try:
                # Get approximate size by counting pages used
                cursor.execute(
                    'SELECT SUM(LENGTH(CAST("%s" AS TEXT))) FROM (SELECT * FROM "%s")'
                    % (tbl_name, tbl_name)
                )
                # Simpler approach: estimate from data length
                cursor.execute('SELECT * FROM "%s" LIMIT 1' % tbl_name)
                sample = cursor.fetchone()
                if sample and tbl["row_count"] > 0:
                    # Estimate avg row size from first row
                    row_size = sum(
                        len(str(v)) if v is not None else 0
                        for v in sample
                    )
                    est_size = row_size * tbl["row_count"]
                else:
                    est_size = 0

                table_sizes.append({
                    "table": tbl_name,
                    "rows": tbl["row_count"],
                    "columns": tbl["column_count"],
                    "estimated_data_size": est_size,
                    "estimated_data_size_display": _format_size(est_size),
                })
            except sqlite3.OperationalError:
                table_sizes.append({
                    "table": tbl_name,
                    "rows": tbl["row_count"],
                    "columns": tbl["column_count"],
                    "estimated_data_size": 0,
                    "estimated_data_size_display": "N/A",
                })

        return {
            "file_size": file_size,
            "file_size_display": _format_size(file_size),
            "page_size": page_size,
            "page_count": page_count,
            "used_pages": used_pages,
            "free_pages": freelist_count,
            "used_space": used_space,
            "used_space_display": _format_size(used_space),
            "free_space": free_space,
            "free_space_display": _format_size(free_space),
            "free_pct": "%.1f%%" % (freelist_count / page_count * 100) if page_count > 0 else "0.0%",
            "tables": table_sizes,
        }

    # -------------------------------------------------------------------------
    # DIFF
    # -------------------------------------------------------------------------

    def diff(self, other_db_path: str) -> Dict[str, Any]:
        """
        Compare schemas of two databases.

        Args:
            other_db_path: Path to the other database to compare.

        Returns:
            Dict with differences (tables only in A, only in B, column diffs).
        """
        other = SQLiteExplorer(other_db_path)
        try:
            my_tables = {t["name"]: t for t in self.get_tables()}
            other_tables = {t["name"]: t for t in other.get_tables()}

            only_in_a = sorted(set(my_tables.keys()) - set(other_tables.keys()))
            only_in_b = sorted(set(other_tables.keys()) - set(my_tables.keys()))
            common = sorted(set(my_tables.keys()) & set(other_tables.keys()))

            column_diffs = []
            for tbl in common:
                my_schema = self.get_schema(tbl)[0]
                other_schema = other.get_schema(tbl)[0]

                my_cols = {c["name"]: c for c in my_schema["columns"]}
                other_cols = {c["name"]: c for c in other_schema["columns"]}

                cols_only_a = sorted(set(my_cols.keys()) - set(other_cols.keys()))
                cols_only_b = sorted(set(other_cols.keys()) - set(my_cols.keys()))

                type_diffs = []
                for col in sorted(set(my_cols.keys()) & set(other_cols.keys())):
                    if my_cols[col]["type"] != other_cols[col]["type"]:
                        type_diffs.append({
                            "column": col,
                            "type_a": my_cols[col]["type"],
                            "type_b": other_cols[col]["type"],
                        })

                row_diff = my_tables[tbl]["row_count"] - other_tables[tbl]["row_count"]

                if cols_only_a or cols_only_b or type_diffs or row_diff != 0:
                    column_diffs.append({
                        "table": tbl,
                        "columns_only_in_a": cols_only_a,
                        "columns_only_in_b": cols_only_b,
                        "type_differences": type_diffs,
                        "row_count_a": my_tables[tbl]["row_count"],
                        "row_count_b": other_tables[tbl]["row_count"],
                        "row_diff": row_diff,
                    })

            identical = (
                not only_in_a
                and not only_in_b
                and not column_diffs
            )

            return {
                "database_a": str(self.db_path),
                "database_b": str(other.db_path),
                "identical_schema": identical,
                "tables_only_in_a": only_in_a,
                "tables_only_in_b": only_in_b,
                "common_tables": len(common),
                "table_differences": column_diffs,
            }
        finally:
            other.close()

    # -------------------------------------------------------------------------
    # VACUUM
    # -------------------------------------------------------------------------

    def vacuum(self) -> Dict[str, Any]:
        """
        Optimize the database with VACUUM.

        Requires closing the read-only connection and opening a writable one.

        Returns:
            Dict with before/after sizes.
        """
        before_size = self.db_path.stat().st_size

        # Close read-only connection
        self.close()

        # Open writable connection for vacuum
        conn = sqlite3.connect(str(self.db_path))
        try:
            conn.execute("VACUUM")
        finally:
            conn.close()

        after_size = self.db_path.stat().st_size
        saved = before_size - after_size

        return {
            "before_size": before_size,
            "before_size_display": _format_size(before_size),
            "after_size": after_size,
            "after_size_display": _format_size(after_size),
            "saved": saved,
            "saved_display": _format_size(saved),
            "saved_pct": "%.1f%%" % (saved / before_size * 100) if before_size > 0 else "0.0%",
        }


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def _format_size(size_bytes: int) -> str:
    """
    Format byte count as human-readable string.

    Args:
        size_bytes: Size in bytes.

    Returns:
        Human-readable size string (e.g., '1.5 MB').
    """
    if size_bytes < 0:
        return "N/A"
    if size_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    size = float(size_bytes)
    while size >= 1024 and i < len(units) - 1:
        size /= 1024
        i += 1
    if i == 0:
        return "%d B" % size_bytes
    return "%.1f %s" % (size, units[i])


# =============================================================================
# CLI DISPLAY FUNCTIONS
# =============================================================================

def _display_info(db: SQLiteExplorer, fmt: str = "text") -> str:
    """Display database info."""
    info = db.get_info()

    if fmt == "json":
        return TableFormatter.format_json(info)

    lines = []
    lines.append("=" * 60)
    lines.append("DATABASE INFO")
    lines.append("=" * 60)
    lines.append("  Path:           %s" % info["path"])
    lines.append("  File Size:      %s (%d bytes)" % (info["file_size_display"], info["file_size"]))
    lines.append("  Last Modified:  %s" % info["modified"])
    lines.append("  SQLite Version: %s" % info["sqlite_version"])
    lines.append("  Encoding:       %s" % info["encoding"])
    lines.append("  Journal Mode:   %s" % info["journal_mode"])
    lines.append("  Page Size:      %s" % _format_size(info["page_size"]))
    lines.append("  Page Count:     %d" % info["page_count"])
    lines.append("  Free Pages:     %d" % info["freelist_count"])
    lines.append("-" * 60)
    lines.append("  Tables:         %d" % info["table_count"])
    lines.append("  Indexes:        %d" % info["index_count"])
    lines.append("  Views:          %d" % info["view_count"])
    lines.append("  Triggers:       %d" % info["trigger_count"])
    lines.append("=" * 60)
    return "\n".join(lines)


def _display_tables(db: SQLiteExplorer, fmt: str = "text") -> str:
    """Display table listing."""
    tables = db.get_tables()

    if fmt == "json":
        return TableFormatter.format_json(tables)

    if not tables:
        return "(no tables found)"

    headers = ["Table Name", "Rows", "Columns"]
    rows = [[t["name"], t["row_count"], t["column_count"]] for t in tables]

    total_rows = sum(t["row_count"] for t in tables if t["row_count"] >= 0)
    rows.append(["--- TOTAL ---", total_rows, ""])

    if fmt == "md":
        return TableFormatter.format_markdown(headers, rows)

    return TableFormatter.format_table(headers, rows, title="TABLES")


def _display_schema(db: SQLiteExplorer, table: str = None, fmt: str = "text") -> str:
    """Display table schema."""
    schemas = db.get_schema(table)

    if fmt == "json":
        return TableFormatter.format_json(schemas)

    parts = []
    for schema in schemas:
        lines = []
        lines.append("=" * 60)
        lines.append("TABLE: %s" % schema["table"])
        lines.append("=" * 60)

        # Columns
        col_headers = ["#", "Column", "Type", "NotNull", "Default", "PK"]
        col_rows = []
        for col in schema["columns"]:
            col_rows.append([
                col["cid"],
                col["name"],
                col["type"],
                "YES" if col["notnull"] else "",
                str(col["default"]) if col["default"] is not None else "",
                "PK" if col["pk"] else "",
            ])

        if fmt == "md":
            lines.append(TableFormatter.format_markdown(col_headers, col_rows))
        else:
            lines.append(TableFormatter.format_table(col_headers, col_rows))

        # Indexes
        if schema["indexes"]:
            lines.append("")
            lines.append("  Indexes:")
            for idx in schema["indexes"]:
                unique = " (UNIQUE)" if idx["unique"] else ""
                lines.append("    - %s%s: %s" % (
                    idx["name"], unique, ", ".join(idx["columns"])
                ))

        # Foreign keys
        if schema["foreign_keys"]:
            lines.append("")
            lines.append("  Foreign Keys:")
            for fk in schema["foreign_keys"]:
                lines.append("    - %s -> %s.%s" % (
                    fk["from"], fk["table"], fk["to"]
                ))

        # CREATE SQL
        lines.append("")
        lines.append("  CREATE SQL:")
        lines.append("    %s" % schema["create_sql"])

        parts.append("\n".join(lines))

    return "\n\n".join(parts)


def _display_browse(db: SQLiteExplorer, table: str, limit: int, offset: int,
                    where: str, order_by: str, fmt: str = "text") -> str:
    """Display browsed data."""
    result = db.browse(table, limit=limit, offset=offset,
                       where=where, order_by=order_by)

    if fmt == "json":
        return TableFormatter.format_json(result)
    if fmt == "md":
        return TableFormatter.format_markdown(result["headers"], result["rows"])

    title = "TABLE: %s  [%s]" % (result["table"], result["showing"])
    return TableFormatter.format_table(
        result["headers"], result["rows"], title=title
    )


def _display_query(db: SQLiteExplorer, sql: str, fmt: str = "text") -> str:
    """Display query results."""
    result = db.query(sql)

    if fmt == "json":
        return TableFormatter.format_json(result)
    if fmt == "md":
        return TableFormatter.format_markdown(result["headers"], result["rows"])

    title = "QUERY: %s  (%d rows)" % (
        sql[:60] + ("..." if len(sql) > 60 else ""),
        result["row_count"],
    )
    return TableFormatter.format_table(
        result["headers"], result["rows"], title=title
    )


def _display_stats(db: SQLiteExplorer, table: str, fmt: str = "text") -> str:
    """Display column statistics."""
    stats = db.get_stats(table)

    if fmt == "json":
        return TableFormatter.format_json(stats)

    headers = ["Column", "Type", "Non-Null", "Null", "Null%", "Distinct",
               "Min", "Max", "Avg"]
    rows = []
    for s in stats:
        rows.append([
            s["column"],
            s["type"],
            s["non_null"],
            s["null_count"],
            s["null_pct"],
            s["distinct"],
            s["min"] if s["min"] is not None else "",
            s["max"] if s["max"] is not None else "",
            s["avg"] if s["avg"] is not None else "",
        ])

    if fmt == "md":
        return TableFormatter.format_markdown(headers, rows)

    return TableFormatter.format_table(
        headers, rows, title="STATS: %s" % table, max_width=30
    )


def _display_search(db: SQLiteExplorer, term: str, tables: List[str],
                    limit: int, fmt: str = "text") -> str:
    """Display search results."""
    matches = db.search(term, tables=tables, limit=limit)

    if fmt == "json":
        return TableFormatter.format_json(matches)

    if not matches:
        return 'No matches found for "%s"' % term

    lines = []
    lines.append("=" * 60)
    lines.append('SEARCH: "%s"  (%d matches)' % (term, len(matches)))
    lines.append("=" * 60)

    for i, match in enumerate(matches, 1):
        lines.append("")
        lines.append("--- Match %d [%s] (columns: %s) ---" % (
            i, match["table"], ", ".join(match["matched_columns"])
        ))
        for key, val in match["data"].items():
            if val is None:
                display_val = "NULL"
            elif isinstance(val, bytes):
                display_val = "<BLOB %d bytes>" % len(val)
            else:
                display_val = str(val)
                if len(display_val) > 80:
                    display_val = display_val[:77] + "..."
            # Highlight matched columns
            marker = " <<" if key in match["matched_columns"] else ""
            lines.append("  %s: %s%s" % (key, display_val, marker))

    lines.append("")
    lines.append("(%d match%s)" % (len(matches), "es" if len(matches) != 1 else ""))
    return "\n".join(lines)


def _display_size(db: SQLiteExplorer, fmt: str = "text") -> str:
    """Display size analysis."""
    size_info = db.get_size()

    if fmt == "json":
        return TableFormatter.format_json(size_info)

    lines = []
    lines.append("=" * 60)
    lines.append("SIZE ANALYSIS")
    lines.append("=" * 60)
    lines.append("  File Size:   %s" % size_info["file_size_display"])
    lines.append("  Used Space:  %s (%d pages)" % (
        size_info["used_space_display"], size_info["used_pages"]
    ))
    lines.append("  Free Space:  %s (%d pages, %s)" % (
        size_info["free_space_display"],
        size_info["free_pages"],
        size_info["free_pct"],
    ))
    lines.append("  Page Size:   %s" % _format_size(size_info["page_size"]))
    lines.append("-" * 60)

    if size_info["tables"]:
        headers = ["Table", "Rows", "Columns", "Est. Data Size"]
        rows = []
        for t in size_info["tables"]:
            rows.append([
                t["table"],
                t["rows"],
                t["columns"],
                t["estimated_data_size_display"],
            ])
        lines.append(TableFormatter.format_table(headers, rows, title="TABLE SIZES"))
    else:
        lines.append("  (no tables)")

    if size_info["free_pages"] > 0:
        lines.append("")
        lines.append("  [!] %s of free space detected. Run 'vacuum' to reclaim." % (
            size_info["free_space_display"]
        ))

    return "\n".join(lines)


def _display_diff(db: SQLiteExplorer, other_path: str, fmt: str = "text") -> str:
    """Display schema diff."""
    result = db.diff(other_path)

    if fmt == "json":
        return TableFormatter.format_json(result)

    lines = []
    lines.append("=" * 60)
    lines.append("SCHEMA DIFF")
    lines.append("=" * 60)
    lines.append("  A: %s" % result["database_a"])
    lines.append("  B: %s" % result["database_b"])
    lines.append("  Common tables: %d" % result["common_tables"])
    lines.append("-" * 60)

    if result["identical_schema"]:
        lines.append("  [OK] Schemas are identical!")
        return "\n".join(lines)

    if result["tables_only_in_a"]:
        lines.append("")
        lines.append("  Tables only in A:")
        for t in result["tables_only_in_a"]:
            lines.append("    + %s" % t)

    if result["tables_only_in_b"]:
        lines.append("")
        lines.append("  Tables only in B:")
        for t in result["tables_only_in_b"]:
            lines.append("    + %s" % t)

    if result["table_differences"]:
        lines.append("")
        lines.append("  Table Differences:")
        for diff in result["table_differences"]:
            lines.append("    Table: %s" % diff["table"])
            if diff["columns_only_in_a"]:
                lines.append("      Columns only in A: %s" % ", ".join(diff["columns_only_in_a"]))
            if diff["columns_only_in_b"]:
                lines.append("      Columns only in B: %s" % ", ".join(diff["columns_only_in_b"]))
            if diff["type_differences"]:
                for td in diff["type_differences"]:
                    lines.append("      Column '%s': %s (A) vs %s (B)" % (
                        td["column"], td["type_a"], td["type_b"]
                    ))
            if diff["row_diff"] != 0:
                lines.append("      Row count: %d (A) vs %d (B) [diff: %+d]" % (
                    diff["row_count_a"], diff["row_count_b"], diff["row_diff"]
                ))

    lines.append("=" * 60)
    return "\n".join(lines)


def _display_vacuum(db: SQLiteExplorer, fmt: str = "text") -> str:
    """Display vacuum results."""
    result = db.vacuum()

    if fmt == "json":
        return TableFormatter.format_json(result)

    lines = []
    lines.append("=" * 60)
    lines.append("VACUUM COMPLETE")
    lines.append("=" * 60)
    lines.append("  Before: %s" % result["before_size_display"])
    lines.append("  After:  %s" % result["after_size_display"])
    lines.append("  Saved:  %s (%s)" % (result["saved_display"], result["saved_pct"]))
    lines.append("=" * 60)
    return "\n".join(lines)


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def main():
    """CLI entry point for SQLiteExplorer."""
    # Fix Windows console encoding
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    parser = argparse.ArgumentParser(
        prog="sqliteexplorer",
        description="SQLiteExplorer - Smart SQLite Database Explorer & Management Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
        Examples:
          %(prog)s info mydata.db                    Database overview
          %(prog)s tables mydata.db                  List all tables
          %(prog)s schema mydata.db users            Show 'users' table schema
          %(prog)s browse mydata.db users            Browse table data
          %(prog)s query mydata.db "SELECT * FROM users LIMIT 5"
          %(prog)s export mydata.db users --format json --output users.json
          %(prog)s stats mydata.db users             Column statistics
          %(prog)s search mydata.db "admin"          Search all tables
          %(prog)s size mydata.db                    Size analysis
          %(prog)s diff db1.db db2.db                Compare schemas
          %(prog)s vacuum mydata.db --confirm        Optimize database

        For more information: https://github.com/DonkRonk17/SQLiteExplorer
        """),
    )

    parser.add_argument("--version", action="version",
                        version="SQLiteExplorer %s" % __version__)

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # --- info ---
    p_info = subparsers.add_parser("info", help="Database metadata and overview")
    p_info.add_argument("database", help="Path to SQLite database")
    p_info.add_argument("--format", choices=["text", "json"], default="text",
                        help="Output format (default: text)")

    # --- tables ---
    p_tables = subparsers.add_parser("tables", help="List all tables")
    p_tables.add_argument("database", help="Path to SQLite database")
    p_tables.add_argument("--format", choices=["text", "json", "md"],
                          default="text", help="Output format")

    # --- schema ---
    p_schema = subparsers.add_parser("schema", help="Show table schema")
    p_schema.add_argument("database", help="Path to SQLite database")
    p_schema.add_argument("table", nargs="?", default=None,
                          help="Table name (omit for all tables)")
    p_schema.add_argument("--format", choices=["text", "json", "md"],
                          default="text", help="Output format")

    # --- browse ---
    p_browse = subparsers.add_parser("browse", help="Browse table data")
    p_browse.add_argument("database", help="Path to SQLite database")
    p_browse.add_argument("table", help="Table to browse")
    p_browse.add_argument("--limit", type=int, default=50,
                          help="Rows per page (default: 50)")
    p_browse.add_argument("--offset", type=int, default=0,
                          help="Starting row (default: 0)")
    p_browse.add_argument("--where", default=None,
                          help="WHERE clause filter")
    p_browse.add_argument("--order-by", default=None,
                          help="ORDER BY clause")
    p_browse.add_argument("--format", choices=["text", "json", "md"],
                          default="text", help="Output format")

    # --- query ---
    p_query = subparsers.add_parser("query", help="Execute SQL query")
    p_query.add_argument("database", help="Path to SQLite database")
    p_query.add_argument("sql", help="SQL query to execute")
    p_query.add_argument("--format", choices=["text", "json", "md"],
                         default="text", help="Output format")

    # --- export ---
    p_export = subparsers.add_parser("export", help="Export table data")
    p_export.add_argument("database", help="Path to SQLite database")
    p_export.add_argument("table", help="Table to export")
    p_export.add_argument("--format", choices=["csv", "json", "md"],
                          default="csv", help="Export format (default: csv)")
    p_export.add_argument("--output", "-o", default=None,
                          help="Output file path (default: stdout)")
    p_export.add_argument("--query", default=None,
                          help="SQL query to export instead of full table")

    # --- stats ---
    p_stats = subparsers.add_parser("stats", help="Column statistics")
    p_stats.add_argument("database", help="Path to SQLite database")
    p_stats.add_argument("table", help="Table to analyze")
    p_stats.add_argument("--format", choices=["text", "json", "md"],
                         default="text", help="Output format")

    # --- search ---
    p_search = subparsers.add_parser("search", help="Search across tables")
    p_search.add_argument("database", help="Path to SQLite database")
    p_search.add_argument("term", help="Search term")
    p_search.add_argument("--tables", default=None,
                          help="Comma-separated table names to search")
    p_search.add_argument("--limit", type=int, default=100,
                          help="Max results (default: 100)")
    p_search.add_argument("--format", choices=["text", "json"],
                          default="text", help="Output format")

    # --- size ---
    p_size = subparsers.add_parser("size", help="Detailed size analysis")
    p_size.add_argument("database", help="Path to SQLite database")
    p_size.add_argument("--format", choices=["text", "json"],
                        default="text", help="Output format")

    # --- diff ---
    p_diff = subparsers.add_parser("diff", help="Compare two databases")
    p_diff.add_argument("database", help="First database (A)")
    p_diff.add_argument("other", help="Second database (B)")
    p_diff.add_argument("--format", choices=["text", "json"],
                        default="text", help="Output format")

    # --- vacuum ---
    p_vacuum = subparsers.add_parser("vacuum", help="Optimize database")
    p_vacuum.add_argument("database", help="Path to SQLite database")
    p_vacuum.add_argument("--confirm", action="store_true",
                          help="Required flag to actually vacuum")
    p_vacuum.add_argument("--format", choices=["text", "json"],
                          default="text", help="Output format")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    try:
        db = SQLiteExplorer(args.database)
    except FileNotFoundError as e:
        print("[X] Error: %s" % e)
        return 1
    except ValueError as e:
        print("[X] Error: %s" % e)
        return 1
    except Exception as e:
        print("[X] Unexpected error opening database: %s" % e)
        return 1

    try:
        if args.command == "info":
            print(_display_info(db, args.format))

        elif args.command == "tables":
            print(_display_tables(db, args.format))

        elif args.command == "schema":
            print(_display_schema(db, args.table, args.format))

        elif args.command == "browse":
            print(_display_browse(
                db, args.table, args.limit, args.offset,
                args.where, args.order_by, args.format
            ))

        elif args.command == "query":
            print(_display_query(db, args.sql, args.format))

        elif args.command == "export":
            content = db.export_table(
                args.table, fmt=args.format,
                output=args.output, query_sql=args.query
            )
            if args.output:
                print("[OK] Exported to: %s" % args.output)
            else:
                print(content)

        elif args.command == "stats":
            print(_display_stats(db, args.table, args.format))

        elif args.command == "search":
            search_tables = None
            if args.tables:
                search_tables = [t.strip() for t in args.tables.split(",")]
            print(_display_search(db, args.term, search_tables,
                                  args.limit, args.format))

        elif args.command == "size":
            print(_display_size(db, args.format))

        elif args.command == "diff":
            print(_display_diff(db, args.other, args.format))

        elif args.command == "vacuum":
            if not args.confirm:
                print("[!] Vacuum modifies the database file.")
                print("    Add --confirm to proceed.")
                print("    Recommended: backup your database first.")
                return 0
            print(_display_vacuum(db, args.format))

        return 0

    except sqlite3.OperationalError as e:
        print("[X] SQLite error: %s" % e)
        return 1
    except ValueError as e:
        print("[X] Error: %s" % e)
        return 1
    except KeyboardInterrupt:
        print("\n[!] Interrupted.")
        return 130
    except Exception as e:
        print("[X] Unexpected error: %s" % e)
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
