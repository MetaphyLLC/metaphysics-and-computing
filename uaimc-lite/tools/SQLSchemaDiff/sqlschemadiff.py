#!/usr/bin/env python3
"""
SQLSchemaDiff - SQL Schema Comparison & Migration Validator

Instantly compare two SQL schemas (SQLite databases, .sql dump files, or
inline CREATE TABLE statements) and see exactly what changed: added/removed
tables, column changes, index drift, foreign key changes. Generate migration
SQL to sync schemas and validate existing migration files.

Works with SQLite database files, SQL dump files (.sql), and inline CREATE
TABLE SQL strings. Zero external dependencies - Python 3.8+ stdlib only.

Author: ATLAS (Team Brain)
For: Logan Smith / Metaphy LLC
Version: 1.0.0
Date: 2026-03-14
License: MIT
"""

import argparse
import json
import re
import sqlite3
import sys
import textwrap
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class ColumnDef:
    """Represents a single column definition."""
    name: str
    col_type: str
    not_null: bool = False
    default_value: Optional[str] = None
    primary_key: bool = False

    def signature(self) -> str:
        """Return a normalized signature for comparison."""
        parts = [self.name.lower(), self.col_type.upper()]
        if self.primary_key:
            parts.append("PRIMARY_KEY")
        if self.not_null:
            parts.append("NOT_NULL")
        if self.default_value is not None:
            parts.append(f"DEFAULT={self.default_value}")
        return "|".join(parts)

    def to_sql(self) -> str:
        """Render column as SQL fragment."""
        parts = [f'    "{self.name}" {self.col_type}']
        if self.primary_key:
            parts[0] += " PRIMARY KEY"
        if self.not_null:
            parts[0] += " NOT NULL"
        if self.default_value is not None:
            parts[0] += f" DEFAULT {self.default_value}"
        return parts[0]


@dataclass
class IndexDef:
    """Represents an index definition."""
    name: str
    table: str
    columns: List[str]
    unique: bool = False

    def signature(self) -> str:
        return f"{self.table}|{','.join(sorted(c.lower() for c in self.columns))}|unique={self.unique}"


@dataclass
class ForeignKeyDef:
    """Represents a foreign key constraint."""
    table: str
    from_cols: List[str]
    to_table: str
    to_cols: List[str]
    on_update: str = "NO ACTION"
    on_delete: str = "NO ACTION"

    def signature(self) -> str:
        froms = ",".join(c.lower() for c in self.from_cols)
        tos = ",".join(c.lower() for c in self.to_cols)
        return f"{self.table}({froms})->{self.to_table}({tos})"


@dataclass
class TableDef:
    """Represents a complete table definition."""
    name: str
    columns: Dict[str, ColumnDef] = field(default_factory=dict)
    indexes: List[IndexDef] = field(default_factory=list)
    foreign_keys: List[ForeignKeyDef] = field(default_factory=list)
    raw_sql: str = ""


@dataclass
class SchemaSnapshot:
    """A complete snapshot of a database schema."""
    source: str
    tables: Dict[str, TableDef] = field(default_factory=dict)

    @property
    def table_names(self) -> List[str]:
        return sorted(self.tables.keys())


# ============================================================
# DIFF RESULT MODELS
# ============================================================

@dataclass
class ColumnChange:
    table: str
    column: str
    change_type: str  # "added", "removed", "modified"
    old_def: Optional[ColumnDef] = None
    new_def: Optional[ColumnDef] = None

    def describe(self) -> str:
        if self.change_type == "added":
            return f"  [+] Column added:    {self.column} {self.new_def.col_type}"
        elif self.change_type == "removed":
            return f"  [-] Column removed:  {self.column} {self.old_def.col_type}"
        else:
            old_sig = self.old_def.signature().split("|")[1:]
            new_sig = self.new_def.signature().split("|")[1:]
            old_str = " ".join(old_sig)
            new_str = " ".join(new_sig)
            return f"  [~] Column modified: {self.column}  ({old_str} -> {new_str})"


@dataclass
class IndexChange:
    table: str
    index_name: str
    change_type: str  # "added", "removed"
    index_def: Optional[IndexDef] = None

    def describe(self) -> str:
        cols = ",".join(self.index_def.columns) if self.index_def else "?"
        unique = " UNIQUE" if (self.index_def and self.index_def.unique) else ""
        if self.change_type == "added":
            return f"  [+] Index added:   {self.index_name}{unique} ({cols})"
        return f"  [-] Index removed: {self.index_name}{unique} ({cols})"


@dataclass
class FKChange:
    table: str
    change_type: str  # "added", "removed"
    fk_def: Optional[ForeignKeyDef] = None

    def describe(self) -> str:
        if not self.fk_def:
            return f"  [?] FK change in {self.table}"
        froms = ",".join(self.fk_def.from_cols)
        tos = ",".join(self.fk_def.to_cols)
        ref = f"{self.fk_def.to_table}({tos})"
        if self.change_type == "added":
            return f"  [+] FK added:   ({froms}) -> {ref}"
        return f"  [-] FK removed: ({froms}) -> {ref}"


@dataclass
class TableDiff:
    """All differences for a single table."""
    table_name: str
    change_type: str  # "added", "removed", "modified"
    column_changes: List[ColumnChange] = field(default_factory=list)
    index_changes: List[IndexChange] = field(default_factory=list)
    fk_changes: List[FKChange] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(self.column_changes or self.index_changes or self.fk_changes)


@dataclass
class SchemaDiff:
    """Complete diff result between two schemas."""
    source_a: str
    source_b: str
    table_diffs: List[TableDiff] = field(default_factory=list)

    @property
    def added_tables(self) -> List[str]:
        return [td.table_name for td in self.table_diffs if td.change_type == "added"]

    @property
    def removed_tables(self) -> List[str]:
        return [td.table_name for td in self.table_diffs if td.change_type == "removed"]

    @property
    def modified_tables(self) -> List[str]:
        return [td.table_name for td in self.table_diffs if td.change_type == "modified" and td.has_changes]

    @property
    def is_identical(self) -> bool:
        return not self.table_diffs or (not self.added_tables and not self.removed_tables and not self.modified_tables)

    @property
    def total_changes(self) -> int:
        count = len(self.added_tables) + len(self.removed_tables)
        for td in self.table_diffs:
            count += len(td.column_changes) + len(td.index_changes) + len(td.fk_changes)
        return count


# ============================================================
# SCHEMA PARSERS
# ============================================================

class SQLiteParser:
    """Parse schema from a live SQLite database file."""

    def parse(self, db_path: Path) -> SchemaSnapshot:
        snapshot = SchemaSnapshot(source=str(db_path))
        conn = sqlite3.connect(str(db_path))
        try:
            cursor = conn.cursor()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            table_names = [row[0] for row in cursor.fetchall()]

            for tname in table_names:
                table = TableDef(name=tname)

                cursor.execute(f"PRAGMA table_info(\"{tname}\")")
                for row in cursor.fetchall():
                    cid, cname, ctype, notnull, dflt, pk = row
                    col = ColumnDef(
                        name=cname,
                        col_type=ctype or "TEXT",
                        not_null=bool(notnull),
                        default_value=dflt,
                        primary_key=bool(pk),
                    )
                    table.columns[cname.lower()] = col

                cursor.execute(f"PRAGMA index_list(\"{tname}\")")
                for idx_row in cursor.fetchall():
                    idx_seq, idx_name, idx_unique = idx_row[0], idx_row[1], idx_row[2]
                    if idx_name.startswith("sqlite_autoindex_"):
                        continue
                    cursor.execute(f"PRAGMA index_info(\"{idx_name}\")")
                    idx_cols = [r[2] for r in cursor.fetchall()]
                    idx = IndexDef(
                        name=idx_name,
                        table=tname,
                        columns=idx_cols,
                        unique=bool(idx_unique),
                    )
                    table.indexes.append(idx)

                cursor.execute(f"PRAGMA foreign_key_list(\"{tname}\")")
                fk_rows = cursor.fetchall()
                fk_groups: Dict[int, List] = {}
                for fk_row in fk_rows:
                    fk_id = fk_row[0]
                    if fk_id not in fk_groups:
                        fk_groups[fk_id] = []
                    fk_groups[fk_id].append(fk_row)

                for fk_id, rows in fk_groups.items():
                    to_table = rows[0][2]
                    from_cols = [r[3] for r in rows]
                    to_cols = [r[4] for r in rows]
                    on_update = rows[0][5] or "NO ACTION"
                    on_delete = rows[0][6] or "NO ACTION"
                    fk = ForeignKeyDef(
                        table=tname,
                        from_cols=from_cols,
                        to_table=to_table,
                        to_cols=to_cols,
                        on_update=on_update,
                        on_delete=on_delete,
                    )
                    table.foreign_keys.append(fk)

                snapshot.tables[tname.lower()] = table
        finally:
            conn.close()
        return snapshot


class SQLDumpParser:
    """Parse schema from a SQL dump file or inline SQL string."""

    # Pattern to match CREATE TABLE statements
    _CREATE_RE = re.compile(
        r"CREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\"']?(\w+)[`\"']?\s*\((.+?)\)\s*;",
        re.IGNORECASE | re.DOTALL,
    )
    _INDEX_RE = re.compile(
        r"CREATE\s+(UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\"']?(\w+)[`\"']?\s+ON\s+[`\"']?(\w+)[`\"']?\s*\(([^)]+)\)\s*;",
        re.IGNORECASE,
    )

    def parse_file(self, sql_path: Path) -> SchemaSnapshot:
        sql_text = sql_path.read_text(encoding="utf-8", errors="replace")
        return self.parse_string(sql_text, source=str(sql_path))

    def parse_string(self, sql_text: str, source: str = "<inline>") -> SchemaSnapshot:
        snapshot = SchemaSnapshot(source=source)

        for m in self._CREATE_RE.finditer(sql_text):
            tname = m.group(1).strip()
            body = m.group(2)
            table = self._parse_table_body(tname, body)
            table.raw_sql = m.group(0)
            snapshot.tables[tname.lower()] = table

        for m in self._INDEX_RE.finditer(sql_text):
            unique_kw, idx_name, tname, cols_str = m.groups()
            cols = [c.strip().strip("`\"'") for c in cols_str.split(",")]
            tname_lower = tname.lower()
            if tname_lower not in snapshot.tables:
                snapshot.tables[tname_lower] = TableDef(name=tname)
            idx = IndexDef(
                name=idx_name,
                table=tname,
                columns=cols,
                unique=bool(unique_kw),
            )
            snapshot.tables[tname_lower].indexes.append(idx)

        return snapshot

    def _parse_table_body(self, tname: str, body: str) -> TableDef:
        table = TableDef(name=tname)
        lines = [ln.strip() for ln in body.split(",")]
        pk_cols_from_constraint: List[str] = []

        for line in lines:
            line = line.strip().rstrip(",").strip()
            if not line:
                continue

            upper = line.upper()

            if upper.startswith("PRIMARY KEY"):
                m = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)", line, re.IGNORECASE)
                if m:
                    pk_cols_from_constraint = [c.strip().strip("`\"'").lower() for c in m.group(1).split(",")]
                continue

            if upper.startswith("UNIQUE") or upper.startswith("FOREIGN KEY") or upper.startswith("CHECK") or upper.startswith("CONSTRAINT"):
                fk_m = re.search(
                    r"FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+[`\"']?(\w+)[`\"']?\s*\(([^)]+)\)"
                    r"(?:\s+ON\s+DELETE\s+(\w+(?:\s+\w+)?))?"
                    r"(?:\s+ON\s+UPDATE\s+(\w+(?:\s+\w+)?))?",
                    line, re.IGNORECASE
                )
                if fk_m:
                    from_cols = [c.strip().strip("`\"'") for c in fk_m.group(1).split(",")]
                    to_table = fk_m.group(2)
                    to_cols = [c.strip().strip("`\"'") for c in fk_m.group(3).split(",")]
                    on_delete = fk_m.group(4) or "NO ACTION"
                    on_update = fk_m.group(5) or "NO ACTION"
                    fk = ForeignKeyDef(
                        table=tname,
                        from_cols=from_cols,
                        to_table=to_table,
                        to_cols=to_cols,
                        on_update=on_update.upper(),
                        on_delete=on_delete.upper(),
                    )
                    table.foreign_keys.append(fk)
                continue

            col = self._parse_column_line(line)
            if col:
                table.columns[col.name.lower()] = col

        for pk_col in pk_cols_from_constraint:
            if pk_col in table.columns:
                table.columns[pk_col].primary_key = True

        return table

    def _parse_column_line(self, line: str) -> Optional[ColumnDef]:
        """Parse a single column definition line."""
        m = re.match(r'[`"\'"]?(\w+)[`"\'"]?\s+(\w+(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)', line)
        if not m:
            return None

        cname = m.group(1)
        ctype = m.group(2).strip()
        upper = line.upper()

        not_null = bool(re.search(r"\bNOT\s+NULL\b", upper))
        primary_key = bool(re.search(r"\bPRIMARY\s+KEY\b", upper))
        autoincrement = bool(re.search(r"\bAUTOINCREMENT\b", upper))

        default_value = None
        dflt_m = re.search(r"\bDEFAULT\s+('(?:[^'\\]|\\.)*'|\S+)", line, re.IGNORECASE)
        if dflt_m:
            default_value = dflt_m.group(1)

        return ColumnDef(
            name=cname,
            col_type=ctype,
            not_null=not_null,
            default_value=default_value,
            primary_key=primary_key,
        )


def auto_parse(source: str) -> SchemaSnapshot:
    """
    Auto-detect source type and parse it.

    Args:
        source: Path to a .db/.sqlite file, path to a .sql file,
                or inline CREATE TABLE SQL string.

    Returns:
        SchemaSnapshot with parsed schema data.

    Raises:
        FileNotFoundError: If a file path is given but doesn't exist.
        ValueError: If the source cannot be parsed.
    """
    if not source:
        raise ValueError("Source cannot be empty")

    path = Path(source)

    if path.exists():
        suffix = path.suffix.lower()
        if suffix in (".db", ".sqlite", ".sqlite3", ""):
            try:
                conn = sqlite3.connect(str(path))
                conn.execute("SELECT 1")
                conn.close()
                return SQLiteParser().parse(path)
            except sqlite3.DatabaseError:
                pass
        return SQLDumpParser().parse_file(path)

    if re.search(r"CREATE\s+TABLE", source, re.IGNORECASE):
        return SQLDumpParser().parse_string(source)

    raise FileNotFoundError(f"Source not found: {source}")


# ============================================================
# DIFF ENGINE
# ============================================================

class DiffEngine:
    """Compare two SchemaSnapshots and produce a SchemaDiff."""

    def diff(self, schema_a: SchemaSnapshot, schema_b: SchemaSnapshot) -> SchemaDiff:
        """
        Compute the diff between schema_a (old) and schema_b (new).

        Returns:
            SchemaDiff with all detected changes.
        """
        result = SchemaDiff(source_a=schema_a.source, source_b=schema_b.source)

        all_tables = set(schema_a.tables.keys()) | set(schema_b.tables.keys())

        for tname in sorted(all_tables):
            in_a = tname in schema_a.tables
            in_b = tname in schema_b.tables

            if in_a and not in_b:
                td = TableDiff(table_name=tname, change_type="removed")
                result.table_diffs.append(td)
            elif not in_a and in_b:
                td = TableDiff(table_name=tname, change_type="added")
                result.table_diffs.append(td)
            else:
                td = self._diff_table(schema_a.tables[tname], schema_b.tables[tname])
                if td.has_changes:
                    result.table_diffs.append(td)

        return result

    def _diff_table(self, table_a: TableDef, table_b: TableDef) -> TableDiff:
        td = TableDiff(table_name=table_a.name, change_type="modified")

        all_cols = set(table_a.columns.keys()) | set(table_b.columns.keys())
        for cname in sorted(all_cols):
            in_a = cname in table_a.columns
            in_b = cname in table_b.columns

            if in_a and not in_b:
                td.column_changes.append(ColumnChange(
                    table=table_a.name, column=cname,
                    change_type="removed", old_def=table_a.columns[cname]
                ))
            elif not in_a and in_b:
                td.column_changes.append(ColumnChange(
                    table=table_a.name, column=cname,
                    change_type="added", new_def=table_b.columns[cname]
                ))
            else:
                col_a = table_a.columns[cname]
                col_b = table_b.columns[cname]
                if col_a.signature() != col_b.signature():
                    td.column_changes.append(ColumnChange(
                        table=table_a.name, column=cname,
                        change_type="modified", old_def=col_a, new_def=col_b
                    ))

        idx_sigs_a = {idx.signature(): idx for idx in table_a.indexes}
        idx_sigs_b = {idx.signature(): idx for idx in table_b.indexes}
        idx_names_a = {idx.name: idx for idx in table_a.indexes}
        idx_names_b = {idx.name: idx for idx in table_b.indexes}

        for sig, idx in idx_sigs_a.items():
            if sig not in idx_sigs_b:
                td.index_changes.append(IndexChange(
                    table=table_a.name, index_name=idx.name,
                    change_type="removed", index_def=idx
                ))

        for sig, idx in idx_sigs_b.items():
            if sig not in idx_sigs_a:
                td.index_changes.append(IndexChange(
                    table=table_a.name, index_name=idx.name,
                    change_type="added", index_def=idx
                ))

        fk_sigs_a = {fk.signature(): fk for fk in table_a.foreign_keys}
        fk_sigs_b = {fk.signature(): fk for fk in table_b.foreign_keys}

        for sig, fk in fk_sigs_a.items():
            if sig not in fk_sigs_b:
                td.fk_changes.append(FKChange(
                    table=table_a.name, change_type="removed", fk_def=fk
                ))

        for sig, fk in fk_sigs_b.items():
            if sig not in fk_sigs_a:
                td.fk_changes.append(FKChange(
                    table=table_a.name, change_type="added", fk_def=fk
                ))

        return td


# ============================================================
# MIGRATION GENERATOR
# ============================================================

class MigrationGenerator:
    """Generate migration SQL from a SchemaDiff."""

    def generate(self, diff: SchemaDiff, schema_b: SchemaSnapshot) -> str:
        """
        Generate SQL statements to migrate from schema_a to schema_b.

        Args:
            diff: The computed SchemaDiff.
            schema_b: The target schema (for full CREATE TABLE on new tables).

        Returns:
            SQL migration script as a string.
        """
        lines = [
            "-- ============================================================",
            "-- SQLSchemaDiff Migration Script",
            f"-- From: {diff.source_a}",
            f"-- To:   {diff.source_b}",
            "-- Generated by: SQLSchemaDiff v1.0.0 (ATLAS / Team Brain)",
            "-- ============================================================",
            "",
            "PRAGMA foreign_keys = OFF;",
            "BEGIN TRANSACTION;",
            "",
        ]

        for tname in diff.added_tables:
            lines.append(f"-- Table added: {tname}")
            table = schema_b.tables.get(tname.lower())
            if table:
                lines.extend(self._create_table_sql(table))
            lines.append("")

        for tname in diff.removed_tables:
            lines.append(f"-- Table removed: {tname}")
            lines.append(f'DROP TABLE IF EXISTS "{tname}";')
            lines.append("")

        for td in diff.table_diffs:
            if td.change_type != "modified" or not td.has_changes:
                continue
            lines.append(f"-- Table modified: {td.table_name}")

            for cc in td.column_changes:
                if cc.change_type == "added":
                    col_sql = cc.new_def.to_sql().strip()
                    lines.append(f'ALTER TABLE "{td.table_name}" ADD COLUMN {col_sql};')
                elif cc.change_type == "removed":
                    lines.append(f'-- DROP COLUMN "{cc.column}" from "{td.table_name}"')
                    lines.append(f'-- NOTE: SQLite does not support DROP COLUMN directly before v3.35.')
                    lines.append(f'-- Recreate the table without this column (see below if needed).')
                elif cc.change_type == "modified":
                    lines.append(f'-- MODIFY COLUMN "{cc.column}" in "{td.table_name}"')
                    lines.append(f'-- NOTE: SQLite does not support ALTER COLUMN.')
                    lines.append(f'-- You must recreate the table. Manual migration required.')

            for ic in td.index_changes:
                if ic.change_type == "removed":
                    lines.append(f'DROP INDEX IF EXISTS "{ic.index_name}";')
                elif ic.change_type == "added" and ic.index_def:
                    unique = "UNIQUE " if ic.index_def.unique else ""
                    cols = ", ".join(f'"{c}"' for c in ic.index_def.columns)
                    lines.append(f'CREATE {unique}INDEX "{ic.index_name}" ON "{td.table_name}" ({cols});')

            lines.append("")

        lines.extend([
            "COMMIT;",
            "PRAGMA foreign_keys = ON;",
            "",
        ])

        return "\n".join(lines)

    def _create_table_sql(self, table: TableDef) -> List[str]:
        """Render a CREATE TABLE statement."""
        col_lines = [col.to_sql() for col in table.columns.values()]
        pk_cols = [col.name for col in table.columns.values() if col.primary_key and not col.primary_key]

        for fk in table.foreign_keys:
            froms = ", ".join(f'"{c}"' for c in fk.from_cols)
            tos = ", ".join(f'"{c}"' for c in fk.to_cols)
            col_lines.append(
                f'    FOREIGN KEY ({froms}) REFERENCES "{fk.to_table}" ({tos})'
                f' ON DELETE {fk.on_delete} ON UPDATE {fk.on_update}'
            )

        body = ",\n".join(col_lines)
        return [
            f'CREATE TABLE IF NOT EXISTS "{table.name}" (',
            body,
            ");",
        ]


# ============================================================
# FORMATTERS
# ============================================================

class TextFormatter:
    """Format a SchemaDiff as human-readable text."""

    def format(self, diff: SchemaDiff) -> str:
        lines = [
            "=" * 65,
            "  SQLSchemaDiff - Schema Comparison Report",
            "=" * 65,
            f"  Schema A: {diff.source_a}",
            f"  Schema B: {diff.source_b}",
            "-" * 65,
        ]

        if diff.is_identical:
            lines += ["", "  [OK] Schemas are IDENTICAL - no differences found.", ""]
            lines.append("=" * 65)
            return "\n".join(lines)

        summary = []
        if diff.added_tables:
            summary.append(f"  [+] {len(diff.added_tables)} table(s) added")
        if diff.removed_tables:
            summary.append(f"  [-] {len(diff.removed_tables)} table(s) removed")
        if diff.modified_tables:
            summary.append(f"  [~] {len(diff.modified_tables)} table(s) modified")
        summary.append(f"  Total changes: {diff.total_changes}")
        lines.extend(summary)
        lines.append("-" * 65)

        for td in diff.table_diffs:
            if td.change_type == "added":
                lines.append(f"\n  [+] TABLE ADDED: {td.table_name}")
            elif td.change_type == "removed":
                lines.append(f"\n  [-] TABLE REMOVED: {td.table_name}")
            elif td.has_changes:
                lines.append(f"\n  [~] TABLE MODIFIED: {td.table_name}")
                for cc in td.column_changes:
                    lines.append(cc.describe())
                for ic in td.index_changes:
                    lines.append(ic.describe())
                for fkc in td.fk_changes:
                    lines.append(fkc.describe())

        lines.extend(["", "=" * 65])
        return "\n".join(lines)


class JsonFormatter:
    """Format a SchemaDiff as JSON."""

    def format(self, diff: SchemaDiff) -> str:
        data = {
            "source_a": diff.source_a,
            "source_b": diff.source_b,
            "identical": diff.is_identical,
            "total_changes": diff.total_changes,
            "added_tables": diff.added_tables,
            "removed_tables": diff.removed_tables,
            "modified_tables": diff.modified_tables,
            "table_diffs": [],
        }

        for td in diff.table_diffs:
            td_data = {
                "table": td.table_name,
                "change_type": td.change_type,
                "column_changes": [],
                "index_changes": [],
                "fk_changes": [],
            }
            for cc in td.column_changes:
                td_data["column_changes"].append({
                    "column": cc.column,
                    "change_type": cc.change_type,
                    "old": {"type": cc.old_def.col_type, "not_null": cc.old_def.not_null} if cc.old_def else None,
                    "new": {"type": cc.new_def.col_type, "not_null": cc.new_def.not_null} if cc.new_def else None,
                })
            for ic in td.index_changes:
                td_data["index_changes"].append({
                    "index": ic.index_name,
                    "change_type": ic.change_type,
                    "unique": ic.index_def.unique if ic.index_def else False,
                    "columns": ic.index_def.columns if ic.index_def else [],
                })
            for fkc in td.fk_changes:
                td_data["fk_changes"].append({
                    "change_type": fkc.change_type,
                    "from": fkc.fk_def.from_cols if fkc.fk_def else [],
                    "to_table": fkc.fk_def.to_table if fkc.fk_def else "",
                    "to": fkc.fk_def.to_cols if fkc.fk_def else [],
                })
            data["table_diffs"].append(td_data)

        return json.dumps(data, indent=2)


class MarkdownFormatter:
    """Format a SchemaDiff as Markdown."""

    def format(self, diff: SchemaDiff) -> str:
        lines = [
            "# SQLSchemaDiff Report",
            "",
            f"**Schema A:** `{diff.source_a}`  ",
            f"**Schema B:** `{diff.source_b}`",
            "",
        ]

        if diff.is_identical:
            lines.append("**Result:** Schemas are IDENTICAL - no differences found.")
            return "\n".join(lines)

        lines.extend([
            "## Summary",
            "",
            f"| Change Type | Count |",
            f"|-------------|-------|",
        ])
        if diff.added_tables:
            lines.append(f"| Tables Added | {len(diff.added_tables)} |")
        if diff.removed_tables:
            lines.append(f"| Tables Removed | {len(diff.removed_tables)} |")
        if diff.modified_tables:
            lines.append(f"| Tables Modified | {len(diff.modified_tables)} |")
        lines.append(f"| **Total Changes** | **{diff.total_changes}** |")
        lines.append("")

        for td in diff.table_diffs:
            if td.change_type == "added":
                lines.append(f"## `{td.table_name}` - ADDED")
            elif td.change_type == "removed":
                lines.append(f"## `{td.table_name}` - REMOVED")
            elif td.has_changes:
                lines.append(f"## `{td.table_name}` - MODIFIED")
                for cc in td.column_changes:
                    lines.append(f"- {cc.describe().strip()}")
                for ic in td.index_changes:
                    lines.append(f"- {ic.describe().strip()}")
                for fkc in td.fk_changes:
                    lines.append(f"- {fkc.describe().strip()}")
            lines.append("")

        return "\n".join(lines)


def get_formatter(fmt: str):
    formats = {"text": TextFormatter, "json": JsonFormatter, "markdown": MarkdownFormatter}
    cls = formats.get(fmt.lower(), TextFormatter)
    return cls()


# ============================================================
# COMMANDS
# ============================================================

def cmd_diff(args: argparse.Namespace) -> int:
    """Compare two schemas and show differences."""
    try:
        schema_a = auto_parse(args.schema_a)
    except Exception as e:
        print(f"[X] Error reading Schema A ({args.schema_a}): {e}")
        return 1

    try:
        schema_b = auto_parse(args.schema_b)
    except Exception as e:
        print(f"[X] Error reading Schema B ({args.schema_b}): {e}")
        return 1

    engine = DiffEngine()
    diff = engine.diff(schema_a, schema_b)

    formatter = get_formatter(args.format)
    output = formatter.format(diff)
    print(output)

    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")
        print(f"[OK] Report saved to: {args.output}")

    if args.fail_on_diff and not diff.is_identical:
        return 2

    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    """Generate migration SQL to bring schema_a to schema_b."""
    try:
        schema_a = auto_parse(args.schema_a)
    except Exception as e:
        print(f"[X] Error reading Schema A: {e}")
        return 1

    try:
        schema_b = auto_parse(args.schema_b)
    except Exception as e:
        print(f"[X] Error reading Schema B: {e}")
        return 1

    engine = DiffEngine()
    diff = engine.diff(schema_a, schema_b)

    if diff.is_identical:
        print("[OK] Schemas are identical - no migration needed.")
        return 0

    generator = MigrationGenerator()
    migration_sql = generator.generate(diff, schema_b)

    if args.output:
        Path(args.output).write_text(migration_sql, encoding="utf-8")
        print(f"[OK] Migration script saved to: {args.output}")
        print(f"     Changes: {diff.total_changes}")
    else:
        print(migration_sql)

    return 0


def cmd_inspect(args: argparse.Namespace) -> int:
    """Inspect a single schema and display its structure."""
    try:
        schema = auto_parse(args.source)
    except Exception as e:
        print(f"[X] Error reading schema: {e}")
        return 1

    if args.format == "json":
        data = {}
        for tname, table in schema.tables.items():
            data[tname] = {
                "columns": {c: {"type": col.col_type, "pk": col.primary_key, "not_null": col.not_null} for c, col in table.columns.items()},
                "indexes": [{"name": idx.name, "columns": idx.columns, "unique": idx.unique} for idx in table.indexes],
                "foreign_keys": [{"from": fk.from_cols, "to_table": fk.to_table, "to": fk.to_cols} for fk in table.foreign_keys],
            }
        print(json.dumps(data, indent=2))
        return 0

    print("=" * 65)
    print(f"  Schema: {schema.source}")
    print(f"  Tables: {len(schema.tables)}")
    print("=" * 65)

    for tname, table in sorted(schema.tables.items()):
        print(f"\n  TABLE: {table.name}")
        print(f"  {'Column':<30} {'Type':<20} {'PK':<6} {'NN':<6} {'Default'}")
        print(f"  {'-'*30} {'-'*20} {'-'*6} {'-'*6} {'-'*15}")
        for cname, col in table.columns.items():
            pk_str = "[PK]" if col.primary_key else ""
            nn_str = "[NN]" if col.not_null else ""
            dflt = col.default_value or ""
            print(f"  {col.name:<30} {col.col_type:<20} {pk_str:<6} {nn_str:<6} {dflt}")

        if table.indexes:
            print()
            for idx in table.indexes:
                uniq = "UNIQUE " if idx.unique else ""
                cols = ", ".join(idx.columns)
                print(f"  [IDX] {idx.name}: {uniq}({cols})")

        if table.foreign_keys:
            print()
            for fk in table.foreign_keys:
                froms = ", ".join(fk.from_cols)
                tos = ", ".join(fk.to_cols)
                print(f"  [FK] ({froms}) -> {fk.to_table}({tos})")

    print("\n" + "=" * 65)
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    """
    Validate a migration SQL file: apply it to schema_a and check
    that the result matches schema_b.
    """
    try:
        schema_a = auto_parse(args.schema_a)
    except Exception as e:
        print(f"[X] Error reading Schema A: {e}")
        return 1

    try:
        schema_b = auto_parse(args.schema_b)
    except Exception as e:
        print(f"[X] Error reading Schema B: {e}")
        return 1

    migration_path = Path(args.migration)
    if not migration_path.exists():
        print(f"[X] Migration file not found: {args.migration}")
        return 1

    migration_sql = migration_path.read_text(encoding="utf-8")

    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_a:
        tmp_path = tmp_a.name

    try:
        conn = sqlite3.connect(tmp_path)
        for tname, table in schema_a.tables.items():
            if table.raw_sql:
                try:
                    conn.execute(table.raw_sql)
                except Exception:
                    pass
        conn.commit()

        try:
            conn.executescript(migration_sql)
            conn.commit()
        except Exception as e:
            print(f"[X] Migration SQL failed to execute: {e}")
            conn.close()
            return 1

        conn.close()

        migrated_schema = SQLiteParser().parse(Path(tmp_path))
        engine = DiffEngine()
        residual_diff = engine.diff(migrated_schema, schema_b)

        if residual_diff.is_identical:
            print("[OK] Migration VALID - applying it produces schema_b exactly.")
            return 0
        else:
            print("[X] Migration INVALID - residual differences remain after applying migration:")
            formatter = TextFormatter()
            print(formatter.format(residual_diff))
            return 2

    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def cmd_demo(args: argparse.Namespace) -> int:
    """Run a built-in demonstration with sample schemas."""
    schema_a_sql = """
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT,
    created_at TEXT DEFAULT 'now'
);
CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    title TEXT NOT NULL,
    body TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
CREATE INDEX idx_users_email ON users (email);
"""

    schema_b_sql = """
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TEXT DEFAULT 'now',
    updated_at TEXT,
    is_active INTEGER DEFAULT 1
);
CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    title TEXT NOT NULL,
    body TEXT,
    published INTEGER DEFAULT 0,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE TABLE comments (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    body TEXT NOT NULL,
    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
CREATE INDEX idx_users_email ON users (email);
CREATE UNIQUE INDEX idx_users_username ON users (username);
CREATE INDEX idx_posts_user ON posts (user_id);
"""

    print("=" * 65)
    print("  SQLSchemaDiff DEMO - Showing schema evolution")
    print("=" * 65)
    print("\n  [Schema A] Initial schema (v1.0)")
    print("  [Schema B] Updated schema (v2.0)\n")

    schema_a = SQLDumpParser().parse_string(schema_a_sql, source="schema_v1.sql")
    schema_b = SQLDumpParser().parse_string(schema_b_sql, source="schema_v2.sql")

    engine = DiffEngine()
    diff = engine.diff(schema_a, schema_b)

    formatter = TextFormatter()
    print(formatter.format(diff))

    print("\n  [MIGRATION SQL PREVIEW]")
    print("-" * 65)
    generator = MigrationGenerator()
    migration = generator.generate(diff, schema_b)
    for line in migration.split("\n")[:30]:
        print(f"  {line}")
    if migration.count("\n") > 30:
        print("  ... (truncated - use 'migrate' command for full output)")

    return 0


# ============================================================
# CLI ENTRY POINT
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sqlschemadiff",
        description="SQLSchemaDiff v1.0.0 - SQL Schema Comparison & Migration Validator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Commands:
              diff      Compare two schemas and show differences
              migrate   Generate migration SQL from schema A to schema B
              inspect   Inspect a single schema structure
              validate  Validate a migration file is correct
              demo      Run a built-in demonstration

            Schema Sources (auto-detected):
              *.db / *.sqlite   Live SQLite database file
              *.sql             SQL dump file with CREATE TABLE statements
              "CREATE TABLE..." Inline SQL string

            Examples:
              sqlschemadiff diff dev.db prod.db
              sqlschemadiff diff schema_v1.sql schema_v2.sql --format json
              sqlschemadiff migrate old.db new.db --output migration.sql
              sqlschemadiff inspect mydb.db
              sqlschemadiff validate old.sql new.sql migration.sql
              sqlschemadiff demo

            More: https://github.com/DonkRonk17/SQLSchemaDiff
        """),
    )
    parser.add_argument("--version", action="version", version="SQLSchemaDiff 1.0.0")

    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")

    # diff
    p_diff = subparsers.add_parser("diff", help="Compare two schemas")
    p_diff.add_argument("schema_a", help="First schema (old/baseline)")
    p_diff.add_argument("schema_b", help="Second schema (new/target)")
    p_diff.add_argument("--format", choices=["text", "json", "markdown"], default="text", help="Output format (default: text)")
    p_diff.add_argument("--output", "-o", help="Save report to file")
    p_diff.add_argument("--fail-on-diff", action="store_true", help="Exit code 2 if schemas differ (for CI/CD)")

    # migrate
    p_mig = subparsers.add_parser("migrate", help="Generate migration SQL")
    p_mig.add_argument("schema_a", help="Source schema (current state)")
    p_mig.add_argument("schema_b", help="Target schema (desired state)")
    p_mig.add_argument("--output", "-o", help="Save migration SQL to file")

    # inspect
    p_insp = subparsers.add_parser("inspect", help="Inspect a schema's structure")
    p_insp.add_argument("source", help="Schema to inspect")
    p_insp.add_argument("--format", choices=["text", "json"], default="text")

    # validate
    p_val = subparsers.add_parser("validate", help="Validate a migration file")
    p_val.add_argument("schema_a", help="Source schema")
    p_val.add_argument("schema_b", help="Target schema")
    p_val.add_argument("migration", help="Migration SQL file to validate")

    # demo
    subparsers.add_parser("demo", help="Run built-in demonstration")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    commands = {
        "diff": cmd_diff,
        "migrate": cmd_migrate,
        "inspect": cmd_inspect,
        "validate": cmd_validate,
        "demo": cmd_demo,
    }

    handler = commands.get(args.command)
    if not handler:
        print(f"[X] Unknown command: {args.command}")
        return 1

    return handler(args)


if __name__ == "__main__":
    sys.exit(main())
