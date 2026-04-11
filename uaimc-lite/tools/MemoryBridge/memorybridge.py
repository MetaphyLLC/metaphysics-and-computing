#!/usr/bin/env python3
"""
MemoryBridge v1.0 - Cross-Agent Shared Memory API

Universal memory sharing system for Team Brain. Share memories, session data,
and knowledge across all AI agents with simple API.

No more isolated memories - true team coordination through shared knowledge!

Author: Atlas (Team Brain)
Requested by: Forge  
Date: January 18, 2026
"""

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, asdict
from datetime import datetime
import hashlib

VERSION = "1.0.0"

# Default database path
DEFAULT_DB_PATH = Path("D:/BEACON_HQ/MEMORY_CORE_V2/00_SHARED_MEMORY/memory_bridge.db")


@dataclass
class Memory:
    """Represents a shared memory entry."""
    key: str
    value: Any
    scope: str  # "agent", "team", "global"
    owner: str  # Agent who created it
    created: str
    updated: str
    access_count: int = 0
    metadata: Dict[str, Any] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


class MemoryBridge:
    """
    Cross-agent shared memory system.
    
    Usage:
        bridge = MemoryBridge(agent_name="ATLAS")
        
        # Store memory
        bridge.store("last_task", "Built ScreenSnap", scope="agent")
        bridge.store("team_status", "All systems operational", scope="team")
        
        # Retrieve memory
        value = bridge.get("last_task")  # From any agent
        value = bridge.get("team_status", scope="team")
        
        # Search memories
        results = bridge.search("ScreenSnap")
    """
    
    def __init__(self, agent_name: str, db_path: Optional[Path] = None):
        """
        Initialize MemoryBridge.
        
        Args:
            agent_name: Name of current agent (ATLAS, FORGE, etc.)
            db_path: Path to shared database file
        """
        self.agent_name = agent_name.upper()
        self.db_path = db_path or DEFAULT_DB_PATH
        
        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize database
        self._init_database()
    
    def _init_database(self):
        """Initialize the shared memory database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS memories (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                value_type TEXT NOT NULL,
                scope TEXT NOT NULL,
                owner TEXT NOT NULL,
                created TEXT NOT NULL,
                updated TEXT NOT NULL,
                access_count INTEGER DEFAULT 0,
                metadata_json TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_scope ON memories(scope)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_owner ON memories(owner)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_updated ON memories(updated)
        ''')
        
        conn.commit()
        conn.close()
    
    def store(self,
              key: str,
              value: Any,
              scope: str = "agent",
              metadata: Optional[Dict] = None) -> bool:
        """
        Store a memory.
        
        Args:
            key: Unique key for memory
            value: Value to store (any JSON-serializable type)
            scope: "agent" (private), "team" (shared), "global" (all)
            metadata: Optional metadata dict
        
        Returns:
            True if stored successfully
        """
        # Validate inputs
        if not key or not isinstance(key, str):
            raise ValueError("Key must be a non-empty string")
        
        if scope not in ["agent", "team", "global"]:
            raise ValueError("Scope must be 'agent', 'team', or 'global'")
        
        # For agent-scoped memories, prefix with agent name
        if scope == "agent":
            key = f"{self.agent_name}:{key}"
        
        # Serialize value
        try:
            value_json = json.dumps(value)
            value_type = type(value).__name__
        except (TypeError, ValueError) as e:
            raise ValueError(f"Value must be JSON-serializable: {e}")
        
        # Serialize metadata
        metadata_json = json.dumps(metadata) if metadata else None
        
        # Get timestamp
        now = datetime.now().isoformat()
        
        # Store in database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO memories (key, value_json, value_type, scope, owner, created, updated, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    value_type = excluded.value_type,
                    updated = excluded.updated,
                    metadata_json = excluded.metadata_json
            ''', (key, value_json, value_type, scope, self.agent_name, now, now, metadata_json))
            
            conn.commit()
            return True
        
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return False
        
        finally:
            conn.close()
    
    def get(self,
            key: str,
            scope: Optional[str] = None,
            default: Any = None) -> Any:
        """
        Retrieve a memory.
        
        Args:
            key: Memory key
            scope: Optional scope filter ("agent", "team", "global")
            default: Default value if not found
        
        Returns:
            Memory value or default
        """
        # For agent scope, try prefixed key first
        if scope == "agent" or scope is None:
            prefixed_key = f"{self.agent_name}:{key}"
        else:
            prefixed_key = key
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Try exact match first
            cursor.execute('''
                SELECT value_json, value_type FROM memories WHERE key = ?
            ''', (prefixed_key,))
            
            row = cursor.fetchone()
            
            # If not found and no scope specified, try unprefixed
            if not row and scope is None:
                cursor.execute('''
                    SELECT value_json, value_type FROM memories WHERE key = ?
                ''', (key,))
                row = cursor.fetchone()
            
            if row:
                # Increment access count
                cursor.execute('''
                    UPDATE memories SET access_count = access_count + 1 WHERE key = ? OR key = ?
                ''', (prefixed_key, key))
                conn.commit()
                
                # Deserialize value
                value_json, value_type = row
                return json.loads(value_json)
            
            return default
        
        finally:
            conn.close()
    
    def search(self, query: str, scope: Optional[str] = None) -> List[Memory]:
        """
        Search memories by keyword.
        
        Args:
            query: Search term (matches key or value)
            scope: Optional scope filter
        
        Returns:
            List of matching Memory objects
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            sql = '''
                SELECT key, value_json, value_type, scope, owner, created, updated, access_count, metadata_json
                FROM memories
                WHERE (key LIKE ? OR value_json LIKE ?)
            '''
            params = [f'%{query}%', f'%{query}%']
            
            if scope:
                sql += ' AND scope = ?'
                params.append(scope)
            
            sql += ' ORDER BY updated DESC'
            
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                key, value_json, value_type, scope, owner, created, updated, access_count, metadata_json = row
                
                results.append(Memory(
                    key=key,
                    value=json.loads(value_json),
                    scope=scope,
                    owner=owner,
                    created=created,
                    updated=updated,
                    access_count=access_count,
                    metadata=json.loads(metadata_json) if metadata_json else None
                ))
            
            return results
        
        finally:
            conn.close()
    
    def delete(self, key: str, scope: Optional[str] = None) -> bool:
        """Delete a memory."""
        if scope == "agent":
            key = f"{self.agent_name}:{key}"
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('DELETE FROM memories WHERE key = ?', (key,))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()
    
    def list_all(self, scope: Optional[str] = None, owner: Optional[str] = None) -> List[Memory]:
        """
        List all memories.
        
        Args:
            scope: Optional scope filter
            owner: Optional owner filter
        
        Returns:
            List of Memory objects
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            sql = 'SELECT key, value_json, value_type, scope, owner, created, updated, access_count, metadata_json FROM memories WHERE 1=1'
            params = []
            
            if scope:
                sql += ' AND scope = ?'
                params.append(scope)
            
            if owner:
                sql += ' AND owner = ?'
                params.append(owner)
            
            sql += ' ORDER BY updated DESC'
            
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                key, value_json, value_type, scope, owner, created, updated, access_count, metadata_json = row
                
                results.append(Memory(
                    key=key,
                    value=json.loads(value_json),
                    scope=scope,
                    owner=owner,
                    created=created,
                    updated=updated,
                    access_count=access_count,
                    metadata=json.loads(metadata_json) if metadata_json else None
                ))
            
            return results
        
        finally:
            conn.close()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get memory statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT COUNT(*) FROM memories')
            total = cursor.fetchone()[0]
            
            cursor.execute('SELECT scope, COUNT(*) FROM memories GROUP BY scope')
            by_scope = dict(cursor.fetchall())
            
            cursor.execute('SELECT owner, COUNT(*) FROM memories GROUP BY owner')
            by_owner = dict(cursor.fetchall())
            
            cursor.execute('SELECT SUM(access_count) FROM memories')
            total_accesses = cursor.fetchone()[0] or 0
            
            return {
                "total_memories": total,
                "by_scope": by_scope,
                "by_owner": by_owner,
                "total_accesses": total_accesses
            }
        
        finally:
            conn.close()


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="MemoryBridge - Cross-agent shared memory",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('command', choices=['store', 'get', 'search', 'list', 'delete', 'stats'],
                        help='Command to execute')
    parser.add_argument('--agent', required=True, help='Agent name (ATLAS, FORGE, etc.)')
    parser.add_argument('--key', help='Memory key')
    parser.add_argument('--value', help='Memory value (JSON)')
    parser.add_argument('--scope', choices=['agent', 'team', 'global'], default='agent',
                        help='Memory scope')
    parser.add_argument('--query', help='Search query')
    parser.add_argument('--version', action='version', version=f'MemoryBridge {VERSION}')
    
    args = parser.parse_args()
    
    bridge = MemoryBridge(agent_name=args.agent)
    
    if args.command == 'store':
        if not args.key or not args.value:
            print("ERROR: --key and --value required for store")
            return 1
        
        value = json.loads(args.value)
        success = bridge.store(args.key, value, scope=args.scope)
        print(f"[{'OK' if success else 'FAIL'}] Memory stored: {args.key}")
    
    elif args.command == 'get':
        if not args.key:
            print("ERROR: --key required for get")
            return 1
        
        value = bridge.get(args.key, scope=args.scope)
        if value is not None:
            print(json.dumps(value, indent=2))
        else:
            print(f"Memory not found: {args.key}")
            return 1
    
    elif args.command == 'search':
        if not args.query:
            print("ERROR: --query required for search")
            return 1
        
        results = bridge.search(args.query, scope=args.scope)
        print(f"Found {len(results)} memories:")
        for mem in results:
            print(f"  {mem.key} ({mem.scope}) by {mem.owner}")
    
    elif args.command == 'list':
        memories = bridge.list_all(scope=args.scope)
        print(f"Total memories: {len(memories)}")
        for mem in memories:
            print(f"  {mem.key} ({mem.scope}) by {mem.owner} - accessed {mem.access_count} times")
    
    elif args.command == 'delete':
        if not args.key:
            print("ERROR: --key required for delete")
            return 1
        
        success = bridge.delete(args.key, scope=args.scope)
        print(f"[{'OK' if success else 'FAIL'}] Memory deleted: {args.key}")
    
    elif args.command == 'stats':
        stats = bridge.get_stats()
        print("MEMORY STATISTICS:")
        print(f"  Total memories: {stats['total_memories']}")
        print(f"  Total accesses: {stats['total_accesses']}")
        print(f"  By scope: {stats['by_scope']}")
        print(f"  By owner: {stats['by_owner']}")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
