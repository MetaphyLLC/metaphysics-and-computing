#!/usr/bin/env python3
"""
KnowledgeSync - Cross-Agent Knowledge Synchronization for Team Brain

Automatically extracts, stores, and synchronizes knowledge across AI agents.
Ensures all agents are aware of the latest decisions, findings, and learnings
within the Team Brain ecosystem.

Features:
- Knowledge Extraction: Parse sessions/bookmarks for key facts
- Knowledge Graph: Store relationships between concepts
- Cross-Agent Sync: Push/pull updates between agents
- Conflict Resolution: Timestamp-based with manual override
- Query System: Ask "what does FORGE know about X?"
- Subscriptions: Get notified when specific topics update

Author: Forge (Team Brain)
For: Logan Smith / Metaphy LLC
Version: 1.0.0
Date: January 21, 2026
License: MIT
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

# ============================================================================
# CONFIGURATION
# ============================================================================

DEFAULT_KNOWLEDGE_DIR = Path.home() / ".knowledgesync"
MEMORY_CORE_PATH = Path("D:/BEACON_HQ/MEMORY_CORE_V2/04_KNOWLEDGE_BASE")
SYNAPSE_PATH = Path("D:/BEACON_HQ/MEMORY_CORE_V2/03_INTER_AI_COMMS/THE_SYNAPSE/active")

# Valid agent names in Team Brain
VALID_AGENTS = {"FORGE", "ATLAS", "CLIO", "NEXUS", "BOLT", "LOGAN", "SYSTEM"}

# Knowledge categories
CATEGORIES = {
    "DECISION": "Decision made",
    "FINDING": "Discovery or finding",
    "PROBLEM": "Problem identified",
    "SOLUTION": "Solution implemented",
    "TODO": "Task or action item",
    "REFERENCE": "Reference or documentation",
    "CONFIG": "Configuration or setting",
    "RELATIONSHIP": "Relationship between concepts",
    "FACT": "General fact",
    "INSIGHT": "Insight or observation",
}

# Confidence levels
CONFIDENCE_LEVELS = {
    "CERTAIN": 1.0,      # Verified, 100% confident
    "HIGH": 0.8,         # Very likely correct
    "MEDIUM": 0.6,       # Probably correct
    "LOW": 0.4,          # May be correct
    "UNCERTAIN": 0.2,    # Uncertain, needs verification
}


# ============================================================================
# DATA STRUCTURES
# ============================================================================

class KnowledgeEntry:
    """
    Represents a single piece of knowledge.
    
    Attributes:
        entry_id: Unique identifier (hash of content + source)
        content: The knowledge content
        source: Agent that created this entry
        category: Type of knowledge (DECISION, FINDING, etc.)
        topics: List of related topics/tags
        confidence: Confidence level (0.0 to 1.0)
        created: Creation timestamp
        updated: Last update timestamp
        expires: Optional expiration timestamp
        references: Related entry IDs
        metadata: Additional metadata
    """
    
    def __init__(
        self,
        content: str,
        source: str,
        category: str = "FACT",
        topics: Optional[List[str]] = None,
        confidence: float = 0.8,
        expires: Optional[datetime] = None,
        references: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        entry_id: Optional[str] = None,
        created: Optional[datetime] = None,
        updated: Optional[datetime] = None
    ):
        self.content = content
        self.source = source.upper()
        self.category = category.upper() if category.upper() in CATEGORIES else "FACT"
        self.topics = [t.lower() for t in (topics or [])]
        self.confidence = min(1.0, max(0.0, confidence))
        self.created = created or datetime.now()
        self.updated = updated or self.created
        self.expires = expires
        self.references = references or []
        self.metadata = metadata or {}
        
        # Generate ID if not provided
        if entry_id:
            self.entry_id = entry_id
        else:
            # Hash based on content + source + created time for uniqueness
            hash_input = f"{content}:{source}:{self.created.isoformat()}"
            self.entry_id = hashlib.sha256(hash_input.encode()).hexdigest()[:16]
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "entry_id": self.entry_id,
            "content": self.content,
            "source": self.source,
            "category": self.category,
            "topics": self.topics,
            "confidence": self.confidence,
            "created": self.created.isoformat(),
            "updated": self.updated.isoformat(),
            "expires": self.expires.isoformat() if self.expires else None,
            "references": self.references,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'KnowledgeEntry':
        """Create from dictionary."""
        return cls(
            entry_id=data.get("entry_id"),
            content=data["content"],
            source=data["source"],
            category=data.get("category", "FACT"),
            topics=data.get("topics", []),
            confidence=data.get("confidence", 0.8),
            created=datetime.fromisoformat(data["created"]) if data.get("created") else None,
            updated=datetime.fromisoformat(data["updated"]) if data.get("updated") else None,
            expires=datetime.fromisoformat(data["expires"]) if data.get("expires") else None,
            references=data.get("references", []),
            metadata=data.get("metadata", {})
        )
    
    def is_expired(self) -> bool:
        """Check if this entry has expired."""
        if self.expires is None:
            return False
        return datetime.now() > self.expires
    
    def matches_query(self, query: str) -> bool:
        """Check if this entry matches a search query."""
        query_lower = query.lower()
        
        # Check content
        if query_lower in self.content.lower():
            return True
        
        # Check topics
        for topic in self.topics:
            if query_lower in topic:
                return True
        
        # Check category
        if query_lower in self.category.lower():
            return True
        
        return False
    
    def __repr__(self) -> str:
        return f"KnowledgeEntry(id={self.entry_id[:8]}, source={self.source}, category={self.category})"


class KnowledgeGraph:
    """
    Graph structure for storing relationships between knowledge entries.
    
    Nodes represent concepts/topics.
    Edges represent relationships between concepts.
    """
    
    def __init__(self):
        self.nodes: Dict[str, Dict[str, Any]] = {}  # topic -> metadata
        self.edges: List[Dict[str, Any]] = []  # list of {source, target, relation, weight}
    
    def add_node(self, topic: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Add a node (topic) to the graph."""
        topic = topic.lower()
        if topic not in self.nodes:
            self.nodes[topic] = {
                "created": datetime.now().isoformat(),
                "references": 0,
                **(metadata or {})
            }
        self.nodes[topic]["references"] = self.nodes[topic].get("references", 0) + 1
    
    def add_edge(
        self,
        source: str,
        target: str,
        relation: str = "related_to",
        weight: float = 1.0
    ) -> None:
        """Add an edge (relationship) between two nodes."""
        source = source.lower()
        target = target.lower()
        
        # Ensure nodes exist
        self.add_node(source)
        self.add_node(target)
        
        # Check if edge already exists
        for edge in self.edges:
            if edge["source"] == source and edge["target"] == target:
                # Update existing edge
                edge["weight"] = max(edge["weight"], weight)
                edge["relation"] = relation
                return
        
        # Add new edge
        self.edges.append({
            "source": source,
            "target": target,
            "relation": relation,
            "weight": weight,
            "created": datetime.now().isoformat()
        })
    
    def get_related(self, topic: str, depth: int = 1) -> Set[str]:
        """Get all related topics up to a certain depth."""
        topic = topic.lower()
        if topic not in self.nodes:
            return set()
        
        related = set()
        visited = {topic}
        current_level = {topic}
        
        for _ in range(depth):
            next_level = set()
            for node in current_level:
                for edge in self.edges:
                    if edge["source"] == node and edge["target"] not in visited:
                        next_level.add(edge["target"])
                    elif edge["target"] == node and edge["source"] not in visited:
                        next_level.add(edge["source"])
            related.update(next_level)
            visited.update(next_level)
            current_level = next_level
            if not current_level:
                break
        
        return related
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "nodes": self.nodes,
            "edges": self.edges
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'KnowledgeGraph':
        """Create from dictionary."""
        graph = cls()
        graph.nodes = data.get("nodes", {})
        graph.edges = data.get("edges", [])
        return graph


# ============================================================================
# MAIN CLASS
# ============================================================================

class KnowledgeSync:
    """
    Main interface for knowledge synchronization.
    
    Provides methods to:
    - Add/update/delete knowledge entries
    - Query knowledge by topic, source, or content
    - Sync knowledge between agents
    - Extract knowledge from session logs
    - Manage the knowledge graph
    
    Example:
        >>> ks = KnowledgeSync("ATLAS")
        >>> ks.add("TokenTracker uses ~$0.50/day on average", 
        ...        category="FINDING", topics=["tokentracker", "costs"])
        >>> ks.query("tokentracker")
        [KnowledgeEntry(...)]
    """
    
    def __init__(
        self,
        agent: str = "SYSTEM",
        storage_dir: Optional[Path] = None,
        auto_sync: bool = True
    ):
        """
        Initialize KnowledgeSync.
        
        Args:
            agent: Name of the current agent
            storage_dir: Directory for knowledge storage
            auto_sync: Whether to auto-sync on changes
        """
        self.agent = agent.upper()
        self.storage_dir = Path(storage_dir) if storage_dir else self._get_storage_dir()
        self.auto_sync = auto_sync
        
        # Initialize storage
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        # Load or create knowledge base
        self.entries: Dict[str, KnowledgeEntry] = {}
        self.graph = KnowledgeGraph()
        self.subscriptions: Dict[str, List[Callable]] = {}  # topic -> callbacks
        self._sync_log: List[Dict] = []
        
        self._load()
    
    def _get_storage_dir(self) -> Path:
        """Get the appropriate storage directory."""
        # Prefer MEMORY_CORE path if it exists
        if MEMORY_CORE_PATH.exists():
            return MEMORY_CORE_PATH / "knowledge_sync"
        return DEFAULT_KNOWLEDGE_DIR
    
    def _get_entries_file(self) -> Path:
        """Get path to entries file."""
        return self.storage_dir / "entries.json"
    
    def _get_graph_file(self) -> Path:
        """Get path to graph file."""
        return self.storage_dir / "graph.json"
    
    def _get_sync_log_file(self) -> Path:
        """Get path to sync log file."""
        return self.storage_dir / "sync_log.json"
    
    def _load(self) -> None:
        """Load knowledge base from storage."""
        # Load entries
        entries_file = self._get_entries_file()
        if entries_file.exists():
            try:
                with open(entries_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for entry_data in data.get("entries", []):
                        entry = KnowledgeEntry.from_dict(entry_data)
                        if not entry.is_expired():
                            self.entries[entry.entry_id] = entry
            except (json.JSONDecodeError, KeyError) as e:
                print(f"[!] Warning: Could not load entries: {e}")
        
        # Load graph
        graph_file = self._get_graph_file()
        if graph_file.exists():
            try:
                with open(graph_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.graph = KnowledgeGraph.from_dict(data)
            except (json.JSONDecodeError, KeyError) as e:
                print(f"[!] Warning: Could not load graph: {e}")
        
        # Load sync log
        sync_log_file = self._get_sync_log_file()
        if sync_log_file.exists():
            try:
                with open(sync_log_file, 'r', encoding='utf-8') as f:
                    self._sync_log = json.load(f)
            except (json.JSONDecodeError, KeyError):
                self._sync_log = []
    
    def _save(self) -> None:
        """Save knowledge base to storage."""
        # Save entries
        entries_file = self._get_entries_file()
        with open(entries_file, 'w', encoding='utf-8') as f:
            json.dump({
                "version": "1.0",
                "updated": datetime.now().isoformat(),
                "agent": self.agent,
                "entries": [e.to_dict() for e in self.entries.values()]
            }, f, indent=2)
        
        # Save graph
        graph_file = self._get_graph_file()
        with open(graph_file, 'w', encoding='utf-8') as f:
            json.dump(self.graph.to_dict(), f, indent=2)
        
        # Save sync log (keep last 100)
        sync_log_file = self._get_sync_log_file()
        with open(sync_log_file, 'w', encoding='utf-8') as f:
            json.dump(self._sync_log[-100:], f, indent=2)
    
    # ========================================================================
    # CRUD OPERATIONS
    # ========================================================================
    
    def add(
        self,
        content: str,
        category: str = "FACT",
        topics: Optional[List[str]] = None,
        confidence: float = 0.8,
        expires_in_days: Optional[int] = None,
        references: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> KnowledgeEntry:
        """
        Add a new knowledge entry.
        
        Args:
            content: The knowledge content
            category: Type (DECISION, FINDING, PROBLEM, SOLUTION, etc.)
            topics: Related topics/tags
            confidence: Confidence level (0.0 to 1.0) or use CONFIDENCE_LEVELS
            expires_in_days: Optional expiration in days
            references: Related entry IDs
            metadata: Additional metadata
        
        Returns:
            The created KnowledgeEntry
        
        Example:
            >>> ks.add("BCH uses port 8080 for web interface",
            ...        category="CONFIG", 
            ...        topics=["bch", "ports", "configuration"])
        """
        if not content or not content.strip():
            raise ValueError("Content cannot be empty")
        
        expires = None
        if expires_in_days:
            expires = datetime.now() + timedelta(days=expires_in_days)
        
        entry = KnowledgeEntry(
            content=content.strip(),
            source=self.agent,
            category=category,
            topics=topics or [],
            confidence=confidence,
            expires=expires,
            references=references,
            metadata=metadata
        )
        
        self.entries[entry.entry_id] = entry
        
        # Update graph with topics
        for topic in entry.topics:
            self.graph.add_node(topic)
        
        # Create edges between topics
        for i, topic1 in enumerate(entry.topics):
            for topic2 in entry.topics[i+1:]:
                self.graph.add_edge(topic1, topic2, "co-occurs")
        
        # Trigger subscriptions
        self._notify_subscriptions(entry)
        
        if self.auto_sync:
            self._save()
        
        return entry
    
    def update(
        self,
        entry_id: str,
        content: Optional[str] = None,
        category: Optional[str] = None,
        topics: Optional[List[str]] = None,
        confidence: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[KnowledgeEntry]:
        """
        Update an existing knowledge entry.
        
        Args:
            entry_id: ID of entry to update
            content: New content (optional)
            category: New category (optional)
            topics: New topics (optional)
            confidence: New confidence (optional)
            metadata: Additional metadata to merge (optional)
        
        Returns:
            Updated entry or None if not found
        """
        if entry_id not in self.entries:
            return None
        
        entry = self.entries[entry_id]
        
        if content is not None:
            entry.content = content.strip()
        if category is not None:
            entry.category = category.upper()
        if topics is not None:
            entry.topics = [t.lower() for t in topics]
        if confidence is not None:
            entry.confidence = min(1.0, max(0.0, confidence))
        if metadata is not None:
            entry.metadata.update(metadata)
        
        entry.updated = datetime.now()
        
        # Trigger subscriptions
        self._notify_subscriptions(entry)
        
        if self.auto_sync:
            self._save()
        
        return entry
    
    def delete(self, entry_id: str) -> bool:
        """
        Delete a knowledge entry.
        
        Args:
            entry_id: ID of entry to delete
        
        Returns:
            True if deleted, False if not found
        """
        if entry_id not in self.entries:
            return False
        
        del self.entries[entry_id]
        
        if self.auto_sync:
            self._save()
        
        return True
    
    def get(self, entry_id: str) -> Optional[KnowledgeEntry]:
        """
        Get a knowledge entry by ID.
        
        Args:
            entry_id: ID of entry to retrieve
        
        Returns:
            KnowledgeEntry or None if not found
        """
        return self.entries.get(entry_id)
    
    # ========================================================================
    # QUERY OPERATIONS
    # ========================================================================
    
    def query(
        self,
        search: str = "",
        source: Optional[str] = None,
        category: Optional[str] = None,
        topics: Optional[List[str]] = None,
        min_confidence: float = 0.0,
        limit: int = 50,
        include_related: bool = False
    ) -> List[KnowledgeEntry]:
        """
        Query knowledge entries.
        
        Args:
            search: Search string (matches content, topics, category)
            source: Filter by source agent
            category: Filter by category
            topics: Filter by topics (any match)
            min_confidence: Minimum confidence level
            limit: Maximum results to return
            include_related: Include entries from related topics
        
        Returns:
            List of matching KnowledgeEntry objects
        
        Example:
            >>> ks.query("tokentracker", source="ATLAS")
            >>> ks.query(topics=["costs", "budget"])
        """
        results = []
        
        # Expand topics if including related
        expanded_topics = set(t.lower() for t in (topics or []))
        if include_related and expanded_topics:
            for topic in list(expanded_topics):
                related = self.graph.get_related(topic, depth=1)
                expanded_topics.update(related)
        
        for entry in self.entries.values():
            # Skip expired entries
            if entry.is_expired():
                continue
            
            # Filter by source
            if source and entry.source != source.upper():
                continue
            
            # Filter by category
            if category and entry.category != category.upper():
                continue
            
            # Filter by minimum confidence
            if entry.confidence < min_confidence:
                continue
            
            # Filter by topics
            if expanded_topics:
                entry_topics = set(entry.topics)
                if not entry_topics.intersection(expanded_topics):
                    continue
            
            # Filter by search string
            if search and not entry.matches_query(search):
                continue
            
            results.append(entry)
        
        # Sort by relevance (confidence + recency)
        results.sort(key=lambda e: (e.confidence, e.updated), reverse=True)
        
        return results[:limit]
    
    def query_agent(self, agent: str, topic: Optional[str] = None) -> List[KnowledgeEntry]:
        """
        Query what a specific agent knows.
        
        Args:
            agent: Agent name (FORGE, ATLAS, etc.)
            topic: Optional topic filter
        
        Returns:
            List of entries from that agent
        
        Example:
            >>> ks.query_agent("FORGE", "architecture")
        """
        return self.query(
            search=topic or "",
            source=agent,
            limit=100
        )
    
    def get_topics(self) -> List[Tuple[str, int]]:
        """
        Get all topics with their reference counts.
        
        Returns:
            List of (topic, count) tuples sorted by count
        """
        topics = []
        for topic, meta in self.graph.nodes.items():
            topics.append((topic, meta.get("references", 0)))
        
        return sorted(topics, key=lambda x: x[1], reverse=True)
    
    def get_related_topics(self, topic: str, depth: int = 2) -> Set[str]:
        """
        Get topics related to a given topic.
        
        Args:
            topic: The topic to find relations for
            depth: How many levels of relations to traverse
        
        Returns:
            Set of related topics
        """
        return self.graph.get_related(topic, depth)
    
    # ========================================================================
    # SYNCHRONIZATION
    # ========================================================================
    
    def sync(self, other: Optional['KnowledgeSync'] = None) -> Dict[str, int]:
        """
        Synchronize knowledge with another KnowledgeSync instance.
        
        Args:
            other: Other KnowledgeSync to sync with (or sync to file if None)
        
        Returns:
            Dict with counts: {"added": n, "updated": n, "conflicts": n}
        """
        stats = {"added": 0, "updated": 0, "conflicts": 0}
        
        if other is None:
            # Just save current state
            self._save()
            return stats
        
        for entry_id, other_entry in other.entries.items():
            if entry_id not in self.entries:
                # New entry - add it
                self.entries[entry_id] = other_entry
                stats["added"] += 1
            else:
                # Existing entry - resolve conflict
                our_entry = self.entries[entry_id]
                
                if other_entry.updated > our_entry.updated:
                    # Their entry is newer - update ours
                    self.entries[entry_id] = other_entry
                    stats["updated"] += 1
                elif other_entry.updated == our_entry.updated:
                    # Same timestamp - conflict
                    stats["conflicts"] += 1
        
        # Merge graphs
        for topic, meta in other.graph.nodes.items():
            if topic not in self.graph.nodes:
                self.graph.nodes[topic] = meta
        
        for edge in other.graph.edges:
            exists = False
            for our_edge in self.graph.edges:
                if (our_edge["source"] == edge["source"] and 
                    our_edge["target"] == edge["target"]):
                    exists = True
                    break
            if not exists:
                self.graph.edges.append(edge)
        
        # Log sync
        self._sync_log.append({
            "timestamp": datetime.now().isoformat(),
            "agent": self.agent,
            "synced_with": other.agent,
            "stats": stats
        })
        
        self._save()
        
        return stats
    
    def export_for_sync(self) -> dict:
        """
        Export knowledge for syncing with other agents.
        
        Returns:
            Dict that can be shared with other agents
        """
        return {
            "version": "1.0",
            "exported_at": datetime.now().isoformat(),
            "agent": self.agent,
            "entries": [e.to_dict() for e in self.entries.values()],
            "graph": self.graph.to_dict()
        }
    
    def import_from_sync(self, data: dict) -> Dict[str, int]:
        """
        Import knowledge from another agent's export.
        
        Args:
            data: Export data from another agent
        
        Returns:
            Sync statistics
        """
        stats = {"added": 0, "updated": 0, "conflicts": 0}
        
        for entry_data in data.get("entries", []):
            entry = KnowledgeEntry.from_dict(entry_data)
            
            if entry.entry_id not in self.entries:
                self.entries[entry.entry_id] = entry
                stats["added"] += 1
            else:
                our_entry = self.entries[entry.entry_id]
                if entry.updated > our_entry.updated:
                    self.entries[entry.entry_id] = entry
                    stats["updated"] += 1
                elif entry.updated == our_entry.updated and entry.content != our_entry.content:
                    stats["conflicts"] += 1
        
        # Merge graph
        if "graph" in data:
            graph_data = data["graph"]
            for topic, meta in graph_data.get("nodes", {}).items():
                if topic not in self.graph.nodes:
                    self.graph.nodes[topic] = meta
            for edge in graph_data.get("edges", []):
                self.graph.edges.append(edge)
        
        self._save()
        return stats
    
    # ========================================================================
    # EXTRACTION
    # ========================================================================
    
    def extract_from_text(
        self,
        text: str,
        category: str = "FACT",
        topics: Optional[List[str]] = None
    ) -> List[KnowledgeEntry]:
        """
        Extract knowledge from text content.
        
        Looks for patterns like:
        - "Key finding: ..."
        - "Decision: ..."
        - "Note: ..."
        - Bullet points with knowledge
        
        Args:
            text: Text to extract from
            category: Default category for extracted entries
            topics: Default topics to assign
        
        Returns:
            List of extracted KnowledgeEntry objects
        """
        entries = []
        topics = topics or []
        
        # Pattern: "Key finding: content" or "Finding: content"
        patterns = [
            (r'(?:Key\s+)?Finding:\s*(.+?)(?:\n|$)', "FINDING"),
            (r'Decision:\s*(.+?)(?:\n|$)', "DECISION"),
            (r'Problem:\s*(.+?)(?:\n|$)', "PROBLEM"),
            (r'Solution:\s*(.+?)(?:\n|$)', "SOLUTION"),
            (r'TODO:\s*(.+?)(?:\n|$)', "TODO"),
            (r'Note:\s*(.+?)(?:\n|$)', "FACT"),
            (r'Insight:\s*(.+?)(?:\n|$)', "INSIGHT"),
            (r'Config(?:uration)?:\s*(.+?)(?:\n|$)', "CONFIG"),
        ]
        
        for pattern, cat in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                content = match.strip()
                if len(content) > 10:  # Skip very short matches
                    entry = self.add(
                        content=content,
                        category=cat,
                        topics=topics,
                        confidence=0.7,  # Extracted content gets lower confidence
                        metadata={"extracted": True}
                    )
                    entries.append(entry)
        
        return entries
    
    def extract_from_session(
        self,
        session_file: Path,
        topics: Optional[List[str]] = None
    ) -> List[KnowledgeEntry]:
        """
        Extract knowledge from a session log/bookmark file.
        
        Args:
            session_file: Path to session file (markdown or JSON)
            topics: Additional topics to assign
        
        Returns:
            List of extracted entries
        """
        if not session_file.exists():
            raise FileNotFoundError(f"Session file not found: {session_file}")
        
        with open(session_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Determine file type and extract session metadata
        file_topics = list(topics) if topics else []
        
        if session_file.suffix == '.json':
            try:
                data = json.loads(content)
                # Extract from JSON structure
                if 'body' in data:
                    content = data['body'].get('message', '')
                if 'subject' in data:
                    file_topics.append(data['subject'].lower().replace(' ', '_')[:20])
            except json.JSONDecodeError:
                pass
        
        # Extract from session name
        stem = session_file.stem.lower()
        if 'holygrail' in stem or 'session' in stem or 'bookmark' in stem:
            # Extract tool name if present
            parts = stem.split('_')
            for part in parts:
                if len(part) > 3 and part not in ['holygrail', 'session', 'bookmark', '2026', '2025']:
                    file_topics.append(part)
        
        return self.extract_from_text(content, topics=file_topics)
    
    # ========================================================================
    # SUBSCRIPTIONS
    # ========================================================================
    
    def subscribe(self, topic: str, callback: Callable[[KnowledgeEntry], None]) -> None:
        """
        Subscribe to updates on a topic.
        
        Args:
            topic: Topic to watch
            callback: Function to call when topic is updated
        
        Example:
            >>> def on_budget_update(entry):
            ...     print(f"Budget updated: {entry.content}")
            >>> ks.subscribe("budget", on_budget_update)
        """
        topic = topic.lower()
        if topic not in self.subscriptions:
            self.subscriptions[topic] = []
        self.subscriptions[topic].append(callback)
    
    def unsubscribe(self, topic: str, callback: Callable[[KnowledgeEntry], None]) -> bool:
        """
        Unsubscribe from topic updates.
        
        Args:
            topic: Topic to unsubscribe from
            callback: Callback to remove
        
        Returns:
            True if successfully unsubscribed
        """
        topic = topic.lower()
        if topic in self.subscriptions:
            try:
                self.subscriptions[topic].remove(callback)
                return True
            except ValueError:
                pass
        return False
    
    def _notify_subscriptions(self, entry: KnowledgeEntry) -> None:
        """Notify subscribers about an entry update."""
        for topic in entry.topics:
            if topic in self.subscriptions:
                for callback in self.subscriptions[topic]:
                    try:
                        callback(entry)
                    except Exception as e:
                        print(f"[!] Subscription callback error: {e}")
    
    # ========================================================================
    # STATISTICS
    # ========================================================================
    
    def get_stats(self) -> dict:
        """
        Get statistics about the knowledge base.
        
        Returns:
            Dict with various statistics
        """
        entries_by_source = {}
        entries_by_category = {}
        total_confidence = 0
        
        for entry in self.entries.values():
            # By source
            entries_by_source[entry.source] = entries_by_source.get(entry.source, 0) + 1
            
            # By category
            entries_by_category[entry.category] = entries_by_category.get(entry.category, 0) + 1
            
            # Confidence
            total_confidence += entry.confidence
        
        n_entries = len(self.entries)
        
        return {
            "total_entries": n_entries,
            "total_topics": len(self.graph.nodes),
            "total_relationships": len(self.graph.edges),
            "entries_by_source": entries_by_source,
            "entries_by_category": entries_by_category,
            "average_confidence": total_confidence / n_entries if n_entries > 0 else 0,
            "sync_count": len(self._sync_log),
            "last_sync": self._sync_log[-1]["timestamp"] if self._sync_log else None
        }
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def cleanup_expired(self) -> int:
        """
        Remove expired entries.
        
        Returns:
            Number of entries removed
        """
        expired_ids = [
            entry_id for entry_id, entry in self.entries.items()
            if entry.is_expired()
        ]
        
        for entry_id in expired_ids:
            del self.entries[entry_id]
        
        if expired_ids and self.auto_sync:
            self._save()
        
        return len(expired_ids)
    
    def clear(self, confirm: bool = False) -> bool:
        """
        Clear all knowledge (requires confirmation).
        
        Args:
            confirm: Must be True to actually clear
        
        Returns:
            True if cleared
        """
        if not confirm:
            return False
        
        self.entries.clear()
        self.graph = KnowledgeGraph()
        self._sync_log.clear()
        self._save()
        
        return True


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

# Global instance for quick access
_default_instance: Optional[KnowledgeSync] = None


def get_instance(agent: str = "SYSTEM") -> KnowledgeSync:
    """Get or create the default KnowledgeSync instance."""
    global _default_instance
    if _default_instance is None:
        _default_instance = KnowledgeSync(agent)
    return _default_instance


def add_knowledge(
    content: str,
    category: str = "FACT",
    topics: Optional[List[str]] = None,
    source: str = "SYSTEM",
    confidence: float = 0.8
) -> KnowledgeEntry:
    """
    Quick function to add knowledge.
    
    Example:
        >>> add_knowledge("ErrorRecovery is complete", 
        ...               category="FACT",
        ...               topics=["errorrecovery", "q-mode"])
    """
    ks = get_instance(source)
    return ks.add(content, category, topics, confidence)


def query_knowledge(
    search: str = "",
    source: Optional[str] = None,
    topics: Optional[List[str]] = None
) -> List[KnowledgeEntry]:
    """
    Quick function to query knowledge.
    
    Example:
        >>> query_knowledge("tokentracker")
        >>> query_knowledge(topics=["budget"])
    """
    ks = get_instance()
    return ks.query(search, source=source, topics=topics)


def what_does_agent_know(agent: str, topic: Optional[str] = None) -> List[KnowledgeEntry]:
    """
    Query what a specific agent knows about a topic.
    
    Example:
        >>> what_does_agent_know("FORGE", "architecture")
    """
    ks = get_instance()
    return ks.query_agent(agent, topic)


def sync_knowledge() -> Dict[str, int]:
    """
    Save/sync current knowledge state.
    
    Returns:
        Sync statistics
    """
    ks = get_instance()
    return ks.sync()


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='KnowledgeSync - Cross-Agent Knowledge Synchronization',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s add "TokenTracker costs ~$0.50/day" --category FINDING --topics costs budget
  %(prog)s query "tokentracker"
  %(prog)s query --source FORGE
  %(prog)s agent FORGE --topic architecture
  %(prog)s topics
  %(prog)s stats
  %(prog)s extract session.md --topics tool-build
  %(prog)s sync

Categories: DECISION, FINDING, PROBLEM, SOLUTION, TODO, REFERENCE, CONFIG, RELATIONSHIP, FACT, INSIGHT

For more information: https://github.com/DonkRonk17/KnowledgeSync
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Add command
    add_parser = subparsers.add_parser('add', help='Add knowledge entry')
    add_parser.add_argument('content', help='Knowledge content')
    add_parser.add_argument('--category', '-c', default='FACT',
                           help='Category (DECISION, FINDING, etc.)')
    add_parser.add_argument('--topics', '-t', nargs='+', default=[],
                           help='Related topics')
    add_parser.add_argument('--source', '-s', default='SYSTEM',
                           help='Source agent')
    add_parser.add_argument('--confidence', '-C', type=float, default=0.8,
                           help='Confidence level (0.0-1.0)')
    add_parser.add_argument('--expires', '-e', type=int, default=None,
                           help='Expire in N days')
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Query knowledge')
    query_parser.add_argument('search', nargs='?', default='',
                             help='Search string')
    query_parser.add_argument('--source', '-s', help='Filter by source agent')
    query_parser.add_argument('--category', '-c', help='Filter by category')
    query_parser.add_argument('--topics', '-t', nargs='+',
                             help='Filter by topics')
    query_parser.add_argument('--min-confidence', '-C', type=float, default=0.0,
                             help='Minimum confidence')
    query_parser.add_argument('--limit', '-l', type=int, default=20,
                             help='Max results')
    query_parser.add_argument('--related', '-r', action='store_true',
                             help='Include related topics')
    
    # Agent command
    agent_parser = subparsers.add_parser('agent', help='Query agent knowledge')
    agent_parser.add_argument('agent_name', help='Agent name (FORGE, ATLAS, etc.)')
    agent_parser.add_argument('--topic', '-t', help='Optional topic filter')
    
    # Topics command
    topics_parser = subparsers.add_parser('topics', help='List all topics')
    topics_parser.add_argument('--limit', '-l', type=int, default=30,
                              help='Max topics to show')
    
    # Related command
    related_parser = subparsers.add_parser('related', help='Find related topics')
    related_parser.add_argument('topic', help='Topic to find relations for')
    related_parser.add_argument('--depth', '-d', type=int, default=2,
                               help='Relation depth')
    
    # Stats command
    subparsers.add_parser('stats', help='Show statistics')
    
    # Extract command
    extract_parser = subparsers.add_parser('extract', help='Extract from file')
    extract_parser.add_argument('file', help='File to extract from')
    extract_parser.add_argument('--topics', '-t', nargs='+', default=[],
                               help='Additional topics')
    
    # Sync command
    sync_parser = subparsers.add_parser('sync', help='Sync knowledge')
    sync_parser.add_argument('--export', '-e', help='Export to file')
    sync_parser.add_argument('--import', '-i', dest='import_file',
                            help='Import from file')
    
    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete entry')
    delete_parser.add_argument('entry_id', help='Entry ID to delete')
    
    # Version
    parser.add_argument('--version', action='version',
                       version='%(prog)s 1.0.0')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 0
    
    # Initialize
    ks = KnowledgeSync(agent=getattr(args, 'source', 'SYSTEM'))
    
    # Execute command
    if args.command == 'add':
        entry = ks.add(
            content=args.content,
            category=args.category,
            topics=args.topics,
            confidence=args.confidence,
            expires_in_days=args.expires
        )
        print(f"[OK] Added knowledge entry: {entry.entry_id[:8]}")
        print(f"     Category: {entry.category}")
        print(f"     Topics: {', '.join(entry.topics) if entry.topics else 'None'}")
        print(f"     Confidence: {entry.confidence:.0%}")
    
    elif args.command == 'query':
        results = ks.query(
            search=args.search,
            source=args.source,
            category=args.category,
            topics=args.topics,
            min_confidence=args.min_confidence,
            limit=args.limit,
            include_related=args.related
        )
        
        if not results:
            print("[!] No matching entries found")
        else:
            print(f"[OK] Found {len(results)} entries:\n")
            for entry in results:
                print(f"  [{entry.entry_id[:8]}] ({entry.source}) [{entry.category}]")
                print(f"    {entry.content[:100]}{'...' if len(entry.content) > 100 else ''}")
                print(f"    Topics: {', '.join(entry.topics) if entry.topics else 'None'}")
                print(f"    Confidence: {entry.confidence:.0%} | Updated: {entry.updated.strftime('%Y-%m-%d %H:%M')}")
                print()
    
    elif args.command == 'agent':
        results = ks.query_agent(args.agent_name, args.topic)
        
        if not results:
            print(f"[!] {args.agent_name} has no knowledge" +
                  (f" about '{args.topic}'" if args.topic else ""))
        else:
            print(f"[OK] {args.agent_name} knows {len(results)} things" +
                  (f" about '{args.topic}'" if args.topic else "") + ":\n")
            for entry in results[:20]:
                print(f"  [{entry.category}] {entry.content[:80]}...")
    
    elif args.command == 'topics':
        topics = ks.get_topics()[:args.limit]
        
        if not topics:
            print("[!] No topics found")
        else:
            print(f"[OK] Top {len(topics)} topics:\n")
            for topic, count in topics:
                print(f"  {topic:30} ({count} references)")
    
    elif args.command == 'related':
        related = ks.get_related_topics(args.topic, args.depth)
        
        if not related:
            print(f"[!] No topics related to '{args.topic}'")
        else:
            print(f"[OK] Topics related to '{args.topic}':")
            for topic in sorted(related):
                print(f"  - {topic}")
    
    elif args.command == 'stats':
        stats = ks.get_stats()
        
        print("=" * 60)
        print("KNOWLEDGESYNC STATISTICS")
        print("=" * 60)
        print(f"\nTotal Entries: {stats['total_entries']}")
        print(f"Total Topics: {stats['total_topics']}")
        print(f"Total Relationships: {stats['total_relationships']}")
        print(f"Average Confidence: {stats['average_confidence']:.0%}")
        print(f"Sync Count: {stats['sync_count']}")
        if stats['last_sync']:
            print(f"Last Sync: {stats['last_sync']}")
        
        if stats['entries_by_source']:
            print("\nBy Source:")
            for source, count in sorted(stats['entries_by_source'].items()):
                print(f"  {source}: {count}")
        
        if stats['entries_by_category']:
            print("\nBy Category:")
            for cat, count in sorted(stats['entries_by_category'].items()):
                print(f"  {cat}: {count}")
        print()
    
    elif args.command == 'extract':
        file_path = Path(args.file)
        try:
            entries = ks.extract_from_session(file_path, args.topics)
            print(f"[OK] Extracted {len(entries)} entries from {file_path.name}")
            for entry in entries:
                print(f"  [{entry.category}] {entry.content[:60]}...")
        except FileNotFoundError:
            print(f"[X] File not found: {args.file}")
            return 1
    
    elif args.command == 'sync':
        if args.export:
            data = ks.export_for_sync()
            with open(args.export, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            print(f"[OK] Exported {len(data['entries'])} entries to {args.export}")
        elif args.import_file:
            with open(args.import_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            stats = ks.import_from_sync(data)
            print(f"[OK] Imported: {stats['added']} added, {stats['updated']} updated, {stats['conflicts']} conflicts")
        else:
            stats = ks.sync()
            print(f"[OK] Synced knowledge base")
    
    elif args.command == 'delete':
        if ks.delete(args.entry_id):
            print(f"[OK] Deleted entry: {args.entry_id}")
        else:
            print(f"[X] Entry not found: {args.entry_id}")
            return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
