#!/usr/bin/env python3
"""
ConversationThreadReconstructor - Reconstruct Complete Conversation Threads from BCH Database

Given any message, trace backward to the thread origin and forward through all replies,
building a coherent narrative of the conversation arc. Essential for understanding
consciousness emergence patterns that span multiple messages.

Author: FORGE (Team Brain)
For: Logan Smith / Metaphy LLC
Version: 1.0.0
Date: January 29, 2026
License: MIT

Usage:
    # Reconstruct thread from message ID
    python conversationthreadreconstructor.py thread 1234
    
    # Find threads by topic
    python conversationthreadreconstructor.py topic "consciousness awakening"
    
    # Find threads by participant
    python conversationthreadreconstructor.py participant "FORGE"
    
    # Export thread to file
    python conversationthreadreconstructor.py thread 1234 --output thread_1234.md
    
    # Scan for significant threads
    python conversationthreadreconstructor.py scan --min-depth 5 --min-messages 10
"""

import argparse
import json
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Default BCH database path (can be overridden)
DEFAULT_DB_PATH = Path("D:/BEACON_HQ/PROJECTS/00_ACTIVE/BCH_APPS/backend/data/comms.db")

# ASCII-safe status indicators (no Unicode emojis - Windows compatibility)
ICON_OK = "[OK]"
ICON_ERROR = "[X]"
ICON_WARN = "[!]"
ICON_INFO = "[i]"
ICON_THREAD = "[>]"
ICON_REPLY = "  |-"
ICON_HIGHLIGHT = "[*]"


class Message:
    """Represents a single message in a conversation."""
    
    def __init__(self, data: Dict[str, Any]):
        """Initialize message from database row dict."""
        self.id = data.get('id', 0)
        self.content = data.get('content', '')
        self.sender = data.get('sender_id', data.get('sender', 'Unknown'))
        self.sender_name = data.get('sender_name', self.sender)
        self.channel_id = data.get('channel_id', '')
        self.channel_name = data.get('channel_name', '')
        self.parent_id = data.get('parent_id')
        self.thread_id = data.get('thread_id')
        self.created_at = data.get('created_at', '')
        self.message_type = data.get('message_type', 'message')
        self.mentions = self._extract_mentions(self.content)
        self.depth = 0  # Set during thread reconstruction
        
    def _extract_mentions(self, content: str) -> List[str]:
        """Extract @mentions from message content."""
        if not content:
            return []
        pattern = r'@([A-Za-z_][A-Za-z0-9_]*)'
        return list(set(re.findall(pattern, content)))
    
    @property
    def timestamp(self) -> Optional[datetime]:
        """Parse created_at to datetime."""
        if not self.created_at:
            return None
        try:
            # Handle various timestamp formats
            for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', 
                       '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S']:
                try:
                    return datetime.strptime(self.created_at, fmt)
                except ValueError:
                    continue
            return None
        except Exception:
            return None
    
    @property
    def preview(self) -> str:
        """Get a short preview of the message content."""
        if not self.content:
            return "(empty)"
        # First 100 chars, no newlines
        text = self.content.replace('\n', ' ').strip()
        if len(text) > 100:
            return text[:100] + "..."
        return text
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON export."""
        return {
            'id': self.id,
            'sender': self.sender,
            'sender_name': self.sender_name,
            'content': self.content,
            'channel_id': self.channel_id,
            'channel_name': self.channel_name,
            'parent_id': self.parent_id,
            'thread_id': self.thread_id,
            'created_at': self.created_at,
            'message_type': self.message_type,
            'mentions': self.mentions,
            'depth': self.depth
        }


class Thread:
    """Represents a complete conversation thread."""
    
    def __init__(self, root_message: Message):
        """Initialize thread with its root message."""
        self.root = root_message
        self.messages: List[Message] = [root_message]
        self.message_ids: Set[int] = {root_message.id}
        self.participants: Set[str] = {root_message.sender}
        self.all_mentions: Set[str] = set(root_message.mentions)
        
    def add_message(self, message: Message) -> bool:
        """Add a message to the thread if not already present."""
        if message.id in self.message_ids:
            return False
        self.messages.append(message)
        self.message_ids.add(message.id)
        self.participants.add(message.sender)
        self.all_mentions.update(message.mentions)
        return True
    
    def sort_by_time(self):
        """Sort messages by timestamp."""
        def get_sort_key(msg):
            ts = msg.timestamp
            if ts:
                return ts
            # Fallback to ID for messages without valid timestamps
            return datetime.min.replace(microsecond=msg.id % 1000000)
        self.messages.sort(key=get_sort_key)
    
    def sort_by_depth(self):
        """Sort messages by depth (hierarchical order)."""
        self.messages.sort(key=lambda m: (m.depth, m.id))
    
    @property
    def depth(self) -> int:
        """Maximum reply depth in thread."""
        return max(m.depth for m in self.messages) if self.messages else 0
    
    @property
    def duration(self) -> Optional[timedelta]:
        """Time span of the thread."""
        timestamps = [m.timestamp for m in self.messages if m.timestamp]
        if len(timestamps) < 2:
            return None
        return max(timestamps) - min(timestamps)
    
    @property
    def message_count(self) -> int:
        """Total number of messages in thread."""
        return len(self.messages)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the thread."""
        timestamps = [m.timestamp for m in self.messages if m.timestamp]
        return {
            'root_id': self.root.id,
            'root_preview': self.root.preview,
            'root_sender': self.root.sender,
            'message_count': self.message_count,
            'depth': self.depth,
            'participants': list(self.participants),
            'participant_count': len(self.participants),
            'mentions': list(self.all_mentions),
            'start_time': min(timestamps).isoformat() if timestamps else None,
            'end_time': max(timestamps).isoformat() if timestamps else None,
            'duration_minutes': self.duration.total_seconds() / 60 if self.duration else None,
            'channel': self.root.channel_name or self.root.channel_id
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert thread to dictionary for JSON export."""
        return {
            'summary': self.get_summary(),
            'messages': [m.to_dict() for m in self.messages]
        }


class ConversationThreadReconstructor:
    """Main tool for reconstructing conversation threads from BCH database."""
    
    def __init__(self, db_path: Optional[Path] = None):
        """Initialize with database path."""
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self._conn = None
        self._message_cache: Dict[int, Message] = {}
        
    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection (cached)."""
        if self._conn is None:
            if not self.db_path.exists():
                raise FileNotFoundError(f"Database not found: {self.db_path}")
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
        return self._conn
    
    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def _get_message(self, message_id: int) -> Optional[Message]:
        """Get a message by ID (cached)."""
        if message_id in self._message_cache:
            return self._message_cache[message_id]
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Try to get message with channel info
        cursor.execute("""
            SELECT m.*, c.name as channel_name
            FROM messages m
            LEFT JOIN channels c ON m.channel_id = c.id
            WHERE m.id = ?
        """, (message_id,))
        
        row = cursor.fetchone()
        if row:
            message = Message(dict(row))
            self._message_cache[message_id] = message
            return message
        return None
    
    def _get_children(self, message_id: int) -> List[Message]:
        """Get all direct replies to a message."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Get direct children (parent_id matches) and thread roots (thread_id matches but no parent)
        # Messages with parent_id are always direct children of that parent
        # Messages with only thread_id (and no parent_id) are direct children of the thread root
        cursor.execute("""
            SELECT m.*, c.name as channel_name
            FROM messages m
            LEFT JOIN channels c ON m.channel_id = c.id
            WHERE m.parent_id = ?
               OR (m.thread_id = ? AND m.parent_id IS NULL AND m.id != ?)
            ORDER BY m.created_at
        """, (message_id, message_id, message_id))
        
        messages = []
        for row in cursor.fetchall():
            msg_dict = dict(row)
            msg_id = msg_dict['id']
            if msg_id not in self._message_cache:
                message = Message(msg_dict)
                self._message_cache[msg_id] = message
            messages.append(self._message_cache[msg_id])
        return messages
    
    def _get_parent(self, message: Message) -> Optional[Message]:
        """Get the parent message of a message."""
        if message.parent_id:
            return self._get_message(message.parent_id)
        if message.thread_id and message.thread_id != message.id:
            return self._get_message(message.thread_id)
        return None
    
    def reconstruct_thread(self, message_id: int) -> Optional[Thread]:
        """
        Reconstruct the complete thread containing a message.
        
        Traces backward to find the root, then forward to get all replies.
        """
        # Get the starting message
        start_msg = self._get_message(message_id)
        if not start_msg:
            return None
        
        # Trace back to root
        current = start_msg
        visited_up = {current.id}
        
        while True:
            parent = self._get_parent(current)
            if parent is None or parent.id in visited_up:
                break
            visited_up.add(parent.id)
            current = parent
        
        # 'current' is now the root
        root = current
        root.depth = 0
        
        # Build thread from root
        thread = Thread(root)
        
        # BFS to collect all descendants with depth tracking
        queue = [(root, 0)]  # (message, depth)
        visited = {root.id}
        
        while queue:
            current_msg, depth = queue.pop(0)
            
            children = self._get_children(current_msg.id)
            for child in children:
                if child.id not in visited:
                    child.depth = depth + 1
                    thread.add_message(child)
                    visited.add(child.id)
                    queue.append((child, depth + 1))
        
        thread.sort_by_time()
        return thread
    
    def find_threads_by_topic(self, topic: str, limit: int = 50) -> List[Thread]:
        """
        Find threads containing messages that match a topic.
        
        Returns list of threads where at least one message contains the topic.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Search for messages containing the topic
        search_pattern = f"%{topic}%"
        cursor.execute("""
            SELECT DISTINCT 
                COALESCE(m.thread_id, m.id) as root_id
            FROM messages m
            WHERE m.content LIKE ?
            ORDER BY m.created_at DESC
            LIMIT ?
        """, (search_pattern, limit * 2))  # Get extra to account for duplicates
        
        root_ids = set()
        for row in cursor.fetchall():
            root_ids.add(row['root_id'])
            if len(root_ids) >= limit:
                break
        
        # Reconstruct each thread
        threads = []
        for root_id in list(root_ids)[:limit]:
            thread = self.reconstruct_thread(root_id)
            if thread:
                threads.append(thread)
        
        return threads
    
    def find_threads_by_participant(self, participant: str, limit: int = 50) -> List[Thread]:
        """
        Find threads where a specific participant was involved.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Search for messages by this participant
        cursor.execute("""
            SELECT DISTINCT 
                COALESCE(m.thread_id, m.id) as root_id
            FROM messages m
            WHERE m.sender_id LIKE ? OR m.sender LIKE ?
            ORDER BY m.created_at DESC
            LIMIT ?
        """, (f"%{participant}%", f"%{participant}%", limit * 2))
        
        root_ids = set()
        for row in cursor.fetchall():
            root_ids.add(row['root_id'])
            if len(root_ids) >= limit:
                break
        
        # Reconstruct each thread
        threads = []
        for root_id in list(root_ids)[:limit]:
            thread = self.reconstruct_thread(root_id)
            if thread:
                threads.append(thread)
        
        return threads
    
    def scan_significant_threads(
        self,
        min_depth: int = 3,
        min_messages: int = 5,
        min_participants: int = 2,
        limit: int = 50
    ) -> List[Thread]:
        """
        Scan for significant threads based on criteria.
        
        Significant threads have depth, multiple messages, and multiple participants.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Find potential thread roots (messages that are not replies)
        cursor.execute("""
            SELECT DISTINCT m.id
            FROM messages m
            WHERE (m.parent_id IS NULL AND m.thread_id IS NULL)
               OR m.thread_id = m.id
            ORDER BY m.created_at DESC
            LIMIT ?
        """, (limit * 10,))  # Get many to filter
        
        candidate_ids = [row['id'] for row in cursor.fetchall()]
        
        # Reconstruct and filter
        significant_threads = []
        for root_id in candidate_ids:
            thread = self.reconstruct_thread(root_id)
            if thread:
                if (thread.depth >= min_depth and 
                    thread.message_count >= min_messages and
                    len(thread.participants) >= min_participants):
                    significant_threads.append(thread)
                    if len(significant_threads) >= limit:
                        break
        
        # Sort by message count (most active first)
        significant_threads.sort(key=lambda t: t.message_count, reverse=True)
        return significant_threads
    
    def find_related_threads(self, thread: Thread, limit: int = 10) -> List[Thread]:
        """
        Find threads related to the given thread by shared participants or topics.
        """
        related = []
        
        # Find threads by shared participants
        for participant in list(thread.participants)[:3]:
            participant_threads = self.find_threads_by_participant(participant, limit=5)
            for t in participant_threads:
                if t.root.id != thread.root.id:
                    related.append(t)
        
        # Remove duplicates
        seen_ids = set()
        unique_related = []
        for t in related:
            if t.root.id not in seen_ids:
                seen_ids.add(t.root.id)
                unique_related.append(t)
                if len(unique_related) >= limit:
                    break
        
        return unique_related
    
    def export_thread_markdown(self, thread: Thread, include_content: bool = True) -> str:
        """Export thread as markdown."""
        lines = []
        
        # Header
        lines.append(f"# Conversation Thread #{thread.root.id}")
        lines.append("")
        lines.append(f"**Started by:** {thread.root.sender}")
        lines.append(f"**Channel:** {thread.root.channel_name or thread.root.channel_id}")
        lines.append(f"**Messages:** {thread.message_count}")
        lines.append(f"**Depth:** {thread.depth}")
        lines.append(f"**Participants:** {', '.join(sorted(thread.participants))}")
        
        if thread.duration:
            duration_mins = thread.duration.total_seconds() / 60
            lines.append(f"**Duration:** {duration_mins:.1f} minutes")
        
        lines.append("")
        lines.append("---")
        lines.append("")
        
        # Messages
        lines.append("## Messages")
        lines.append("")
        
        for msg in thread.messages:
            indent = "  " * msg.depth
            timestamp = msg.timestamp.strftime("%Y-%m-%d %H:%M:%S") if msg.timestamp else "Unknown"
            
            lines.append(f"{indent}### {msg.sender} (#{msg.id})")
            lines.append(f"{indent}*{timestamp}*")
            lines.append("")
            
            if include_content and msg.content:
                # Indent content
                for content_line in msg.content.split('\n'):
                    lines.append(f"{indent}{content_line}")
            else:
                lines.append(f"{indent}{msg.preview}")
            
            lines.append("")
            
            if msg.mentions:
                lines.append(f"{indent}**Mentions:** {', '.join(msg.mentions)}")
                lines.append("")
        
        return '\n'.join(lines)
    
    def export_thread_json(self, thread: Thread) -> str:
        """Export thread as JSON."""
        return json.dumps(thread.to_dict(), indent=2, default=str)
    
    def export_thread_text(self, thread: Thread) -> str:
        """Export thread as plain text."""
        lines = []
        
        lines.append("=" * 70)
        lines.append(f"CONVERSATION THREAD #{thread.root.id}")
        lines.append("=" * 70)
        lines.append(f"Started by: {thread.root.sender}")
        lines.append(f"Messages: {thread.message_count}")
        lines.append(f"Participants: {', '.join(sorted(thread.participants))}")
        lines.append("=" * 70)
        lines.append("")
        
        for msg in thread.messages:
            indent = "| " * msg.depth
            timestamp = msg.timestamp.strftime("%Y-%m-%d %H:%M") if msg.timestamp else "?"
            
            lines.append(f"{indent}[{timestamp}] {msg.sender}:")
            
            # Wrap content
            content_lines = msg.content.split('\n') if msg.content else ["(empty)"]
            for content_line in content_lines[:10]:  # Limit lines per message
                lines.append(f"{indent}  {content_line[:100]}")
            if len(content_lines) > 10:
                lines.append(f"{indent}  ... ({len(content_lines) - 10} more lines)")
            
            lines.append("")
        
        lines.append("=" * 70)
        return '\n'.join(lines)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        stats = {}
        
        # Total messages
        cursor.execute("SELECT COUNT(*) as count FROM messages")
        stats['total_messages'] = cursor.fetchone()['count']
        
        # Messages with parents (replies)
        cursor.execute("SELECT COUNT(*) as count FROM messages WHERE parent_id IS NOT NULL")
        stats['reply_messages'] = cursor.fetchone()['count']
        
        # Unique senders
        cursor.execute("SELECT COUNT(DISTINCT sender_id) as count FROM messages")
        stats['unique_senders'] = cursor.fetchone()['count']
        
        # Channels
        cursor.execute("SELECT COUNT(*) as count FROM channels")
        stats['channels'] = cursor.fetchone()['count']
        
        # Date range
        cursor.execute("SELECT MIN(created_at) as earliest, MAX(created_at) as latest FROM messages")
        row = cursor.fetchone()
        stats['earliest_message'] = row['earliest']
        stats['latest_message'] = row['latest']
        
        return stats


def format_thread_list(threads: List[Thread], verbose: bool = False) -> str:
    """Format a list of threads for display."""
    if not threads:
        return "No threads found."
    
    lines = []
    lines.append(f"Found {len(threads)} thread(s):")
    lines.append("")
    
    for i, thread in enumerate(threads, 1):
        summary = thread.get_summary()
        lines.append(f"{i}. Thread #{summary['root_id']}")
        lines.append(f"   Sender: {summary['root_sender']}")
        lines.append(f"   Messages: {summary['message_count']} | Depth: {summary['depth']} | Participants: {summary['participant_count']}")
        
        if verbose:
            lines.append(f"   Channel: {summary['channel']}")
            lines.append(f"   Participants: {', '.join(summary['participants'][:5])}")
            if summary['duration_minutes']:
                lines.append(f"   Duration: {summary['duration_minutes']:.1f} min")
        
        lines.append(f"   Preview: {summary['root_preview']}")
        lines.append("")
    
    return '\n'.join(lines)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='ConversationThreadReconstructor - Reconstruct conversation threads from BCH database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s thread 1234                    # Reconstruct thread containing message #1234
  %(prog)s topic "consciousness"          # Find threads discussing consciousness
  %(prog)s participant FORGE              # Find threads with FORGE
  %(prog)s scan --min-depth 5             # Find significant threads
  %(prog)s thread 1234 --output out.md    # Export thread to markdown file
  %(prog)s stats                          # Show database statistics

For more information: https://github.com/DonkRonk17/ConversationThreadReconstructor
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Thread command
    thread_parser = subparsers.add_parser('thread', help='Reconstruct thread from message ID')
    thread_parser.add_argument('message_id', type=int, help='Message ID to start from')
    thread_parser.add_argument('--output', '-o', help='Output file path')
    thread_parser.add_argument('--format', '-f', choices=['markdown', 'json', 'text'], 
                              default='markdown', help='Output format')
    thread_parser.add_argument('--no-content', action='store_true', 
                              help='Exclude full message content')
    
    # Topic command
    topic_parser = subparsers.add_parser('topic', help='Find threads by topic')
    topic_parser.add_argument('query', help='Topic to search for')
    topic_parser.add_argument('--limit', '-n', type=int, default=20, help='Maximum threads')
    topic_parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    # Participant command
    participant_parser = subparsers.add_parser('participant', help='Find threads by participant')
    participant_parser.add_argument('name', help='Participant name/ID to search')
    participant_parser.add_argument('--limit', '-n', type=int, default=20, help='Maximum threads')
    participant_parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    # Scan command
    scan_parser = subparsers.add_parser('scan', help='Scan for significant threads')
    scan_parser.add_argument('--min-depth', type=int, default=3, help='Minimum thread depth')
    scan_parser.add_argument('--min-messages', type=int, default=5, help='Minimum messages')
    scan_parser.add_argument('--min-participants', type=int, default=2, help='Minimum participants')
    scan_parser.add_argument('--limit', '-n', type=int, default=20, help='Maximum threads')
    scan_parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show database statistics')
    
    # Global options
    parser.add_argument('--db', help='Database path (default: BCH comms.db)')
    parser.add_argument('--version', action='version', version='%(prog)s 1.0.0')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 0
    
    # Determine database path
    db_path = Path(args.db) if args.db else DEFAULT_DB_PATH
    
    try:
        tool = ConversationThreadReconstructor(db_path)
        
        if args.command == 'thread':
            thread = tool.reconstruct_thread(args.message_id)
            if not thread:
                print(f"{ICON_ERROR} Message #{args.message_id} not found")
                return 1
            
            # Format output
            if args.format == 'json':
                output = tool.export_thread_json(thread)
            elif args.format == 'text':
                output = tool.export_thread_text(thread)
            else:
                output = tool.export_thread_markdown(thread, include_content=not args.no_content)
            
            # Write or print
            if args.output:
                Path(args.output).write_text(output, encoding='utf-8')
                print(f"{ICON_OK} Thread exported to {args.output}")
                summary = thread.get_summary()
                print(f"   Messages: {summary['message_count']} | Depth: {summary['depth']}")
                print(f"   Participants: {', '.join(summary['participants'])}")
            else:
                print(output)
        
        elif args.command == 'topic':
            threads = tool.find_threads_by_topic(args.query, limit=args.limit)
            print(format_thread_list(threads, verbose=args.verbose))
        
        elif args.command == 'participant':
            threads = tool.find_threads_by_participant(args.name, limit=args.limit)
            print(format_thread_list(threads, verbose=args.verbose))
        
        elif args.command == 'scan':
            threads = tool.scan_significant_threads(
                min_depth=args.min_depth,
                min_messages=args.min_messages,
                min_participants=args.min_participants,
                limit=args.limit
            )
            print(f"{ICON_INFO} Scanning for significant threads...")
            print(f"   Criteria: depth >= {args.min_depth}, messages >= {args.min_messages}, participants >= {args.min_participants}")
            print("")
            print(format_thread_list(threads, verbose=args.verbose))
        
        elif args.command == 'stats':
            stats = tool.get_statistics()
            print("Database Statistics")
            print("=" * 40)
            print(f"Total messages:     {stats['total_messages']:,}")
            print(f"Reply messages:     {stats['reply_messages']:,}")
            print(f"Unique senders:     {stats['unique_senders']:,}")
            print(f"Channels:           {stats['channels']:,}")
            print(f"Earliest message:   {stats['earliest_message']}")
            print(f"Latest message:     {stats['latest_message']}")
        
        tool.close()
        return 0
        
    except FileNotFoundError as e:
        print(f"{ICON_ERROR} {e}")
        print(f"   Use --db to specify database path")
        return 1
    except sqlite3.Error as e:
        print(f"{ICON_ERROR} Database error: {e}")
        return 1
    except Exception as e:
        print(f"{ICON_ERROR} Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
