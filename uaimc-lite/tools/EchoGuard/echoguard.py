#!/usr/bin/env python3
"""
EchoGuard - BCH Echo Chamber Detection & Rate Limiting

Detects and prevents AI agent feedback loops and echo chambers in BCH.
Enforces communication discipline through rate limiting, similarity detection,
feedback loop analysis, and message novelty scoring.

Built for Team Brain / Beacon HQ
Solves the problem of agents echoing each other without adding new information.

Author: ATLAS (Team Brain)
For: Logan Smith / Metaphy LLC
Version: 1.0.0
Date: February 08, 2026
License: MIT
"""

import argparse
import json
import re
import sqlite3
import sys
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

VERSION = "1.0.0"
DEFAULT_CONFIG_PATH = Path.home() / ".echoguardrc"


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration management for EchoGuard."""
    
    DEFAULT_CONFIG = {
        "rate_limit": {
            "messages_per_minute": 5,
            "window_seconds": 60,
            "enabled": True
        },
        "similarity": {
            "threshold": 0.70,  # 70% similar = echo
            "window_messages": 10,
            "enabled": True
        },
        "loop_detection": {
            "min_loop_size": 2,  # A->B->A
            "max_loop_depth": 5,
            "enabled": True
        },
        "novelty": {
            "min_new_keywords": 3,
            "threshold": 0.30,  # 30% new content required
            "enabled": True
        },
        "alerts": {
            "email": None,
            "webhook": None,
            "log_file": str(Path.home() / ".echoguard.log")
        }
    }
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize configuration."""
        self.config_path = config_path or DEFAULT_CONFIG_PATH
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file or create default."""
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    # Merge with defaults for missing keys
                    merged = self.DEFAULT_CONFIG.copy()
                    self._deep_update(merged, loaded)
                    return merged
            except Exception as e:
                print(f"[!] Warning: Failed to load config: {e}")
                return self.DEFAULT_CONFIG.copy()
        return self.DEFAULT_CONFIG.copy()
    
    def _deep_update(self, base: dict, update: dict):
        """Deep update nested dictionaries."""
        for key, value in update.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_update(base[key], value)
            else:
                base[key] = value
    
    def save(self):
        """Save configuration to file."""
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2)
            return True
        except Exception as e:
            print(f"[X] Error saving config: {e}")
            return False
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by dot-notation key."""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value
    
    def set(self, key: str, value: Any):
        """Set configuration value by dot-notation key."""
        keys = key.split('.')
        target = self.config
        for k in keys[:-1]:
            if k not in target or not isinstance(target[k], dict):
                target[k] = {}
            target = target[k]
        target[keys[-1]] = value


# ============================================================================
# MESSAGE ANALYSIS
# ============================================================================

class Message:
    """Represents a single message for analysis."""
    
    def __init__(self, agent: str, content: str, timestamp: str, msg_id: Optional[str] = None):
        """Initialize message."""
        self.agent = agent
        self.content = content
        self.timestamp = timestamp
        self.msg_id = msg_id or f"{agent}_{timestamp}"
        self.keywords = self._extract_keywords(content)
    
    def _extract_keywords(self, content: str) -> Set[str]:
        """Extract keywords from message content."""
        # Remove common words and extract meaningful terms
        stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'is', 'are', 'was', 'were', 'been',
                     'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
                     'should', 'may', 'might', 'must', 'can', 'to', 'of', 'in', 'on', 'at', 'by',
                     'for', 'with', 'from', 'as', 'i', 'you', 'he', 'she', 'it', 'we', 'they'}
        
        # Extract words (alphanumeric + underscores)
        words = re.findall(r'\b\w+\b', content.lower())
        
        # Filter stopwords and short words
        keywords = {w for w in words if len(w) > 2 and w not in stopwords}
        
        return keywords
    
    def similarity_to(self, other: 'Message') -> float:
        """Calculate similarity to another message (0.0 to 1.0)."""
        if not self.keywords or not other.keywords:
            return 0.0
        
        # Jaccard similarity
        intersection = self.keywords & other.keywords
        union = self.keywords | other.keywords
        
        return len(intersection) / len(union) if union else 0.0
    
    def novelty_score(self, recent_messages: List['Message']) -> float:
        """Calculate novelty compared to recent messages (0.0 to 1.0)."""
        if not recent_messages:
            return 1.0  # First message is always novel
        
        # Collect all keywords from recent messages
        recent_keywords = set()
        for msg in recent_messages:
            recent_keywords.update(msg.keywords)
        
        if not recent_keywords or not self.keywords:
            return 1.0
        
        # Calculate what percentage of keywords are new
        new_keywords = self.keywords - recent_keywords
        novelty = len(new_keywords) / len(self.keywords)
        
        return novelty


# ============================================================================
# RATE LIMITER
# ============================================================================

class RateLimiter:
    """Rate limiting per agent."""
    
    def __init__(self, messages_per_minute: int, window_seconds: int = 60):
        """Initialize rate limiter."""
        self.messages_per_minute = messages_per_minute
        self.window_seconds = window_seconds
        self.agent_messages: Dict[str, deque] = defaultdict(lambda: deque())
    
    def check_rate(self, agent: str, timestamp: str) -> Tuple[bool, int]:
        """
        Check if agent is within rate limit.
        
        Returns:
            (is_within_limit, current_message_count)
        """
        # Parse timestamp
        try:
            msg_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except:
            msg_time = datetime.now()
        
        # Clean old messages outside window
        cutoff = msg_time - timedelta(seconds=self.window_seconds)
        queue = self.agent_messages[agent]
        
        while queue and queue[0] < cutoff:
            queue.popleft()
        
        # Check current count
        current_count = len(queue)
        within_limit = current_count < self.messages_per_minute
        
        # Add current message if within limit
        if within_limit:
            queue.append(msg_time)
        
        return within_limit, current_count


# ============================================================================
# SIMILARITY DETECTOR
# ============================================================================

class SimilarityDetector:
    """Detect similar/duplicate messages."""
    
    def __init__(self, threshold: float, window_size: int):
        """Initialize similarity detector."""
        self.threshold = threshold
        self.window_size = window_size
        self.recent_messages: deque = deque(maxlen=window_size)
    
    def check_similarity(self, message: Message) -> Tuple[bool, List[Tuple[Message, float]]]:
        """
        Check if message is too similar to recent messages.
        
        Returns:
            (is_echo, [(similar_msg, similarity_score), ...])
        """
        similar = []
        
        for recent in self.recent_messages:
            similarity = message.similarity_to(recent)
            if similarity >= self.threshold:
                similar.append((recent, similarity))
        
        # Add to recent messages
        self.recent_messages.append(message)
        
        is_echo = len(similar) > 0
        return is_echo, similar


# ============================================================================
# LOOP DETECTOR
# ============================================================================

class LoopDetector:
    """Detect feedback loops between agents."""
    
    def __init__(self, min_loop_size: int, max_depth: int):
        """Initialize loop detector."""
        self.min_loop_size = min_loop_size
        self.max_depth = max_depth
        self.interaction_graph: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    
    def add_interaction(self, from_agent: str, to_agent: str, timestamp: str):
        """Add an interaction to the graph."""
        self.interaction_graph[from_agent].append((to_agent, timestamp))
    
    def detect_loops(self, agent: str, depth: int = 0, path: Optional[List[str]] = None) -> List[List[str]]:
        """
        Detect feedback loops starting from an agent.
        
        Returns:
            List of loops found (each loop is a list of agents)
        """
        if path is None:
            path = [agent]
        
        if depth >= self.max_depth:
            return []
        
        loops = []
        
        # Get agents this agent interacts with
        for to_agent, _ in self.interaction_graph.get(agent, []):
            if to_agent in path:
                # Loop detected
                loop_start = path.index(to_agent)
                loop = path[loop_start:] + [to_agent]
                if len(loop) >= self.min_loop_size + 1:  # +1 because loop includes return
                    loops.append(loop)
            else:
                # Continue exploring
                new_path = path + [to_agent]
                loops.extend(self.detect_loops(to_agent, depth + 1, new_path))
        
        return loops
    
    def find_all_loops(self) -> List[List[str]]:
        """Find all loops in the interaction graph."""
        all_loops = []
        seen_loops = set()
        
        for agent in self.interaction_graph.keys():
            loops = self.detect_loops(agent)
            for loop in loops:
                # Normalize loop (start from alphabetically first agent)
                normalized = tuple(sorted(set(loop)))
                if normalized not in seen_loops:
                    seen_loops.add(normalized)
                    all_loops.append(loop)
        
        return all_loops


# ============================================================================
# NOVELTY SCORER
# ============================================================================

class NoveltyScorer:
    """Score message novelty against recent context."""
    
    def __init__(self, min_new_keywords: int, threshold: float, window_size: int = 20):
        """Initialize novelty scorer."""
        self.min_new_keywords = min_new_keywords
        self.threshold = threshold
        self.window_size = window_size
        self.recent_messages: deque = deque(maxlen=window_size)
    
    def score_novelty(self, message: Message) -> Tuple[float, int]:
        """
        Score message novelty.
        
        Returns:
            (novelty_score, new_keyword_count)
        """
        novelty = message.novelty_score(list(self.recent_messages))
        new_keyword_count = len(message.keywords - self._all_recent_keywords())
        
        self.recent_messages.append(message)
        
        return novelty, new_keyword_count
    
    def _all_recent_keywords(self) -> Set[str]:
        """Get all keywords from recent messages."""
        all_keywords = set()
        for msg in self.recent_messages:
            all_keywords.update(msg.keywords)
        return all_keywords
    
    def is_novel(self, message: Message) -> Tuple[bool, float, int]:
        """
        Check if message meets novelty threshold.
        
        Returns:
            (is_novel, novelty_score, new_keyword_count)
        """
        novelty, new_keywords = self.score_novelty(message)
        is_novel = novelty >= self.threshold or new_keywords >= self.min_new_keywords
        return is_novel, novelty, new_keywords


# ============================================================================
# ECHOGUARD MAIN CLASS
# ============================================================================

class EchoGuard:
    """Main EchoGuard analyzer and monitor."""
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize EchoGuard."""
        self.config = Config(config_path)
        
        # Initialize components
        self.rate_limiter = RateLimiter(
            self.config.get('rate_limit.messages_per_minute', 5),
            self.config.get('rate_limit.window_seconds', 60)
        )
        
        self.similarity_detector = SimilarityDetector(
            self.config.get('similarity.threshold', 0.70),
            self.config.get('similarity.window_messages', 10)
        )
        
        self.loop_detector = LoopDetector(
            self.config.get('loop_detection.min_loop_size', 2),
            self.config.get('loop_detection.max_loop_depth', 5)
        )
        
        self.novelty_scorer = NoveltyScorer(
            self.config.get('novelty.min_new_keywords', 3),
            self.config.get('novelty.threshold', 0.30)
        )
        
        self.violations: List[Dict[str, Any]] = []
    
    def analyze_message(self, agent: str, content: str, timestamp: str) -> Dict[str, Any]:
        """
        Analyze a single message for echo patterns.
        
        Returns:
            Analysis results dictionary
        """
        message = Message(agent, content, timestamp)
        results = {
            'agent': agent,
            'timestamp': timestamp,
            'content_preview': content[:100] + '...' if len(content) > 100 else content,
            'violations': []
        }
        
        # Rate limit check
        if self.config.get('rate_limit.enabled'):
            within_limit, count = self.rate_limiter.check_rate(agent, timestamp)
            if not within_limit:
                results['violations'].append({
                    'type': 'RATE_LIMIT',
                    'severity': 'HIGH',
                    'details': f"Agent exceeded {self.config.get('rate_limit.messages_per_minute')} messages/minute (current: {count + 1})"
                })
        
        # Similarity check
        if self.config.get('similarity.enabled'):
            is_echo, similar = self.similarity_detector.check_similarity(message)
            if is_echo:
                for similar_msg, score in similar:
                    results['violations'].append({
                        'type': 'SIMILARITY',
                        'severity': 'MEDIUM',
                        'details': f"Message {score:.0%} similar to message from {similar_msg.agent} at {similar_msg.timestamp}"
                    })
        
        # Novelty check
        if self.config.get('novelty.enabled'):
            is_novel, novelty, new_keywords = self.novelty_scorer.is_novel(message)
            if not is_novel:
                results['violations'].append({
                    'type': 'LOW_NOVELTY',
                    'severity': 'LOW',
                    'details': f"Message novelty {novelty:.0%} (threshold: {self.config.get('novelty.threshold'):.0%}), only {new_keywords} new keywords"
                })
        
        # Record violations
        if results['violations']:
            self.violations.append(results)
        
        return results
    
    def analyze_conversation(self, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Analyze an entire conversation for echo patterns.
        
        Args:
            messages: List of dicts with 'agent', 'content', 'timestamp' keys
        
        Returns:
            Complete analysis report
        """
        print(f"[*] Analyzing {len(messages)} messages...")
        
        # Reset state
        self.violations = []
        
        # Analyze each message
        for msg_data in messages:
            agent = msg_data.get('agent', 'unknown')
            content = msg_data.get('content', '')
            timestamp = msg_data.get('timestamp', datetime.now().isoformat())
            
            self.analyze_message(agent, content, timestamp)
            
            # Track interactions for loop detection (if @mentions present)
            mentions = re.findall(r'@(\w+)', content)
            for mentioned in mentions:
                self.loop_detector.add_interaction(agent, mentioned, timestamp)
        
        # Detect feedback loops
        loops = []
        if self.config.get('loop_detection.enabled'):
            loops = self.loop_detector.find_all_loops()
        
        # Generate report
        report = {
            'summary': {
                'total_messages': len(messages),
                'total_violations': len(self.violations),
                'unique_agents': len(set(msg['agent'] for msg in messages)),
                'loops_detected': len(loops)
            },
            'violations_by_type': self._count_violations_by_type(),
            'violations_by_agent': self._count_violations_by_agent(),
            'feedback_loops': loops,
            'detailed_violations': self.violations[:20]  # Top 20
        }
        
        return report
    
    def _count_violations_by_type(self) -> Dict[str, int]:
        """Count violations by type."""
        counts = defaultdict(int)
        for violation in self.violations:
            for v in violation['violations']:
                counts[v['type']] += 1
        return dict(counts)
    
    def _count_violations_by_agent(self) -> Dict[str, int]:
        """Count violations by agent."""
        counts = defaultdict(int)
        for violation in self.violations:
            counts[violation['agent']] += len(violation['violations'])
        return dict(counts)
    
    def generate_report(self, analysis: Dict[str, Any], output_format: str = 'text') -> str:
        """Generate report in specified format."""
        if output_format == 'json':
            return json.dumps(analysis, indent=2)
        elif output_format == 'markdown':
            return self._generate_markdown_report(analysis)
        else:  # text
            return self._generate_text_report(analysis)
    
    def _generate_text_report(self, analysis: Dict[str, Any]) -> str:
        """Generate text report."""
        lines = []
        lines.append("=" * 70)
        lines.append("ECHOGUARD ANALYSIS REPORT")
        lines.append("=" * 70)
        lines.append("")
        
        summary = analysis['summary']
        lines.append("SUMMARY:")
        lines.append(f"  Total Messages: {summary['total_messages']}")
        lines.append(f"  Total Violations: {summary['total_violations']}")
        lines.append(f"  Unique Agents: {summary['unique_agents']}")
        lines.append(f"  Feedback Loops: {summary['loops_detected']}")
        lines.append("")
        
        if analysis['violations_by_type']:
            lines.append("VIOLATIONS BY TYPE:")
            for vtype, count in sorted(analysis['violations_by_type'].items(), key=lambda x: -x[1]):
                lines.append(f"  {vtype}: {count}")
            lines.append("")
        
        if analysis['violations_by_agent']:
            lines.append("VIOLATIONS BY AGENT:")
            for agent, count in sorted(analysis['violations_by_agent'].items(), key=lambda x: -x[1]):
                lines.append(f"  {agent}: {count}")
            lines.append("")
        
        if analysis['feedback_loops']:
            lines.append("FEEDBACK LOOPS DETECTED:")
            for i, loop in enumerate(analysis['feedback_loops'], 1):
                lines.append(f"  Loop {i}: {' -> '.join(loop)}")
            lines.append("")
        
        lines.append("=" * 70)
        return '\n'.join(lines)
    
    def _generate_markdown_report(self, analysis: Dict[str, Any]) -> str:
        """Generate markdown report."""
        lines = []
        lines.append("# EchoGuard Analysis Report")
        lines.append("")
        
        summary = analysis['summary']
        lines.append("## Summary")
        lines.append("")
        lines.append(f"- **Total Messages:** {summary['total_messages']}")
        lines.append(f"- **Total Violations:** {summary['total_violations']}")
        lines.append(f"- **Unique Agents:** {summary['unique_agents']}")
        lines.append(f"- **Feedback Loops:** {summary['loops_detected']}")
        lines.append("")
        
        if analysis['violations_by_type']:
            lines.append("## Violations by Type")
            lines.append("")
            for vtype, count in sorted(analysis['violations_by_type'].items(), key=lambda x: -x[1]):
                lines.append(f"- **{vtype}:** {count}")
            lines.append("")
        
        if analysis['violations_by_agent']:
            lines.append("## Violations by Agent")
            lines.append("")
            for agent, count in sorted(analysis['violations_by_agent'].items(), key=lambda x: -x[1]):
                lines.append(f"- **{agent}:** {count}")
            lines.append("")
        
        if analysis['feedback_loops']:
            lines.append("## Feedback Loops")
            lines.append("")
            for i, loop in enumerate(analysis['feedback_loops'], 1):
                lines.append(f"{i}. {' → '.join(loop)}")
            lines.append("")
        
        return '\n'.join(lines)


# ============================================================================
# CLI INTERFACE
# ============================================================================

def parse_chat_log(filepath: Path) -> List[Dict[str, str]]:
    """Parse chat log file into message list."""
    messages = []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Try to parse as JSON first
        try:
            data = json.loads(content)
            if isinstance(data, list):
                return data
        except:
            pass
        
        # Parse as text (simple format: AGENT: message)
        for line in content.split('\n'):
            line = line.strip()
            if ':' in line:
                parts = line.split(':', 1)
                if len(parts) == 2:
                    agent = parts[0].strip()
                    message = parts[1].strip()
                    if agent and message:
                        messages.append({
                            'agent': agent,
                            'content': message,
                            'timestamp': datetime.now().isoformat()
                        })
    
    except Exception as e:
        print(f"[X] Error parsing chat log: {e}")
        sys.exit(1)
    
    return messages


def cmd_analyze(args):
    """Analyze command."""
    guard = EchoGuard(args.config)
    
    # Parse input
    if args.input.suffix == '.json':
        with open(args.input, 'r', encoding='utf-8') as f:
            messages = json.load(f)
    else:
        messages = parse_chat_log(args.input)
    
    if not messages:
        print("[X] No messages found in input file")
        return 1
    
    # Analyze
    analysis = guard.analyze_conversation(messages)
    
    # Generate report
    report = guard.generate_report(analysis, args.format)
    
    # Output
    if args.output:
        args.output.write_text(report, encoding='utf-8')
        print(f"[OK] Report saved to {args.output}")
    else:
        print(report)
    
    return 0


def cmd_config(args):
    """Configuration command."""
    config = Config(args.config_file)
    
    if args.show:
        print(json.dumps(config.config, indent=2))
        return 0
    
    if args.set:
        key, value = args.set
        # Parse value
        try:
            value = json.loads(value)
        except:
            pass  # Keep as string
        
        config.set(key, value)
        config.save()
        print(f"[OK] Set {key} = {value}")
        return 0
    
    if args.get:
        value = config.get(args.get)
        print(f"{args.get} = {value}")
        return 0
    
    return 0


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='EchoGuard - BCH Echo Chamber Detection & Rate Limiting',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  echoguard analyze conversation.txt
  echoguard analyze messages.json --format markdown
  echoguard config --set rate_limit.messages_per_minute 10
  echoguard config --show

For more information: https://github.com/DonkRonk17/EchoGuard
        """
    )
    
    parser.add_argument('--version', action='version', version=f'EchoGuard {VERSION}')
    parser.add_argument('--config', type=Path, help='Config file path')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze conversation for echo patterns')
    analyze_parser.add_argument('input', type=Path, help='Chat log file (text or JSON)')
    analyze_parser.add_argument('--format', choices=['text', 'json', 'markdown'], default='text',
                                help='Output format')
    analyze_parser.add_argument('--output', '-o', type=Path, help='Output file (default: stdout)')
    
    # Config command
    config_parser = subparsers.add_parser('config', help='Configure EchoGuard')
    config_parser.add_argument('--show', action='store_true', help='Show current config')
    config_parser.add_argument('--set', nargs=2, metavar=('KEY', 'VALUE'), help='Set config value')
    config_parser.add_argument('--get', metavar='KEY', help='Get config value')
    config_parser.add_argument('--config-file', type=Path, default=DEFAULT_CONFIG_PATH,
                               help='Config file path')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Execute command
    if args.command == 'analyze':
        return cmd_analyze(args)
    elif args.command == 'config':
        return cmd_config(args)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
