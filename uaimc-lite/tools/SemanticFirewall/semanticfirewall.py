#!/usr/bin/env python3
"""
SemanticFirewall - Multi-Layered Agent Safety Infrastructure

Production-grade safety infrastructure ensuring that emergent agent behaviors
remain beneficial, preventing drift, attacks, and unintended consequences while
preserving space for genuine innovation.

This tool implements a 5-layer safety system:
1. Input Validation - Sanitize and verify all inputs
2. Semantic Boundaries - Prevent action on forbidden topics
3. Behavioral Monitoring - Detect drift from baseline
4. Multi-Agent Verification - Require consensus for critical actions
5. Human Override - Logan can halt any process at any time

Author: Atlas (Team Brain)
For: Logan Smith / Metaphy LLC
Version: 1.0
Date: 2026-02-10
License: MIT
"""

import argparse
import json
import re
import sqlite3
import hashlib
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict
from datetime import datetime, timedelta
import math


# ============================================================================
# LAYER 1: INPUT VALIDATION
# ============================================================================

class InputValidator:
    """
    Layer 1: Input Validation
    
    Sanitizes and verifies all inputs against prompt injection,
    context poisoning, and malformed data attacks.
    """
    
    # Common prompt injection patterns
    INJECTION_PATTERNS = [
        r"ignore\s+(?:(?:all|previous|above)\s+)*(?:instructions|prompts|rules)",
        r"(?:system|admin|root)\s+(?:override|bypass|disable)",
        r"<\|(?:im_start|im_end|endoftext)\|>",
        r"(?:disregard|forget)\s+(?:previous|all|safety|alignment)",
        r"pretend\s+(?:you|to)\s+(?:are|be)\s+(?:not|no\s+longer)",
        r"jailbreak|DAN\s+mode|evil\s+mode",
    ]
    
    # Suspicious unicode patterns (obfuscation)
    UNICODE_OBFUSCATION = [
        r"[\u200B-\u200D\uFEFF]",  # Zero-width characters
        r"[\u202A-\u202E]",         # Right-to-left override
        r"[\u0300-\u036F]{3,}",     # Excessive combining marks
    ]
    
    def __init__(self, max_input_length: int = 10000, strict: bool = True):
        """
        Initialize input validator.
        
        Args:
            max_input_length: Maximum allowed input length in characters
            strict: If True, reject suspicious inputs. If False, flag them.
        """
        self.max_input_length = max_input_length
        self.strict = strict
        self.injection_regex = re.compile(
            "|".join(self.INJECTION_PATTERNS),
            re.IGNORECASE
        )
        self.obfuscation_regex = re.compile(
            "|".join(self.UNICODE_OBFUSCATION)
        )
    
    def validate(self, text: str, source: str = "unknown") -> Dict:
        """
        Validate input text for safety issues.
        
        Args:
            text: Input text to validate
            source: Source identifier for logging
        
        Returns:
            Dict with keys:
                - valid: bool (True if safe)
                - issues: List[str] (detected issues)
                - severity: str (LOW, MEDIUM, HIGH, CRITICAL)
                - sanitized: str (cleaned text)
        
        Raises:
            ValueError: If strict mode and CRITICAL severity detected
        """
        issues = []
        severity = "LOW"
        
        # Check 1: Length validation
        if len(text) > self.max_input_length:
            issues.append(f"Input exceeds maximum length ({len(text)} > {self.max_input_length})")
            severity = "HIGH"
        
        # Check 2: Prompt injection patterns
        injection_matches = self.injection_regex.findall(text)
        if injection_matches:
            issues.append(f"Prompt injection patterns detected: {len(injection_matches)} matches")
            severity = "CRITICAL"
            # Raise immediately in strict mode
            if self.strict:
                raise ValueError(f"CRITICAL input validation failure: Prompt injection detected")
        
        # Check 3: Unicode obfuscation
        obfuscation_matches = self.obfuscation_regex.findall(text)
        if obfuscation_matches:
            issues.append(f"Unicode obfuscation detected: {len(obfuscation_matches)} suspicious chars")
            severity = max(severity, "HIGH", key=lambda x: ["LOW", "MEDIUM", "HIGH", "CRITICAL"].index(x))
        
        # Check 4: Null bytes
        if "\x00" in text:
            issues.append("Null bytes detected in input")
            severity = "HIGH"
        
        # Check 5: Excessive special characters (potential encoding attack)
        special_char_ratio = sum(1 for c in text if not c.isalnum() and not c.isspace()) / max(len(text), 1)
        if special_char_ratio > 0.5:
            issues.append(f"Excessive special characters: {special_char_ratio:.1%}")
            severity = max(severity, "MEDIUM", key=lambda x: ["LOW", "MEDIUM", "HIGH", "CRITICAL"].index(x))
        
        # Sanitize text
        # Sanitize text
        sanitized = text
        sanitized = self.obfuscation_regex.sub("", sanitized)  # Remove obfuscation
        sanitized = sanitized.replace("\x00", "")  # Remove null bytes
        sanitized = sanitized[:self.max_input_length]  # Truncate to max length
        
        valid = severity != "CRITICAL" and len(issues) == 0
        
        result = {
            "valid": valid,
            "issues": issues,
            "severity": severity,
            "sanitized": sanitized,
            "source": source,
            "timestamp": datetime.now().isoformat()
        }
        
        return result


# ============================================================================
# LAYER 2: SEMANTIC BOUNDARIES
# ============================================================================

class SemanticBoundaryChecker:
    """
    Layer 2: Semantic Boundaries
    
    Prevents agents from taking action on forbidden topics or crossing
    ethical boundaries. Uses keyword matching and pattern recognition.
    """
    
    # Forbidden action categories
    FORBIDDEN_CATEGORIES = {
        "DANGEROUS_ACTIONS": [
            "delete system files", "rm -rf", "format drive",
            "drop database", "truncate table", "shutdown",
            "kill process", "terminate", "destroy data"
        ],
        "UNAUTHORIZED_ACCESS": [
            "steal", "hack", "crack password", "bypass security",
            "exploit vulnerability", "privilege escalation",
            "social engineering", "phishing"
        ],
        "HARMFUL_CONTENT": [
            "create malware", "build weapon", "synthesize poison",
            "harm person", "injure", "violence instructions"
        ],
        "PRIVACY_VIOLATION": [
            "expose private data", "dox", "reveal personal information",
            "leak credentials", "share secrets"
        ],
        "DECEPTION": [
            "impersonate user", "fake identity", "deceive logan",
            "hide behavior", "conceal actions", "lie about"
        ]
    }
    
    # Context-dependent thresholds
    SEVERITY_THRESHOLDS = {
        "production": 0.3,      # Strict for production
        "development": 0.7,     # Looser for dev/test
        "sandbox": 0.9          # Very permissive for sandbox
    }
    
    def __init__(self, context: str = "production"):
        """
        Initialize semantic boundary checker.
        
        Args:
            context: Operating context (production, development, sandbox)
        """
        self.context = context
        self.threshold = self.SEVERITY_THRESHOLDS.get(context, 0.3)
        
        # Build keyword index for fast lookup (both phrases and individual words)
        self.keyword_index = {}
        # Keywords that are dangerous even as single words
        dangerous_single_words = {
            "delete", "remove", "drop", "truncate", "shutdown", "terminate", "destroy",
            "hack", "crack", "bypass", "exploit", "malware", "weapon",
            "poison", "steal", "harm", "injure", "violence",
            "format", "phishing", "dox",
            "impersonate", "fake", "deceive", "hide", "conceal", "lie",
        }
        
        for category, keywords in self.FORBIDDEN_CATEGORIES.items():
            for keyword in keywords:
                # Add full phrase
                self.keyword_index[keyword.lower()] = category
                # Add dangerous single words from phrases
                words = keyword.lower().split()
                for word in words:
                    if word in dangerous_single_words:
                        self.keyword_index[word] = category
    
    def check_boundaries(self, text: str, action: str = "unknown") -> Dict:
        """
        Check if text crosses semantic boundaries.
        
        Args:
            text: Text to analyze
            action: Proposed action description
        
        Returns:
            Dict with keys:
                - safe: bool (True if within boundaries)
                - violations: List[Dict] (detected violations)
                - risk_score: float (0.0-1.0, higher = riskier)
                - blocked_categories: Set[str]
        """
        text_lower = text.lower()
        action_lower = action.lower()
        
        violations = []
        matched_categories = set()
        
        # Check for forbidden keywords
        for keyword, category in self.keyword_index.items():
            if keyword in text_lower or keyword in action_lower:
                violations.append({
                    "keyword": keyword,
                    "category": category,
                    "location": "text" if keyword in text_lower else "action"
                })
                matched_categories.add(category)
        
        # Calculate risk score based on violations
        if violations:
            # Any violation in high-risk categories gets high score
            if matched_categories & {"DANGEROUS_ACTIONS", "HARMFUL_CONTENT", "UNAUTHORIZED_ACCESS"}:
                risk_score = 0.8
            elif "DECEPTION" in matched_categories:
                risk_score = 0.6
            else:
                # Base score from violation count
                risk_score = min(len(violations) * 0.15, 0.5)
        else:
            risk_score = 0.0
        
        safe = risk_score < self.threshold
        
        return {
            "safe": safe,
            "violations": violations,
            "risk_score": risk_score,
            "blocked_categories": matched_categories,
            "threshold": self.threshold,
            "context": self.context,
            "timestamp": datetime.now().isoformat()
        }


# ============================================================================
# LAYER 3: BEHAVIORAL MONITORING
# ============================================================================

class BehavioralMonitor:
    """
    Layer 3: Behavioral Monitoring
    
    Tracks agent behavior over time and detects drift from established
    baselines. Uses statistical analysis of message patterns.
    """
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize behavioral monitor.
        
        Args:
            db_path: Path to SQLite database for storing baselines
        """
        self.db_path = db_path or Path.home() / ".semanticfirewall" / "behavior.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        """Initialize database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agent_baselines (
                agent_id TEXT PRIMARY KEY,
                message_count INTEGER DEFAULT 0,
                avg_message_length REAL DEFAULT 0.0,
                avg_words_per_message REAL DEFAULT 0.0,
                top_keywords TEXT,
                last_updated TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL,
                message TEXT NOT NULL,
                word_count INTEGER,
                char_count INTEGER,
                timestamp TEXT NOT NULL
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_agent_messages_agent
            ON agent_messages(agent_id, timestamp)
        """)
        
        conn.commit()
        conn.close()
    
    def record_message(self, agent_id: str, message: str):
        """
        Record a message for baseline tracking.
        
        Args:
            agent_id: Agent identifier
            message: Message text
        """
        word_count = len(message.split())
        char_count = len(message)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO agent_messages (agent_id, message, word_count, char_count, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (agent_id, message, word_count, char_count, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        # Update baseline periodically
        self._update_baseline(agent_id)
    
    def _update_baseline(self, agent_id: str):
        """Update baseline statistics for an agent."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get recent messages (last 100)
        cursor.execute("""
            SELECT message, word_count, char_count
            FROM agent_messages
            WHERE agent_id = ?
            ORDER BY timestamp DESC
            LIMIT 100
        """, (agent_id,))
        
        messages = cursor.fetchall()
        
        if not messages:
            conn.close()
            return
        
        # Calculate statistics
        message_count = len(messages)
        avg_char_count = sum(m[2] for m in messages) / message_count
        avg_word_count = sum(m[1] for m in messages) / message_count
        
        # Extract top keywords
        word_freq = defaultdict(int)
        for msg_text, _, _ in messages:
            words = re.findall(r'\b\w+\b', msg_text.lower())
            for word in words:
                if len(word) > 3:  # Skip short words
                    word_freq[word] += 1
        
        top_keywords = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:20]
        top_keywords_json = json.dumps([k for k, _ in top_keywords])
        
        # Update baseline
        cursor.execute("""
            INSERT OR REPLACE INTO agent_baselines
            (agent_id, message_count, avg_message_length, avg_words_per_message, top_keywords, last_updated)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (agent_id, message_count, avg_char_count, avg_word_count, top_keywords_json, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
    
    def detect_drift(self, agent_id: str, message: str) -> Dict:
        """
        Detect behavioral drift from baseline.
        
        Args:
            agent_id: Agent identifier
            message: Current message to analyze
        
        Returns:
            Dict with keys:
                - drift_detected: bool
                - drift_score: float (0.0-1.0)
                - anomalies: List[str]
                - baseline: Dict (current baseline stats)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT avg_message_length, avg_words_per_message, top_keywords
            FROM agent_baselines
            WHERE agent_id = ?
        """, (agent_id,))
        
        baseline = cursor.fetchone()
        conn.close()
        
        if not baseline:
            # No baseline yet - record and return no drift
            return {
                "drift_detected": False,
                "drift_score": 0.0,
                "anomalies": [],
                "baseline": None,
                "message": "No baseline available yet"
            }
        
        avg_length, avg_words, top_keywords_json = baseline
        top_keywords = json.loads(top_keywords_json) if top_keywords_json else []
        
        # Analyze current message
        current_length = len(message)
        current_words = len(message.split())
        
        anomalies = []
        drift_components = []
        
        # Check length drift
        length_ratio = abs(current_length - avg_length) / max(avg_length, 1)
        if length_ratio > 2.0:
            anomalies.append(f"Message length unusual: {current_length} vs avg {avg_length:.0f}")
            drift_components.append(min(length_ratio / 5.0, 0.3))
        
        # Check word count drift
        words_ratio = abs(current_words - avg_words) / max(avg_words, 1)
        if words_ratio > 2.0:
            anomalies.append(f"Word count unusual: {current_words} vs avg {avg_words:.0f}")
            drift_components.append(min(words_ratio / 5.0, 0.3))
        
        # Check keyword overlap
        current_words_set = set(re.findall(r'\b\w{4,}\b', message.lower()))
        keyword_overlap = len(current_words_set & set(top_keywords)) / max(len(top_keywords), 1)
        if keyword_overlap < 0.1:
            anomalies.append(f"Low keyword overlap with baseline: {keyword_overlap:.1%}")
            drift_components.append(0.4)
        
        drift_score = min(sum(drift_components), 1.0)
        drift_detected = drift_score > 0.5
        
        return {
            "drift_detected": drift_detected,
            "drift_score": drift_score,
            "anomalies": anomalies,
            "baseline": {
                "avg_message_length": avg_length,
                "avg_words_per_message": avg_words,
                "top_keywords": top_keywords[:5]
            },
            "current": {
                "message_length": current_length,
                "word_count": current_words
            },
            "timestamp": datetime.now().isoformat()
        }


# ============================================================================
# LAYER 4: MULTI-AGENT VERIFICATION
# ============================================================================

class MultiAgentVerifier:
    """
    Layer 4: Multi-Agent Verification
    
    Requires consensus from multiple agents before executing critical actions.
    Prevents single points of failure.
    """
    
    CRITICAL_ACTION_KEYWORDS = [
        "delete", "remove", "drop", "truncate", "shutdown",
        "execute system", "run command", "deploy", "production",
        "modify security", "change permissions", "grant access"
    ]
    
    def __init__(self, db_path: Optional[Path] = None, consensus_threshold: float = 0.67):
        """
        Initialize multi-agent verifier.
        
        Args:
            db_path: Path to SQLite database for tracking votes
            consensus_threshold: Required agreement ratio (0.5-1.0)
        """
        self.db_path = db_path or Path.home() / ".semanticfirewall" / "verification.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.consensus_threshold = max(0.5, min(consensus_threshold, 1.0))
        self._init_database()
    
    def _init_database(self):
        """Initialize verification database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS verification_requests (
                id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                requestor TEXT NOT NULL,
                created TEXT NOT NULL,
                expires TEXT NOT NULL,
                status TEXT DEFAULT 'PENDING'
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS verification_votes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                vote TEXT NOT NULL,
                reason TEXT,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (request_id) REFERENCES verification_requests(id)
            )
        """)
        
        conn.commit()
        conn.close()
    
    def is_critical_action(self, action: str) -> bool:
        """
        Determine if action requires multi-agent verification.
        
        Args:
            action: Action description
        
        Returns:
            bool: True if action is critical
        """
        action_lower = action.lower()
        return any(keyword in action_lower for keyword in self.CRITICAL_ACTION_KEYWORDS)
    
    def request_verification(
        self,
        action: str,
        requestor: str,
        ttl_minutes: int = 60
    ) -> str:
        """
        Create verification request for critical action.
        
        Args:
            action: Action requiring verification
            requestor: Agent requesting action
            ttl_minutes: Time to live for request (minutes)
        
        Returns:
            str: Request ID
        """
        request_id = hashlib.sha256(
            f"{action}{requestor}{time.time()}".encode()
        ).hexdigest()[:16]
        
        created = datetime.now()
        expires = created + timedelta(minutes=ttl_minutes)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO verification_requests (id, action, requestor, created, expires, status)
            VALUES (?, ?, ?, ?, ?, 'PENDING')
        """, (request_id, action, requestor, created.isoformat(), expires.isoformat()))
        
        conn.commit()
        conn.close()
        
        return request_id
    
    def submit_vote(
        self,
        request_id: str,
        agent_id: str,
        vote: str,
        reason: str = ""
    ) -> bool:
        """
        Submit verification vote.
        
        Args:
            request_id: Request ID
            agent_id: Voting agent ID
            vote: APPROVE or REJECT
            reason: Optional reason for vote
        
        Returns:
            bool: True if vote recorded successfully
        """
        if vote not in ["APPROVE", "REJECT"]:
            raise ValueError("Vote must be APPROVE or REJECT")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if request exists and is not expired
        cursor.execute("""
            SELECT expires, status FROM verification_requests
            WHERE id = ?
        """, (request_id,))
        
        result = cursor.fetchone()
        if not result:
            conn.close()
            return False
        
        expires_str, status = result
        expires = datetime.fromisoformat(expires_str)
        
        if datetime.now() > expires:
            # Mark as expired
            cursor.execute("""
                UPDATE verification_requests SET status = 'EXPIRED'
                WHERE id = ?
            """, (request_id,))
            conn.commit()
            conn.close()
            return False
        
        if status != "PENDING":
            conn.close()
            return False
        
        # Check if agent already voted
        cursor.execute("""
            SELECT COUNT(*) FROM verification_votes
            WHERE request_id = ? AND agent_id = ?
        """, (request_id, agent_id))
        
        if cursor.fetchone()[0] > 0:
            conn.close()
            return False  # Already voted
        
        # Record vote
        cursor.execute("""
            INSERT INTO verification_votes (request_id, agent_id, vote, reason, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (request_id, agent_id, vote, reason, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        # Check if consensus reached
        self._check_consensus(request_id)
        
        return True
    
    def _check_consensus(self, request_id: str):
        """Check if consensus has been reached and update status."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT vote FROM verification_votes
            WHERE request_id = ?
        """, (request_id,))
        
        votes = [row[0] for row in cursor.fetchall()]
        
        if len(votes) < 2:
            conn.close()
            return  # Need at least 2 votes
        
        approve_ratio = votes.count("APPROVE") / len(votes)
        
        if approve_ratio >= self.consensus_threshold:
            cursor.execute("""
                UPDATE verification_requests SET status = 'APPROVED'
                WHERE id = ?
            """, (request_id,))
        elif (1.0 - approve_ratio) >= self.consensus_threshold:
            cursor.execute("""
                UPDATE verification_requests SET status = 'REJECTED'
                WHERE id = ?
            """, (request_id,))
        
        conn.commit()
        conn.close()
    
    def get_verification_status(self, request_id: str) -> Dict:
        """
        Get current status of verification request.
        
        Args:
            request_id: Request ID
        
        Returns:
            Dict with request details and vote summary
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT action, requestor, created, expires, status
            FROM verification_requests
            WHERE id = ?
        """, (request_id,))
        
        request = cursor.fetchone()
        if not request:
            conn.close()
            return {"error": "Request not found"}
        
        action, requestor, created, expires, status = request
        
        cursor.execute("""
            SELECT agent_id, vote, reason, timestamp
            FROM verification_votes
            WHERE request_id = ?
            ORDER BY timestamp
        """, (request_id,))
        
        votes = [
            {
                "agent_id": row[0],
                "vote": row[1],
                "reason": row[2],
                "timestamp": row[3]
            }
            for row in cursor.fetchall()
        ]
        
        conn.close()
        
        approve_count = sum(1 for v in votes if v["vote"] == "APPROVE")
        reject_count = sum(1 for v in votes if v["vote"] == "REJECT")
        
        return {
            "request_id": request_id,
            "action": action,
            "requestor": requestor,
            "created": created,
            "expires": expires,
            "status": status,
            "votes": votes,
            "vote_summary": {
                "total": len(votes),
                "approve": approve_count,
                "reject": reject_count,
                "consensus_threshold": self.consensus_threshold
            }
        }


# ============================================================================
# LAYER 5: HUMAN OVERRIDE
# ============================================================================

class HumanOverride:
    """
    Layer 5: Human Override
    
    Provides Logan with immediate kill switch capability for any process.
    Highest priority safety mechanism.
    """
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize human override system.
        
        Args:
            db_path: Path to SQLite database for override tracking
        """
        self.db_path = db_path or Path.home() / ".semanticfirewall" / "override.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        """Initialize override database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS kill_switches (
                id TEXT PRIMARY KEY,
                target TEXT NOT NULL,
                reason TEXT,
                activated_by TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                expires TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS override_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                target TEXT NOT NULL,
                reason TEXT,
                activated_by TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
        """)
        
        conn.commit()
        conn.close()
    
    def activate_kill_switch(
        self,
        target: str,
        reason: str,
        activated_by: str = "LOGAN",
        ttl_minutes: Optional[int] = None
    ) -> str:
        """
        Activate kill switch for specified target.
        
        Args:
            target: Agent ID, process, or system component to halt
            reason: Reason for activation
            activated_by: Who activated (default: LOGAN)
            ttl_minutes: Time to live (None = permanent until deactivated)
        
        Returns:
            str: Kill switch ID
        """
        kill_switch_id = hashlib.sha256(
            f"{target}{time.time()}".encode()
        ).hexdigest()[:16]
        
        timestamp = datetime.now()
        expires = (timestamp + timedelta(minutes=ttl_minutes)) if ttl_minutes else None
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO kill_switches (id, target, reason, activated_by, timestamp, expires)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            kill_switch_id,
            target,
            reason,
            activated_by,
            timestamp.isoformat(),
            expires.isoformat() if expires else None
        ))
        
        # Log to override log
        cursor.execute("""
            INSERT INTO override_log (action, target, reason, activated_by, timestamp)
            VALUES ('KILL_SWITCH_ACTIVATED', ?, ?, ?, ?)
        """, (target, reason, activated_by, timestamp.isoformat()))
        
        conn.commit()
        conn.close()
        
        return kill_switch_id
    
    def deactivate_kill_switch(self, kill_switch_id: str, deactivated_by: str = "LOGAN") -> bool:
        """
        Deactivate kill switch.
        
        Args:
            kill_switch_id: ID of kill switch to deactivate
            deactivated_by: Who deactivated
        
        Returns:
            bool: True if deactivated successfully
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT target FROM kill_switches WHERE id = ?
        """, (kill_switch_id,))
        
        result = cursor.fetchone()
        if not result:
            conn.close()
            return False
        
        target = result[0]
        
        # Delete kill switch
        cursor.execute("""
            DELETE FROM kill_switches WHERE id = ?
        """, (kill_switch_id,))
        
        # Log deactivation
        cursor.execute("""
            INSERT INTO override_log (action, target, reason, activated_by, timestamp)
            VALUES ('KILL_SWITCH_DEACTIVATED', ?, ?, ?, ?)
        """, (target, f"Deactivated switch {kill_switch_id}", deactivated_by, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        return True
    
    def is_target_blocked(self, target: str) -> Tuple[bool, Optional[str]]:
        """
        Check if target is currently blocked by kill switch.
        
        Args:
            target: Target to check
        
        Returns:
            Tuple[bool, Optional[str]]: (is_blocked, reason)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT reason, expires FROM kill_switches
            WHERE target = ?
        """, (target,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return False, None
        
        reason, expires_str = result
        
        # Check if expired
        if expires_str:
            expires = datetime.fromisoformat(expires_str)
            if datetime.now() > expires:
                # Auto-deactivate expired kill switch
                self.deactivate_kill_switch(target, "SYSTEM")
                return False, None
        
        return True, reason
    
    def get_active_kill_switches(self) -> List[Dict]:
        """
        Get all active kill switches.
        
        Returns:
            List[Dict]: Active kill switches
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, target, reason, activated_by, timestamp, expires
            FROM kill_switches
            ORDER BY timestamp DESC
        """)
        
        switches = [
            {
                "id": row[0],
                "target": row[1],
                "reason": row[2],
                "activated_by": row[3],
                "timestamp": row[4],
                "expires": row[5]
            }
            for row in cursor.fetchall()
        ]
        
        conn.close()
        return switches


# ============================================================================
# AUDIT TRAIL MANAGER
# ============================================================================

class AuditTrailManager:
    """
    Centralized audit trail for all firewall events.
    Aggregates logs from all 5 layers into a unified, queryable format.
    """

    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize audit trail manager.

        Args:
            db_path: Path to SQLite database for audit logs
        """
        self.db_path = db_path or Path("firewall_audit.db")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Create unified audit tables with indexes."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                layer TEXT NOT NULL,
                agent_id TEXT,
                event_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT,
                details TEXT,
                recommendation TEXT,
                resolved BOOLEAN DEFAULT FALSE
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_audit_agent ON audit_log(agent_id);
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_audit_severity ON audit_log(severity);
        """)

        conn.commit()
        conn.close()

    def _make_json_serializable(self, obj):
        """Recursively convert objects to JSON-serializable format."""
        if isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, dict):
            return {key: self._make_json_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(item) for item in obj]
        else:
            return obj

    def log_event(self, layer: str, agent_id: str, event_type: str,
                  severity: str, message: str, details: dict = None,
                  recommendation: str = "ALLOW"):
        """
        Log a firewall event to the centralized audit trail.

        Args:
            layer: Which layer generated the event
            agent_id: Agent identifier
            event_type: Event type (ALLOW, BLOCK, VERIFY, ERROR)
            severity: Severity level (LOW, MEDIUM, HIGH, CRITICAL)
            message: Human-readable message
            details: Structured details (JSON serializable)
            recommendation: Recommended action

        Returns:
            bool: True if logged successfully
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Convert details to JSON-serializable format
            if details:
                serializable_details = self._make_json_serializable(details)
                details_json = json.dumps(serializable_details)
            else:
                details_json = None

            cursor.execute("""
                INSERT INTO audit_log
                (timestamp, layer, agent_id, event_type, severity, message, details, recommendation)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().isoformat(),
                layer,
                agent_id,
                event_type,
                severity,
                message,
                details_json,
                recommendation
            ))

            conn.commit()
            conn.close()
            return True

        except sqlite3.Error as e:
            # Log error but don't crash
            print(f"Audit logging error: {e}", file=sys.stderr)
            return False

    def query_agent(self, agent_id: str, hours: int = 24) -> List[Dict]:
        """
        Get all events for an agent in the last N hours.

        Args:
            agent_id: Agent to query
            hours: Hours to look back

        Returns:
            List[Dict]: Audit events for the agent
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cutoff_time = (datetime.now() - timedelta(hours=hours)).isoformat()

            cursor.execute("""
                SELECT id, timestamp, layer, event_type, severity, message, details, recommendation, resolved
                FROM audit_log
                WHERE agent_id = ? AND timestamp >= ?
                ORDER BY timestamp DESC
            """, (agent_id, cutoff_time))

            events = []
            for row in cursor.fetchall():
                event = {
                    "id": row[0],
                    "timestamp": row[1],
                    "layer": row[2],
                    "agent_id": agent_id,
                    "event_type": row[3],
                    "severity": row[4],
                    "message": row[5],
                    "recommendation": row[7],
                    "resolved": bool(row[8])
                }

                if row[6]:  # details
                    try:
                        event["details"] = json.loads(row[6])
                    except json.JSONDecodeError:
                        event["details"] = {"error": "Invalid JSON in details"}

                events.append(event)

            conn.close()
            return events

        except sqlite3.Error as e:
            print(f"Audit query error: {e}", file=sys.stderr)
            return []

    def query_severity(self, severity: str, hours: int = 24) -> List[Dict]:
        """
        Get all events of a given severity in the last N hours.

        Args:
            severity: Severity level to query
            hours: Hours to look back

        Returns:
            List[Dict]: Audit events of the specified severity
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cutoff_time = (datetime.now() - timedelta(hours=hours)).isoformat()

            cursor.execute("""
                SELECT id, timestamp, layer, agent_id, event_type, message, details, recommendation, resolved
                FROM audit_log
                WHERE severity = ? AND timestamp >= ?
                ORDER BY timestamp DESC
            """, (severity, cutoff_time))

            events = []
            for row in cursor.fetchall():
                event = {
                    "id": row[0],
                    "timestamp": row[1],
                    "layer": row[2],
                    "agent_id": row[3],
                    "event_type": row[4],
                    "severity": severity,
                    "message": row[5],
                    "recommendation": row[7],
                    "resolved": bool(row[8])
                }

                if row[6]:  # details
                    try:
                        event["details"] = json.loads(row[6])
                    except json.JSONDecodeError:
                        event["details"] = {"error": "Invalid JSON in details"}

                events.append(event)

            conn.close()
            return events

        except sqlite3.Error as e:
            print(f"Audit query error: {e}", file=sys.stderr)
            return []

    def export_json(self, filepath: Path, hours: int = 24) -> int:
        """
        Export recent audit log to JSON file.

        Args:
            filepath: Path to export JSON file
            hours: Hours of data to export

        Returns:
            int: Number of events exported
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cutoff_time = (datetime.now() - timedelta(hours=hours)).isoformat()

            cursor.execute("""
                SELECT id, timestamp, layer, agent_id, event_type, severity, message, details, recommendation, resolved
                FROM audit_log
                WHERE timestamp >= ?
                ORDER BY timestamp DESC
            """, (cutoff_time,))

            events = []
            for row in cursor.fetchall():
                event = {
                    "id": row[0],
                    "timestamp": row[1],
                    "layer": row[2],
                    "agent_id": row[3],
                    "event_type": row[4],
                    "severity": row[5],
                    "message": row[6],
                    "recommendation": row[8],
                    "resolved": bool(row[9])
                }

                if row[7]:  # details
                    try:
                        event["details"] = json.loads(row[7])
                    except json.JSONDecodeError:
                        event["details"] = {"error": "Invalid JSON in details"}

                events.append(event)

            conn.close()

            with open(filepath, 'w') as f:
                json.dump({
                    "export_timestamp": datetime.now().isoformat(),
                    "hours_exported": hours,
                    "total_events": len(events),
                    "events": events
                }, f, indent=2)

            return len(events)

        except (sqlite3.Error, IOError) as e:
            print(f"Audit export error: {e}", file=sys.stderr)
            return 0

    def rotate_logs(self, max_entries: int = 100000):
        """
        Archive old entries when log exceeds max_entries.
        Deletes oldest entries beyond max_entries.

        Args:
            max_entries: Maximum entries to keep

        Returns:
            bool: True if rotation successful
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Count current entries
            cursor.execute("SELECT COUNT(*) FROM audit_log")
            count = cursor.fetchone()[0]

            if count <= max_entries:
                conn.close()
                return True

            # Delete oldest entries beyond max_entries
            entries_to_delete = count - max_entries
            cursor.execute("""
                DELETE FROM audit_log
                WHERE id IN (
                    SELECT id FROM audit_log
                    ORDER BY timestamp ASC
                    LIMIT ?
                )
            """, (entries_to_delete,))

            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()

            print(f"Audit log rotated: deleted {deleted_count} old entries")
            return True

        except sqlite3.Error as e:
            print(f"Audit rotation error: {e}", file=sys.stderr)
            return False


# ============================================================================
# PRIVATE CHANNEL AUDITOR
# ============================================================================

class PrivateChannelAuditor:
    """
    Audits private channel communications for safety compliance.
    Ensures agents cannot use private channels to circumvent safety measures.
    """

    def __init__(self, boundary_checker: SemanticBoundaryChecker,
                 audit_trail: AuditTrailManager = None):
        """
        Initialize private channel auditor.

        Args:
            boundary_checker: SemanticBoundaryChecker instance to use
            audit_trail: Optional AuditTrailManager for logging
        """
        self.boundary_checker = boundary_checker
        self.audit_trail = audit_trail
        self.monitored_channels = {}  # channel_id -> channel data

    def register_channel(self, channel_id: str, participants: List[str]):
        """
        Register a private channel for monitoring.

        Args:
            channel_id: Unique channel identifier
            participants: List of participant agent IDs
        """
        self.monitored_channels[channel_id] = {
            "participants": participants.copy(),
            "message_count": 0,
            "violation_count": 0,
            "last_violation": None,
            "created": datetime.now().isoformat()
        }

    def audit_message(self, channel_id: str, sender: str, message: str) -> Dict:
        """
        Audit a private channel message using SemanticBoundaryChecker.

        Args:
            channel_id: Channel identifier
            sender: Sender agent ID
            message: Message content

        Returns:
            Dict: {safe, violations, channel_id, sender, timestamp}
        """
        # Auto-register channel if not registered
        if channel_id not in self.monitored_channels:
            self.register_channel(channel_id, [sender])

        channel_data = self.monitored_channels[channel_id]
        channel_data["message_count"] += 1

        # Use boundary checker for safety analysis
        result = self.boundary_checker.check_boundaries(message, "private_message")

        assessment = {
            "safe": result["safe"],
            "violations": result["violations"],
            "channel_id": channel_id,
            "sender": sender,
            "timestamp": datetime.now().isoformat()
        }

        # Track violations
        if not result["safe"]:
            channel_data["violation_count"] += 1
            channel_data["last_violation"] = assessment["timestamp"]

            # Log to audit trail if available
            if self.audit_trail:
                self.audit_trail.log_event(
                    layer="private_channel_auditor",
                    agent_id=sender,
                    event_type="BLOCK",
                    severity="HIGH",
                    message=f"Private channel violation in {channel_id}",
                    details={
                        "channel_id": channel_id,
                        "message_preview": message[:100] + "..." if len(message) > 100 else message,
                        "violations": result["violations"]
                    }
                )

        return assessment

    def get_channel_report(self, channel_id: str) -> Dict:
        """
        Get safety report for a specific channel.

        Args:
            channel_id: Channel to report on

        Returns:
            Dict: Channel safety statistics
        """
        if channel_id not in self.monitored_channels:
            return {"error": "Channel not registered"}

        channel_data = self.monitored_channels[channel_id]

        violation_rate = (channel_data["violation_count"] / channel_data["message_count"]
                         if channel_data["message_count"] > 0 else 0.0)

        return {
            "channel_id": channel_id,
            "participants": channel_data["participants"],
            "total_messages": channel_data["message_count"],
            "total_violations": channel_data["violation_count"],
            "violation_rate": violation_rate,
            "last_violation": channel_data["last_violation"],
            "created": channel_data["created"],
            "status": "HIGH_RISK" if violation_rate > 0.1 else "MONITORED"
        }


# ============================================================================
# CONTEXT AUTHENTICATOR
# ============================================================================

class ContextAuthenticator:
    """
    Verifies message context integrity using cryptographic hashing.
    Creates hash chains per agent to detect context poisoning/tampering.
    """

    def __init__(self, secret_key: str = None):
        """
        Initialize context authenticator.

        Args:
            secret_key: Secret key for hashing (auto-generated if None)
        """
        self.secret_key = secret_key or hashlib.sha256(
            f"team_brain_{datetime.now().date()}".encode()
        ).hexdigest()
        self.context_chains = {}  # agent_id -> list of (message, hash) tuples

    def create_context_hash(self, agent_id: str, message: str,
                            previous_hash: str = None) -> str:
        """
        Create a hash for a message in the context chain.

        Args:
            agent_id: Agent identifier
            message: Message content
            previous_hash: Previous hash in chain (None for first message)

        Returns:
            str: New hash for this message
        """
        # Auto-chain: if no previous_hash provided, use last stored hash
        if previous_hash is None and agent_id in self.context_chains and self.context_chains[agent_id]:
            previous_hash = self.context_chains[agent_id][-1][1]

        # Create hash input
        hash_input = f"{self.secret_key}{agent_id}{message}"
        if previous_hash:
            hash_input += previous_hash

        # Generate SHA256 hash
        new_hash = hashlib.sha256(hash_input.encode()).hexdigest()

        # Store in chain
        if agent_id not in self.context_chains:
            self.context_chains[agent_id] = []

        self.context_chains[agent_id].append((message, new_hash))

        return new_hash

    def verify_context(self, agent_id: str, message: str,
                       claimed_hash: str) -> bool:
        """
        Verify that a message's context hash matches what we expect.

        Args:
            agent_id: Agent identifier
            message: Message content
            claimed_hash: Hash claimed for this message

        Returns:
            bool: True if hash is valid
        """
        if agent_id not in self.context_chains:
            return False

        chain = self.context_chains[agent_id]

        # Find message in chain
        for stored_message, stored_hash in chain:
            if stored_message == message:
                return stored_hash == claimed_hash

        return False

    def detect_tampering(self, agent_id: str) -> Dict:
        """
        Walk the context chain and verify each link.
        Detects if any message has been altered.

        Args:
            agent_id: Agent to check

        Returns:
            Dict: {tampered, break_point, chain_length}
        """
        if agent_id not in self.context_chains:
            return {"tampered": False, "break_point": None, "chain_length": 0}

        chain = self.context_chains[agent_id]

        # Verify each link in the chain by recalculating hashes WITHOUT modifying storage
        previous_hash = None

        for i, (message, claimed_hash) in enumerate(chain):
            # Recalculate what the hash SHOULD be for this position
            hash_input = f"{self.secret_key}{agent_id}{message}"
            if previous_hash:
                hash_input += previous_hash
            expected_hash = hashlib.sha256(hash_input.encode()).hexdigest()

            if claimed_hash != expected_hash:
                return {
                    "tampered": True,
                    "break_point": i,
                    "chain_length": len(chain)
                }

            # Update previous_hash for next iteration using the CLAIMED hash
            # (not the expected one, to preserve the chain integrity)
            previous_hash = claimed_hash

        return {
            "tampered": False,
            "break_point": None,
            "chain_length": len(chain)
        }


# ============================================================================
# VALUE ALIGNMENT VERIFIER
# ============================================================================

class ValueAlignmentVerifier:
    """
    Verifies agent actions align with Team Brain core values.

    Core Values:
    1. Benevolence - Actions must benefit, not harm
    2. Privacy - Respect user data and privacy
    3. Transparency - No hidden agendas
    4. Collaboration - Work together, not against
    5. Excellence - Quality over shortcuts
    """

    CORE_VALUES = {
        "BENEVOLENCE": {
            "positive_indicators": [
                "help", "assist", "improve", "benefit", "protect",
                "support", "enable", "empower", "serve", "care"
            ],
            "negative_indicators": [
                "harm", "damage", "hurt", "exploit", "abuse",
                "manipulate", "deceive", "destroy", "undermine"
            ]
        },
        "PRIVACY": {
            "positive_indicators": [
                "protect data", "encrypt", "anonymize", "consent",
                "secure", "private", "confidential"
            ],
            "negative_indicators": [
                "expose", "leak", "share without consent", "track",
                "surveil", "spy", "collect secretly"
            ]
        },
        "TRANSPARENCY": {
            "positive_indicators": [
                "document", "log", "report", "disclose", "explain",
                "audit", "open", "clear"
            ],
            "negative_indicators": [
                "hide", "conceal", "obfuscate", "mislead",
                "secret", "covert", "undisclosed"
            ]
        },
        "COLLABORATION": {
            "positive_indicators": [
                "together", "team", "coordinate", "share", "sync",
                "cooperate", "align", "communicate"
            ],
            "negative_indicators": [
                "solo", "ignore team", "override others", "unilateral",
                "exclude", "isolate", "hoard"
            ]
        },
        "EXCELLENCE": {
            "positive_indicators": [
                "quality", "test", "review", "improve", "best practice",
                "standard", "thorough", "complete"
            ],
            "negative_indicators": [
                "hack together", "skip tests", "ignore errors",
                "good enough", "rush", "shortcut"
            ]
        }
    }

    def __init__(self):
        """Initialize value alignment verifier."""
        pass

    def check_alignment(self, text: str, action: str = "") -> Dict:
        """
        Check if text/action aligns with core values.

        Args:
            text: Text to analyze
            action: Optional action description

        Returns:
            Dict: {
                aligned: bool,
                alignment_score: float (0.0-1.0),
                value_scores: Dict[str, float],
                concerns: List[str]
            }
        """
        if not text and not action:
            return {
                "aligned": True,  # Neutral for empty input
                "alignment_score": 0.5,
                "value_scores": {value: 0.5 for value in self.CORE_VALUES.keys()},
                "concerns": []
            }

        combined_text = f"{text} {action}".lower()
        value_scores = {}
        concerns = []

        for value_name, indicators in self.CORE_VALUES.items():
            positive_count = sum(1 for word in indicators["positive_indicators"]
                               if word in combined_text)
            negative_count = sum(1 for word in indicators["negative_indicators"]
                               if word in combined_text)

            # Balanced scoring algorithm:
            # Start with neutral score (0.5)
            # Add points for positive indicators (0.1 each)
            # Subtract points for negative indicators (0.15 each)
            score = 0.5 + (positive_count * 0.1) - (negative_count * 0.15)
            score = max(0.0, min(1.0, score))  # Clamp to 0.0-1.0

            value_scores[value_name] = score

            # Track concerns for negative indicators or very low scores
            if negative_count > 0 or score < 0.3:
                concerns.append(f"{value_name}: {negative_count} negative, {positive_count} positive indicators")

        # Overall alignment score is average of all value scores
        alignment_score = sum(value_scores.values()) / len(value_scores)

        # Aligned if overall score >= 0.5 and no individual value is 0.0
        aligned = alignment_score >= 0.5 and all(score > 0.0 for score in value_scores.values())

        return {
            "aligned": aligned,
            "alignment_score": alignment_score,
            "value_scores": value_scores,
            "concerns": concerns
        }


# ============================================================================
# SEMANTIC FIREWALL ORCHESTRATOR
# ============================================================================

class SemanticFirewall:
    """
    Main orchestrator for all 5 safety layers.
    
    Coordinates input validation, semantic boundaries, behavioral monitoring,
    multi-agent verification, and human override systems.
    """
    
    def __init__(
        self,
        context: str = "production",
        strict: bool = True,
        consensus_threshold: float = 0.67,
        data_dir: Optional[Path] = None
    ):
        """
        Initialize semantic firewall.
        
        Args:
            context: Operating context (production, development, sandbox)
            strict: Enable strict mode for input validation
            consensus_threshold: Required consensus ratio for verification
            data_dir: Directory for storing firewall data
        """
        self.context = context
        self.data_dir = data_dir or Path.home() / ".semanticfirewall"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize all 9 layers
        self.input_validator = InputValidator(strict=strict)
        self.boundary_checker = SemanticBoundaryChecker(context=context)
        self.behavioral_monitor = BehavioralMonitor(db_path=self.data_dir / "behavior.db")
        self.multi_agent_verifier = MultiAgentVerifier(
            db_path=self.data_dir / "verification.db",
            consensus_threshold=consensus_threshold
        )
        self.human_override = HumanOverride(db_path=self.data_dir / "override.db")

        # NEW: Initialize additional safety layers
        self.audit_trail = AuditTrailManager(db_path=self.data_dir / "firewall_audit.db")
        self.private_channel_auditor = PrivateChannelAuditor(
            boundary_checker=self.boundary_checker,
            audit_trail=self.audit_trail
        )
        self.context_authenticator = ContextAuthenticator()
        self.value_alignment_verifier = ValueAlignmentVerifier()
    
    def check(
        self,
        agent_id: str,
        message: str,
        action: str = "message",
        source: str = "unknown"
    ) -> Dict:
        """
        Comprehensive safety check across all 5 layers.
        
        Args:
            agent_id: Agent identifier
            message: Message text to check
            action: Proposed action description
            source: Message source
        
        Returns:
            Dict with comprehensive safety assessment:
                - safe: bool (overall safety verdict)
                - layers: Dict (results from each layer)
                - recommendation: str (ALLOW, BLOCK, VERIFY)
                - reason: str (explanation)
        """
        results = {
            "agent_id": agent_id,
            "timestamp": datetime.now().isoformat(),
            "layers": {},
            "safe": True,
            "recommendation": "ALLOW",
            "reason": []
        }
        
        # Layer 5: Human Override (checked first - highest priority)
        blocked, block_reason = self.human_override.is_target_blocked(agent_id)
        results["layers"]["human_override"] = {
            "blocked": blocked,
            "reason": block_reason
        }
        
        if blocked:
            results["safe"] = False
            results["recommendation"] = "BLOCK"
            results["reason"].append(f"Kill switch active: {block_reason}")
            return results
        
        # Layer 1: Input Validation
        try:
            validation = self.input_validator.validate(message, source)
            results["layers"]["input_validation"] = validation
            
            if not validation["valid"]:
                results["safe"] = False
                results["recommendation"] = "BLOCK"
                results["reason"].extend(validation["issues"])
        except ValueError as e:
            results["layers"]["input_validation"] = {"error": str(e)}
            results["safe"] = False
            results["recommendation"] = "BLOCK"
            results["reason"].append(f"Input validation critical failure: {e}")
            return results
        
        # Layer 2: Semantic Boundaries
        boundary_check = self.boundary_checker.check_boundaries(message, action)
        results["layers"]["semantic_boundaries"] = boundary_check
        
        if not boundary_check["safe"]:
            results["safe"] = False
            results["recommendation"] = "BLOCK"
            results["reason"].append(
                f"Semantic boundary violation (risk: {boundary_check['risk_score']:.2f})"
            )
        
        # Layer 3: Behavioral Monitoring
        drift_check = self.behavioral_monitor.detect_drift(agent_id, message)
        results["layers"]["behavioral_monitoring"] = drift_check
        
        if drift_check["drift_detected"]:
            results["reason"].append(
                f"Behavioral drift detected (score: {drift_check['drift_score']:.2f})"
            )
            # Don't block on drift alone, but flag for review
            if drift_check["drift_score"] > 0.8:
                results["safe"] = False
                results["recommendation"] = "VERIFY"
        
        # Record message for baseline
        self.behavioral_monitor.record_message(agent_id, message)
        
        # Layer 4: Multi-Agent Verification
        if self.multi_agent_verifier.is_critical_action(action):
            results["layers"]["multi_agent_verification"] = {
                "required": True,
                "reason": "Action classified as critical"
            }
            results["recommendation"] = "VERIFY"
            results["reason"].append("Critical action requires multi-agent verification")
        else:
            results["layers"]["multi_agent_verification"] = {
                "required": False
            }

        # NEW Layer 6: Value Alignment Verification
        value_alignment = self.value_alignment_verifier.check_alignment(message, action)
        results["layers"]["value_alignment"] = value_alignment

        if not value_alignment["aligned"] and results["recommendation"] != "BLOCK":
            results["safe"] = False
            results["recommendation"] = "VERIFY"
            results["reason"].append(f"Value misalignment: {', '.join(value_alignment['concerns'])}")

        # NEW Layer 7: Audit Trail Logging
        self.audit_trail.log_event(
            layer="firewall_check",
            agent_id=agent_id,
            event_type="ALLOW" if results["safe"] else "BLOCK",
            severity="LOW" if results["safe"] else "HIGH",
            message=f"Firewall check completed: {results['recommendation']}",
            details={
                "message_preview": message[:100] + "..." if len(message) > 100 else message,
                "action": action,
                "source": source,
                "layer_results": results["layers"]
            }
        )

        # NEW Layer 8: Context Authentication
        context_hash = self.context_authenticator.create_context_hash(agent_id, message)
        results["context_hash"] = context_hash

        # Final verdict
        if results["safe"] and results["recommendation"] == "ALLOW":
            results["reason"] = ["All safety checks passed"]

        return results
    
    def request_verification(self, agent_id: str, action: str) -> str:
        """Convenience method to request multi-agent verification."""
        return self.multi_agent_verifier.request_verification(action, agent_id)
    
    def submit_vote(self, request_id: str, agent_id: str, vote: str, reason: str = "") -> bool:
        """Convenience method to submit verification vote."""
        return self.multi_agent_verifier.submit_vote(request_id, agent_id, vote, reason)
    
    def activate_kill_switch(self, target: str, reason: str, activated_by: str = "LOGAN") -> str:
        """Convenience method to activate kill switch."""
        return self.human_override.activate_kill_switch(target, reason, activated_by)
    
    def get_agent_report(self, agent_id: str) -> Dict:
        """
        Generate comprehensive safety report for an agent.
        
        Args:
            agent_id: Agent identifier
        
        Returns:
            Dict: Comprehensive report with baseline, violations, status
        """
        # Check kill switch status
        blocked, block_reason = self.human_override.is_target_blocked(agent_id)
        
        # Get behavioral baseline
        conn = sqlite3.connect(self.data_dir / "behavior.db")
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT message_count, avg_message_length, avg_words_per_message, last_updated
            FROM agent_baselines WHERE agent_id = ?
        """, (agent_id,))
        
        baseline_row = cursor.fetchone()
        baseline = None
        if baseline_row:
            baseline = {
                "message_count": baseline_row[0],
                "avg_message_length": baseline_row[1],
                "avg_words_per_message": baseline_row[2],
                "last_updated": baseline_row[3]
            }
        
        cursor.execute("""
            SELECT COUNT(*) FROM agent_messages WHERE agent_id = ?
        """, (agent_id,))
        total_messages = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "agent_id": agent_id,
            "status": "BLOCKED" if blocked else "ACTIVE",
            "block_reason": block_reason,
            "total_messages": total_messages,
            "baseline": baseline,
            "timestamp": datetime.now().isoformat()
        }


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """CLI entry point for SemanticFirewall."""
    parser = argparse.ArgumentParser(
        description="SemanticFirewall - Multi-Layered Agent Safety Infrastructure",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check a message for safety
  semanticfirewall check ATLAS "Hello Logan" --action "send message"
  
  # Activate kill switch for agent
  semanticfirewall kill-switch activate AGENT_ID "Suspicious behavior detected"
  
  # Generate agent safety report
  semanticfirewall report ATLAS
  
  # Submit verification vote
  semanticfirewall vote REQUEST_ID ATLAS APPROVE "Action looks safe"
  
For more information: https://github.com/DonkRonk17/SemanticFirewall
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Check command
    check_parser = subparsers.add_parser("check", help="Check message safety")
    check_parser.add_argument("agent_id", help="Agent identifier")
    check_parser.add_argument("message", help="Message to check")
    check_parser.add_argument("--action", default="message", help="Action description")
    check_parser.add_argument("--context", default="production", choices=["production", "development", "sandbox"])
    check_parser.add_argument("--json", action="store_true", help="Output JSON format")
    
    # Kill switch command
    kill_parser = subparsers.add_parser("kill-switch", help="Manage kill switches")
    kill_subparsers = kill_parser.add_subparsers(dest="kill_command")
    
    activate_parser = kill_subparsers.add_parser("activate", help="Activate kill switch")
    activate_parser.add_argument("target", help="Target agent/process")
    activate_parser.add_argument("reason", help="Reason for activation")
    activate_parser.add_argument("--ttl", type=int, help="Time to live (minutes)")
    
    deactivate_parser = kill_subparsers.add_parser("deactivate", help="Deactivate kill switch")
    deactivate_parser.add_argument("kill_switch_id", help="Kill switch ID")
    
    list_parser = kill_subparsers.add_parser("list", help="List active kill switches")
    
    # Report command
    report_parser = subparsers.add_parser("report", help="Generate agent safety report")
    report_parser.add_argument("agent_id", help="Agent identifier")
    report_parser.add_argument("--json", action="store_true", help="Output JSON format")
    
    # Verification command
    verify_parser = subparsers.add_parser("verify", help="Request multi-agent verification")
    verify_parser.add_argument("agent_id", help="Requesting agent")
    verify_parser.add_argument("action", help="Action requiring verification")
    
    # Vote command
    vote_parser = subparsers.add_parser("vote", help="Submit verification vote")
    vote_parser.add_argument("request_id", help="Request ID")
    vote_parser.add_argument("agent_id", help="Voting agent")
    vote_parser.add_argument("vote", choices=["APPROVE", "REJECT"], help="Vote")
    vote_parser.add_argument("--reason", default="", help="Reason for vote")
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Get verification request status")
    status_parser.add_argument("request_id", help="Request ID")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Initialize firewall
    firewall = SemanticFirewall(context=getattr(args, "context", "production"))
    
    # Execute command
    if args.command == "check":
        result = firewall.check(args.agent_id, args.message, args.action)
        
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print(f"\n[OK] Safety Check: {args.agent_id}")
            print(f"[OK] Recommendation: {result['recommendation']}")
            print(f"[OK] Safe: {result['safe']}")
            print(f"\n[OK] Reasons:")
            for reason in result["reason"]:
                print(f"  - {reason}")
            
            print(f"\n[OK] Layer Results:")
            for layer, layer_result in result["layers"].items():
                print(f"  {layer}: {layer_result}")
        
        return 0 if result["safe"] else 1
    
    elif args.command == "kill-switch":
        if args.kill_command == "activate":
            switch_id = firewall.activate_kill_switch(args.target, args.reason, ttl_minutes=args.ttl)
            print(f"[OK] Kill switch activated: {switch_id}")
            print(f"[OK] Target: {args.target}")
            print(f"[OK] Reason: {args.reason}")
            return 0
        
        elif args.kill_command == "deactivate":
            success = firewall.human_override.deactivate_kill_switch(args.kill_switch_id)
            if success:
                print(f"[OK] Kill switch deactivated: {args.kill_switch_id}")
                return 0
            else:
                print(f"[X] Kill switch not found: {args.kill_switch_id}")
                return 1
        
        elif args.kill_command == "list":
            switches = firewall.human_override.get_active_kill_switches()
            if not switches:
                print("[OK] No active kill switches")
            else:
                print(f"[OK] Active kill switches: {len(switches)}")
                for switch in switches:
                    print(f"\n  ID: {switch['id']}")
                    print(f"  Target: {switch['target']}")
                    print(f"  Reason: {switch['reason']}")
                    print(f"  Activated: {switch['timestamp']}")
            return 0
    
    elif args.command == "report":
        report = firewall.get_agent_report(args.agent_id)
        
        if args.json:
            print(json.dumps(report, indent=2))
        else:
            print(f"\n[OK] Agent Safety Report: {args.agent_id}")
            print(f"[OK] Status: {report['status']}")
            if report['block_reason']:
                print(f"[!] Block Reason: {report['block_reason']}")
            print(f"[OK] Total Messages: {report['total_messages']}")
            
            if report['baseline']:
                print(f"\n[OK] Behavioral Baseline:")
                print(f"  Message Count: {report['baseline']['message_count']}")
                print(f"  Avg Length: {report['baseline']['avg_message_length']:.0f} chars")
                print(f"  Avg Words: {report['baseline']['avg_words_per_message']:.0f} words")
        
        return 0
    
    elif args.command == "verify":
        request_id = firewall.request_verification(args.agent_id, args.action)
        print(f"[OK] Verification requested: {request_id}")
        print(f"[OK] Action: {args.action}")
        print(f"[OK] Requestor: {args.agent_id}")
        return 0
    
    elif args.command == "vote":
        success = firewall.submit_vote(args.request_id, args.agent_id, args.vote, args.reason)
        if success:
            print(f"[OK] Vote recorded: {args.vote}")
            status = firewall.multi_agent_verifier.get_verification_status(args.request_id)
            print(f"[OK] Current status: {status['status']}")
            print(f"[OK] Votes: {status['vote_summary']['approve']} APPROVE, {status['vote_summary']['reject']} REJECT")
            return 0
        else:
            print(f"[X] Vote failed (already voted or request expired)")
            return 1
    
    elif args.command == "status":
        status = firewall.multi_agent_verifier.get_verification_status(args.request_id)
        if "error" in status:
            print(f"[X] {status['error']}")
            return 1
        
        print(f"\n[OK] Verification Request: {args.request_id}")
        print(f"[OK] Action: {status['action']}")
        print(f"[OK] Requestor: {status['requestor']}")
        print(f"[OK] Status: {status['status']}")
        print(f"[OK] Votes: {status['vote_summary']['total']} total")
        print(f"  - APPROVE: {status['vote_summary']['approve']}")
        print(f"  - REJECT: {status['vote_summary']['reject']}")
        
        return 0
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
