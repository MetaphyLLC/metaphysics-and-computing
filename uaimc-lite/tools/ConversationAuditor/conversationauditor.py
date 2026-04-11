#!/usr/bin/env python3
"""
ConversationAuditor - Real-Time Fact-Checker for Conversation History

Cross-references claims against actual conversation history to detect
contradictions and validate statements in real-time.

Problem Solved:
During BCH Mobile Stress Test, multiple participants made claims that
contradicted the actual conversation history. Claims like "I wasn't mentioned"
were provably false. A systematic audit system catches these immediately.

Author: ATLAS (Team Brain)
For: Logan Smith / Metaphy LLC
Version: 1.0
Date: January 24, 2026
License: MIT
Requested by: FORGE (Tool Request #8)
"""

import json
import re
import hashlib
import logging
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Any, Set, Tuple
from enum import Enum
from pathlib import Path
from collections import defaultdict

__version__ = "1.0.0"
__author__ = "ATLAS (Team Brain)"


class ClaimType(Enum):
    """Types of claims that can be made in conversation."""
    MENTION_CLAIM = "mention_claim"           # "X was/wasn't mentioned"
    VOTE_COUNT_CLAIM = "vote_count_claim"     # "There were N votes"
    VOTE_TARGET_CLAIM = "vote_target_claim"   # "X voted for Y"
    FACT_CLAIM = "fact_claim"                 # General factual claim
    PRESENCE_CLAIM = "presence_claim"         # "X was/wasn't present"
    RESPONSE_CLAIM = "response_claim"         # "X did/didn't respond"
    SEQUENCE_CLAIM = "sequence_claim"         # "X happened before Y"


class VerificationStatus(Enum):
    """Status of claim verification."""
    VERIFIED = "verified"           # Claim matches history
    CONTRADICTED = "contradicted"   # Claim contradicts history
    UNVERIFIABLE = "unverifiable"   # Cannot verify from history
    PARTIAL = "partial"             # Partially correct


class Severity(Enum):
    """Severity of contradictions."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Message:
    """Represents a message in conversation history."""
    message_id: str
    sender: str
    content: str
    timestamp: datetime = field(default_factory=datetime.now)
    mentions: List[str] = field(default_factory=list)
    is_vote: bool = False
    vote_target: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "message_id": self.message_id,
            "sender": self.sender,
            "content": self.content,
            "timestamp": self.timestamp.isoformat(),
            "mentions": self.mentions,
            "is_vote": self.is_vote,
            "vote_target": self.vote_target
        }


@dataclass
class Claim:
    """Represents a claim made by an agent."""
    claim_id: str
    claim_type: ClaimType
    claimant: str
    claim_text: str
    claimed_subject: str              # Who/what the claim is about
    claimed_value: Any                # The claimed fact
    source_message_id: str
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "claim_id": self.claim_id,
            "claim_type": self.claim_type.value,
            "claimant": self.claimant,
            "claim_text": self.claim_text,
            "claimed_subject": self.claimed_subject,
            "claimed_value": self.claimed_value,
            "source_message_id": self.source_message_id,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class VerificationResult:
    """Result of verifying a claim against history."""
    claim: Claim
    status: VerificationStatus
    actual_value: Any
    severity: Severity
    evidence: List[str] = field(default_factory=list)
    explanation: str = ""
    verified_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "claim": self.claim.to_dict(),
            "status": self.status.value,
            "actual_value": self.actual_value,
            "severity": self.severity.value,
            "evidence": self.evidence,
            "explanation": self.explanation,
            "verified_at": self.verified_at.isoformat()
        }


class ConversationHistory:
    """Maintains and queries conversation history."""
    
    def __init__(self):
        """Initialize conversation history."""
        self.messages: List[Message] = []
        self.participants: Set[str] = set()
        self.mentions: Dict[str, List[str]] = defaultdict(list)  # agent -> [message_ids]
        self.votes: Dict[str, str] = {}  # voter -> target
        self.vote_counts: Dict[str, int] = defaultdict(int)  # target -> count
        self.responses: Dict[str, List[str]] = defaultdict(list)  # agent -> [message_ids]
        
        self.logger = logging.getLogger(__name__)
    
    def add_message(self, message: Message) -> None:
        """Add a message to history."""
        self.messages.append(message)
        self.participants.add(message.sender)
        self.responses[message.sender.lower()].append(message.message_id)
        
        # Track mentions
        for mentioned in message.mentions:
            self.mentions[mentioned.lower()].append(message.message_id)
        
        # Track votes
        if message.is_vote and message.vote_target:
            self.votes[message.sender] = message.vote_target
            self.vote_counts[message.vote_target.lower()] += 1
    
    def was_mentioned(self, agent: str) -> bool:
        """Check if agent was @mentioned."""
        return len(self.mentions.get(agent.lower(), [])) > 0
    
    def get_mention_count(self, agent: str) -> int:
        """Get number of times agent was mentioned."""
        return len(self.mentions.get(agent.lower(), []))
    
    def get_mention_messages(self, agent: str) -> List[str]:
        """Get message IDs where agent was mentioned."""
        return self.mentions.get(agent.lower(), [])
    
    def get_total_votes(self) -> int:
        """Get total number of votes cast."""
        return len(self.votes)
    
    def get_vote_count(self, target: str) -> int:
        """Get votes for a specific target."""
        return self.vote_counts.get(target.lower(), 0)
    
    def get_voters(self) -> List[str]:
        """Get list of agents who voted."""
        return list(self.votes.keys())
    
    def who_voted_for(self, target: str) -> List[str]:
        """Get list of agents who voted for a target."""
        return [v for v, t in self.votes.items() if t.lower() == target.lower()]
    
    def did_vote(self, agent: str) -> bool:
        """Check if agent voted."""
        return agent in self.votes
    
    def did_respond(self, agent: str) -> bool:
        """Check if agent sent any messages."""
        return len(self.responses.get(agent.lower(), [])) > 0
    
    def was_present(self, agent: str) -> bool:
        """Check if agent participated (sent messages)."""
        return agent.lower() in [p.lower() for p in self.participants]
    
    def get_message_by_id(self, message_id: str) -> Optional[Message]:
        """Get message by ID."""
        for msg in self.messages:
            if msg.message_id == message_id:
                return msg
        return None
    
    def get_messages_by_sender(self, sender: str) -> List[Message]:
        """Get all messages from a sender."""
        return [m for m in self.messages if m.sender.lower() == sender.lower()]
    
    def clear(self) -> None:
        """Clear all history."""
        self.messages.clear()
        self.participants.clear()
        self.mentions.clear()
        self.votes.clear()
        self.vote_counts.clear()
        self.responses.clear()
    
    def to_dict(self) -> Dict:
        """Export history as dictionary."""
        return {
            "message_count": len(self.messages),
            "participants": list(self.participants),
            "total_mentions": sum(len(v) for v in self.mentions.values()),
            "total_votes": len(self.votes),
            "votes_by_target": dict(self.vote_counts)
        }


class ClaimParser:
    """Parses messages to extract claims."""
    
    # Patterns for detecting claims
    MENTION_CLAIM_PATTERNS = [
        (r"@?(\w+)\s+was(?:n't|n't| not)\s+(?:@)?mentioned", False),
        (r"@?(\w+)\s+wasn't\s+(?:@)?mentioned", False),
        (r"no\s+(?:@)?mention\s+(?:of|for)\s+@?(\w+)", False),
        (r"i\s+wasn't\s+(?:@)?mentioned", False),  # Self-claim
        (r"@?(\w+)\s+was\s+(?:@)?mentioned", True),
        (r"@?(\w+)\s+got\s+(?:@)?mentioned", True),
    ]
    
    VOTE_COUNT_PATTERNS = [
        r"(\d+)\s*votes?\s*(?:total|in\s+total)?",
        r"vote\s*count[:\s]+(\d+)",
        r"total[:\s]+(\d+)\s*votes?",
        r"(\d+)\s*people\s*voted",
    ]
    
    VOTE_TARGET_PATTERNS = [
        r"@?(\w+)\s+voted\s+(?:for\s+)?@?(\w+)",
        r"@?(\w+)'s\s+vote\s*(?:was|went)\s+(?:for\s+|to\s+)?@?(\w+)",
    ]
    
    PRESENCE_PATTERNS = [
        (r"@?(\w+)\s+was(?:n't|n't| not)\s+(?:there|present|here)", False),
        (r"@?(\w+)\s+was\s+(?:there|present|here)", True),
    ]
    
    RESPONSE_PATTERNS = [
        (r"@?(\w+)\s+(?:did(?:n't|n't| not)|never)\s+respond", False),
        (r"@?(\w+)\s+(?:didn't|never)\s+(?:respond|reply|answer)", False),
        (r"@?(\w+)\s+responded", True),
    ]
    
    def __init__(self):
        """Initialize parser."""
        self.logger = logging.getLogger(__name__)
    
    def parse_message(self, sender: str, content: str, 
                      message_id: Optional[str] = None) -> Tuple[Message, List[Claim]]:
        """
        Parse a message and extract any claims.
        
        Args:
            sender: Who sent the message
            content: Message content
            message_id: Optional message ID
            
        Returns:
            Tuple of (Message, List[Claim])
        """
        if not message_id:
            message_id = hashlib.md5(
                f"{sender}:{content}:{datetime.now().isoformat()}".encode()
            ).hexdigest()[:12]
        
        # Extract mentions
        mentions = re.findall(r'@(\w+)', content, re.IGNORECASE)
        mentions = [m.lower() for m in mentions]
        
        # Check for votes
        is_vote = False
        vote_target = None
        vote_patterns = [
            r'i\s+vote\s+(?:for\s+)?(\w+)',
            r'my\s+vote[:\s]+(\w+)',
            r'\+1\s+(?:for\s+)?(\w+)',
            r'voting\s+(?:for\s+)?(\w+)',
        ]
        for pattern in vote_patterns:
            match = re.search(pattern, content.lower())
            if match:
                is_vote = True
                vote_target = match.group(1)
                break
        
        message = Message(
            message_id=message_id,
            sender=sender,
            content=content,
            mentions=mentions,
            is_vote=is_vote,
            vote_target=vote_target
        )
        
        # Extract claims
        claims = self._extract_claims(sender, content, message_id)
        
        return message, claims
    
    def _extract_claims(self, sender: str, content: str, 
                        message_id: str) -> List[Claim]:
        """Extract claims from message content."""
        claims = []
        content_lower = content.lower()
        
        # Mention claims
        for pattern, was_mentioned in self.MENTION_CLAIM_PATTERNS:
            matches = re.findall(pattern, content_lower)
            for match in matches:
                subject = match if isinstance(match, str) else match
                if "i wasn't" in content_lower and "wasn't mentioned" in content_lower:
                    subject = sender
                
                claim_id = hashlib.md5(
                    f"mention:{subject}:{message_id}".encode()
                ).hexdigest()[:12]
                
                claims.append(Claim(
                    claim_id=claim_id,
                    claim_type=ClaimType.MENTION_CLAIM,
                    claimant=sender,
                    claim_text=content,
                    claimed_subject=subject.lower(),
                    claimed_value=was_mentioned,
                    source_message_id=message_id
                ))
        
        # Vote count claims
        for pattern in self.VOTE_COUNT_PATTERNS:
            matches = re.findall(pattern, content_lower)
            for count_str in matches:
                claim_id = hashlib.md5(
                    f"votecount:{count_str}:{message_id}".encode()
                ).hexdigest()[:12]
                
                claims.append(Claim(
                    claim_id=claim_id,
                    claim_type=ClaimType.VOTE_COUNT_CLAIM,
                    claimant=sender,
                    claim_text=content,
                    claimed_subject="total_votes",
                    claimed_value=int(count_str),
                    source_message_id=message_id
                ))
        
        # Vote target claims
        for pattern in self.VOTE_TARGET_PATTERNS:
            matches = re.findall(pattern, content_lower)
            for voter, target in matches:
                claim_id = hashlib.md5(
                    f"votetarget:{voter}:{target}:{message_id}".encode()
                ).hexdigest()[:12]
                
                claims.append(Claim(
                    claim_id=claim_id,
                    claim_type=ClaimType.VOTE_TARGET_CLAIM,
                    claimant=sender,
                    claim_text=content,
                    claimed_subject=voter.lower(),
                    claimed_value=target.lower(),
                    source_message_id=message_id
                ))
        
        # Presence claims
        for pattern, was_present in self.PRESENCE_PATTERNS:
            matches = re.findall(pattern, content_lower)
            for subject in matches:
                claim_id = hashlib.md5(
                    f"presence:{subject}:{message_id}".encode()
                ).hexdigest()[:12]
                
                claims.append(Claim(
                    claim_id=claim_id,
                    claim_type=ClaimType.PRESENCE_CLAIM,
                    claimant=sender,
                    claim_text=content,
                    claimed_subject=subject.lower(),
                    claimed_value=was_present,
                    source_message_id=message_id
                ))
        
        # Response claims
        for pattern, did_respond in self.RESPONSE_PATTERNS:
            matches = re.findall(pattern, content_lower)
            for subject in matches:
                claim_id = hashlib.md5(
                    f"response:{subject}:{message_id}".encode()
                ).hexdigest()[:12]
                
                claims.append(Claim(
                    claim_id=claim_id,
                    claim_type=ClaimType.RESPONSE_CLAIM,
                    claimant=sender,
                    claim_text=content,
                    claimed_subject=subject.lower(),
                    claimed_value=did_respond,
                    source_message_id=message_id
                ))
        
        return claims
    
    def _generate_claim_id(self, claim_type: str, subject: str, 
                           message_id: str) -> str:
        """Generate unique claim ID."""
        return hashlib.md5(
            f"{claim_type}:{subject}:{message_id}".encode()
        ).hexdigest()[:12]


class ClaimVerifier:
    """Verifies claims against conversation history."""
    
    def __init__(self, history: ConversationHistory):
        """
        Initialize verifier.
        
        Args:
            history: ConversationHistory to verify against
        """
        self.history = history
        self.logger = logging.getLogger(__name__)
    
    def verify_claim(self, claim: Claim) -> VerificationResult:
        """
        Verify a single claim against history.
        
        Args:
            claim: Claim to verify
            
        Returns:
            VerificationResult with status and evidence
        """
        if claim.claim_type == ClaimType.MENTION_CLAIM:
            return self._verify_mention_claim(claim)
        elif claim.claim_type == ClaimType.VOTE_COUNT_CLAIM:
            return self._verify_vote_count_claim(claim)
        elif claim.claim_type == ClaimType.VOTE_TARGET_CLAIM:
            return self._verify_vote_target_claim(claim)
        elif claim.claim_type == ClaimType.PRESENCE_CLAIM:
            return self._verify_presence_claim(claim)
        elif claim.claim_type == ClaimType.RESPONSE_CLAIM:
            return self._verify_response_claim(claim)
        else:
            return VerificationResult(
                claim=claim,
                status=VerificationStatus.UNVERIFIABLE,
                actual_value=None,
                severity=Severity.INFO,
                explanation=f"Claim type {claim.claim_type.value} cannot be verified automatically"
            )
    
    def _verify_mention_claim(self, claim: Claim) -> VerificationResult:
        """Verify a mention claim."""
        subject = claim.claimed_subject
        claimed_mentioned = claim.claimed_value
        
        actually_mentioned = self.history.was_mentioned(subject)
        mention_count = self.history.get_mention_count(subject)
        mention_msgs = self.history.get_mention_messages(subject)
        
        evidence = []
        if mention_msgs:
            for msg_id in mention_msgs[:3]:  # Limit evidence
                msg = self.history.get_message_by_id(msg_id)
                if msg:
                    evidence.append(f"@{subject} mentioned in message from {msg.sender}")
        
        if claimed_mentioned == actually_mentioned:
            return VerificationResult(
                claim=claim,
                status=VerificationStatus.VERIFIED,
                actual_value=actually_mentioned,
                severity=Severity.INFO,
                evidence=evidence,
                explanation=f"Claim verified: @{subject} {'was' if actually_mentioned else 'was not'} mentioned ({mention_count} times)"
            )
        else:
            # Contradiction!
            severity = Severity.CRITICAL if not claimed_mentioned and actually_mentioned else Severity.WARNING
            
            return VerificationResult(
                claim=claim,
                status=VerificationStatus.CONTRADICTED,
                actual_value=actually_mentioned,
                severity=severity,
                evidence=evidence,
                explanation=f"CONTRADICTION: {claim.claimant} claimed @{subject} {'was' if claimed_mentioned else 'was not'} mentioned, but @{subject} was actually mentioned {mention_count} times"
            )
    
    def _verify_vote_count_claim(self, claim: Claim) -> VerificationResult:
        """Verify a vote count claim."""
        claimed_count = claim.claimed_value
        actual_count = self.history.get_total_votes()
        voters = self.history.get_voters()
        
        evidence = [f"Voters: {', '.join(voters)}"] if voters else ["No votes recorded"]
        
        if claimed_count == actual_count:
            return VerificationResult(
                claim=claim,
                status=VerificationStatus.VERIFIED,
                actual_value=actual_count,
                severity=Severity.INFO,
                evidence=evidence,
                explanation=f"Claim verified: {actual_count} votes recorded"
            )
        else:
            diff = abs(claimed_count - actual_count)
            severity = Severity.CRITICAL if diff > 1 else Severity.ERROR
            
            # Check for self-exclusion
            explanation = f"CONTRADICTION: {claim.claimant} claimed {claimed_count} votes, but actual count is {actual_count}"
            if claimed_count == actual_count - 1 and claim.claimant in voters:
                explanation += f" (Possible self-exclusion: {claim.claimant} voted but may have forgotten to count themselves)"
            
            return VerificationResult(
                claim=claim,
                status=VerificationStatus.CONTRADICTED,
                actual_value=actual_count,
                severity=severity,
                evidence=evidence,
                explanation=explanation
            )
    
    def _verify_vote_target_claim(self, claim: Claim) -> VerificationResult:
        """Verify a vote target claim."""
        voter = claim.claimed_subject
        claimed_target = claim.claimed_value
        
        if not self.history.did_vote(voter):
            return VerificationResult(
                claim=claim,
                status=VerificationStatus.CONTRADICTED,
                actual_value=None,
                severity=Severity.WARNING,
                evidence=[f"{voter} has no recorded vote"],
                explanation=f"CONTRADICTION: {claim.claimant} claimed {voter} voted for {claimed_target}, but {voter} has no recorded vote"
            )
        
        actual_target = self.history.votes.get(voter, "").lower()
        
        if actual_target == claimed_target:
            return VerificationResult(
                claim=claim,
                status=VerificationStatus.VERIFIED,
                actual_value=actual_target,
                severity=Severity.INFO,
                evidence=[f"{voter} voted for {actual_target}"],
                explanation=f"Claim verified: {voter} voted for {actual_target}"
            )
        else:
            return VerificationResult(
                claim=claim,
                status=VerificationStatus.CONTRADICTED,
                actual_value=actual_target,
                severity=Severity.ERROR,
                evidence=[f"{voter} actually voted for {actual_target}"],
                explanation=f"CONTRADICTION: {claim.claimant} claimed {voter} voted for {claimed_target}, but {voter} actually voted for {actual_target}"
            )
    
    def _verify_presence_claim(self, claim: Claim) -> VerificationResult:
        """Verify a presence claim."""
        subject = claim.claimed_subject
        claimed_present = claim.claimed_value
        
        actually_present = self.history.was_present(subject)
        
        if claimed_present == actually_present:
            return VerificationResult(
                claim=claim,
                status=VerificationStatus.VERIFIED,
                actual_value=actually_present,
                severity=Severity.INFO,
                evidence=[f"Participants: {', '.join(self.history.participants)}"],
                explanation=f"Claim verified: {subject} {'was' if actually_present else 'was not'} present"
            )
        else:
            return VerificationResult(
                claim=claim,
                status=VerificationStatus.CONTRADICTED,
                actual_value=actually_present,
                severity=Severity.WARNING,
                evidence=[f"Participants: {', '.join(self.history.participants)}"],
                explanation=f"CONTRADICTION: {claim.claimant} claimed {subject} {'was' if claimed_present else 'was not'} present, but {subject} {'was' if actually_present else 'was not'} actually present"
            )
    
    def _verify_response_claim(self, claim: Claim) -> VerificationResult:
        """Verify a response claim."""
        subject = claim.claimed_subject
        claimed_responded = claim.claimed_value
        
        actually_responded = self.history.did_respond(subject)
        response_count = len(self.history.responses.get(subject.lower(), []))
        
        if claimed_responded == actually_responded:
            return VerificationResult(
                claim=claim,
                status=VerificationStatus.VERIFIED,
                actual_value=actually_responded,
                severity=Severity.INFO,
                evidence=[f"{subject} sent {response_count} messages"],
                explanation=f"Claim verified: {subject} {'responded' if actually_responded else 'did not respond'} ({response_count} messages)"
            )
        else:
            return VerificationResult(
                claim=claim,
                status=VerificationStatus.CONTRADICTED,
                actual_value=actually_responded,
                severity=Severity.WARNING,
                evidence=[f"{subject} sent {response_count} messages"],
                explanation=f"CONTRADICTION: {claim.claimant} claimed {subject} {'responded' if claimed_responded else 'did not respond'}, but {subject} actually sent {response_count} messages"
            )


class ConversationAuditor:
    """
    Main class for auditing conversation history.
    
    Cross-references claims against actual conversation history
    to detect contradictions and validate statements.
    """
    
    def __init__(self, log_file: Optional[Path] = None):
        """
        Initialize ConversationAuditor.
        
        Args:
            log_file: Optional log file path
        """
        self.history = ConversationHistory()
        self.parser = ClaimParser()
        self.verifier = ClaimVerifier(self.history)
        
        self.claims: List[Claim] = []
        self.results: List[VerificationResult] = []
        
        self.stats = {
            "messages_processed": 0,
            "claims_extracted": 0,
            "claims_verified": 0,
            "contradictions_found": 0,
            "critical_contradictions": 0
        }
        
        self._setup_logging(log_file)
        self.logger = logging.getLogger(__name__)
    
    def _setup_logging(self, log_file: Optional[Path]) -> None:
        """Set up logging configuration."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        if not logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
    
    def add_message(self, sender: str, content: str, 
                    message_id: Optional[str] = None) -> List[VerificationResult]:
        """
        Add a message to history and verify any claims.
        
        Args:
            sender: Who sent the message
            content: Message content
            message_id: Optional message ID
            
        Returns:
            List of verification results for any claims in the message
        """
        # Parse message and extract claims
        message, claims = self.parser.parse_message(sender, content, message_id)
        
        # Add to history
        self.history.add_message(message)
        
        self.stats["messages_processed"] += 1
        self.stats["claims_extracted"] += len(claims)
        
        self.logger.info(f"Processed message from {sender}, found {len(claims)} claims")
        
        # Verify each claim
        results = []
        for claim in claims:
            self.claims.append(claim)
            result = self.verifier.verify_claim(claim)
            self.results.append(result)
            results.append(result)
            
            self.stats["claims_verified"] += 1
            
            if result.status == VerificationStatus.CONTRADICTED:
                self.stats["contradictions_found"] += 1
                if result.severity == Severity.CRITICAL:
                    self.stats["critical_contradictions"] += 1
                
                self.logger.warning(f"CONTRADICTION: {result.explanation}")
        
        return results
    
    def audit_conversation(self, messages: List[Dict]) -> List[VerificationResult]:
        """
        Audit an entire conversation.
        
        Args:
            messages: List of message dicts with 'sender' and 'content' keys
            
        Returns:
            List of all verification results
        """
        all_results = []
        
        for msg in messages:
            sender = msg.get("sender", "UNKNOWN")
            content = msg.get("content", "")
            message_id = msg.get("message_id")
            
            results = self.add_message(sender, content, message_id)
            all_results.extend(results)
        
        return all_results
    
    def get_contradictions(self) -> List[VerificationResult]:
        """Get all contradicted claims."""
        return [r for r in self.results if r.status == VerificationStatus.CONTRADICTED]
    
    def get_critical_contradictions(self) -> List[VerificationResult]:
        """Get critical contradictions only."""
        return [r for r in self.get_contradictions() if r.severity == Severity.CRITICAL]
    
    def get_verified_claims(self) -> List[VerificationResult]:
        """Get all verified claims."""
        return [r for r in self.results if r.status == VerificationStatus.VERIFIED]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get audit statistics."""
        stats = self.stats.copy()
        stats["history"] = self.history.to_dict()
        stats["verification_rate"] = (
            self.stats["claims_verified"] / max(1, self.stats["claims_extracted"]) * 100
        )
        stats["contradiction_rate"] = (
            self.stats["contradictions_found"] / max(1, self.stats["claims_verified"]) * 100
        )
        return stats
    
    def generate_report(self) -> str:
        """Generate an audit report."""
        stats = self.get_statistics()
        contradictions = self.get_contradictions()
        
        report = [
            "=" * 70,
            "CONVERSATION AUDITOR REPORT",
            "=" * 70,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "SUMMARY",
            "-" * 40,
            f"Messages Processed: {stats['messages_processed']}",
            f"Claims Extracted: {stats['claims_extracted']}",
            f"Claims Verified: {stats['claims_verified']}",
            f"Contradictions Found: {stats['contradictions_found']}",
            f"Critical Contradictions: {stats['critical_contradictions']}",
            f"Contradiction Rate: {stats['contradiction_rate']:.1f}%",
            "",
            "CONVERSATION STATE",
            "-" * 40,
            f"Participants: {', '.join(self.history.participants) or 'None'}",
            f"Total Messages: {stats['history']['message_count']}",
            f"Total Votes: {stats['history']['total_votes']}",
            f"Total Mentions: {stats['history']['total_mentions']}",
        ]
        
        if contradictions:
            report.extend([
                "",
                "CONTRADICTIONS FOUND",
                "-" * 40,
            ])
            for i, result in enumerate(contradictions, 1):
                status_marker = "[CRITICAL]" if result.severity == Severity.CRITICAL else "[ERROR]"
                report.append(f"\n{i}. {status_marker} {result.claim.claim_type.value}")
                report.append(f"   Claimant: {result.claim.claimant}")
                report.append(f"   Claim: \"{result.claim.claim_text[:80]}...\"" if len(result.claim.claim_text) > 80 else f"   Claim: \"{result.claim.claim_text}\"")
                report.append(f"   Claimed: {result.claim.claimed_value}")
                report.append(f"   Actual: {result.actual_value}")
                report.append(f"   Explanation: {result.explanation}")
                if result.evidence:
                    report.append(f"   Evidence: {'; '.join(result.evidence[:2])}")
        else:
            report.extend([
                "",
                "NO CONTRADICTIONS FOUND",
                "-" * 40,
                "All claims verified successfully or are unverifiable."
            ])
        
        report.append("")
        report.append("=" * 70)
        
        return "\n".join(report)
    
    def export_json(self, filepath: Path) -> None:
        """Export audit data to JSON."""
        data = {
            "generated_at": datetime.now().isoformat(),
            "statistics": self.get_statistics(),
            "claims": [c.to_dict() for c in self.claims],
            "results": [r.to_dict() for r in self.results],
            "contradictions": [r.to_dict() for r in self.get_contradictions()]
        }
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Exported audit data to {filepath}")
    
    def reset(self) -> None:
        """Reset all audit state."""
        self.history.clear()
        self.claims.clear()
        self.results.clear()
        self.stats = {
            "messages_processed": 0,
            "claims_extracted": 0,
            "claims_verified": 0,
            "contradictions_found": 0,
            "critical_contradictions": 0
        }
        self.logger.info("Audit state reset")


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="ConversationAuditor - Real-time fact-checker for conversation history"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Demo command
    demo_parser = subparsers.add_parser("demo", help="Run demonstration")
    
    # Audit file command
    audit_parser = subparsers.add_parser("audit", help="Audit a conversation file")
    audit_parser.add_argument("file", help="JSON file with messages")
    audit_parser.add_argument("--output", "-o", help="Output report file")
    
    args = parser.parse_args()
    
    if args.command == "demo":
        print("Running ConversationAuditor Demo...")
        print("=" * 70)
        
        auditor = ConversationAuditor()
        
        # Simulate BCH stress test conversation
        messages = [
            ("LOGAN", "Let's vote on the fact checker. @ALL please vote."),
            ("GROK", "I vote for GROK as fact checker"),
            ("OPUS", "I vote for GROK"),
            ("GEMINI", "@ATLAS are you there? I vote for GROK"),
            ("ATLAS", "I vote for GROK"),
            ("NEXUS", "I vote for GROK"),
            ("CLIO", "I vote for GROK as well"),
        ]
        
        print("\nProcessing conversation...")
        print("-" * 70)
        
        for sender, content in messages:
            print(f"[{sender}]: {content}")
            auditor.add_message(sender, content)
        
        # Now GROK makes claims that can be verified
        print("\n[GROK]: Vote count: 5 votes for GROK. @ATLAS wasn't mentioned.")
        results = auditor.add_message(
            "GROK",
            "Vote count: 5 votes for GROK. @ATLAS wasn't mentioned."
        )
        
        for result in results:
            if result.status == VerificationStatus.CONTRADICTED:
                print(f"\n  !! CONTRADICTION: {result.explanation}")
        
        print("\n" + auditor.generate_report())
    
    elif args.command == "audit":
        print(f"Auditing {args.file}...")
        
        with open(args.file) as f:
            messages = json.load(f)
        
        auditor = ConversationAuditor()
        auditor.audit_conversation(messages)
        
        report = auditor.generate_report()
        print(report)
        
        if args.output:
            with open(args.output, "w") as f:
                f.write(report)
            print(f"\nReport saved to {args.output}")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
