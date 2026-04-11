#!/usr/bin/env python3
"""
TokenTracker v1.0 - Real-time Token Usage Monitor for Team Brain

Zero dependencies, cross-platform token tracking for AI agents.
Monitors usage, enforces budgets, generates reports.

Author: Team Brain (Atlas)
License: MIT
"""

import json
import sqlite3
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

__version__ = "1.0.0"


class TokenTracker:
    """Token usage tracking and budget management for Team Brain."""
    
    # Known AI agents in Team Brain
    AGENTS = {
        "FORGE": "Opus 4.5 (Orchestrator)",
        "ATLAS": "Sonnet 4.5 (Executor/Builder)",
        "CLIO": "Linux/Ubuntu Agent",
        "NEXUS": "Multi-platform Agent",
        "BOLT": "Cline/Grok (Free Executor)",
        "GEMINI": "Extension Agent",
        "LOGAN": "Human oversight"
    }
    
    # Default monthly budget in USD
    DEFAULT_BUDGET = 60.00
    
    # Approximate token costs (USD per 1M tokens)
    # Based on Claude pricing as of Jan 2026
    TOKEN_COSTS = {
        "opus-4.5": {"input": 15.00, "output": 75.00},
        "sonnet-4.5": {"input": 3.00, "output": 15.00},
        "sonnet-3.5": {"input": 3.00, "output": 15.00},
        "haiku-3.5": {"input": 0.80, "output": 4.00},
        "grok": {"input": 0.00, "output": 0.00},  # Free tier
        "gemini": {"input": 0.00, "output": 0.00}  # Using extension
    }
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize TokenTracker
        
        Args:
            db_path: Optional custom path for SQLite database
        """
        if db_path is None:
            # Default: store in same directory as script
            db_path = Path(__file__).parent / "token_usage.db"
        
        self.db_path = Path(db_path)
        self._init_database()
    
    def _validate_agent(self, agent: str) -> str:
        """Validate and normalize agent name."""
        if not agent or not agent.strip():
            raise ValueError("Agent name cannot be empty.")
        
        agent_upper = agent.strip().upper()
        
        # Check for suspicious characters (basic SQL injection prevention)
        if any(char in agent for char in [';', '--', '/*', '*/', 'DROP', 'DELETE', 'INSERT', 'UPDATE']):
            raise ValueError(f"Invalid characters in agent name: {agent}")
        
        # Warn if not a known agent
        if agent_upper not in self.AGENTS:
            print(f"[WARNING] Unknown agent: {agent_upper} (will be logged anyway)")
        
        return agent_upper
    
    def _validate_model(self, model: str) -> str:
        """Validate and normalize model name."""
        if not model or not model.strip():
            raise ValueError("Model name cannot be empty.")
        
        model_lower = model.strip().lower()
        
        if model_lower not in self.TOKEN_COSTS:
            print(f"[WARNING] Unknown model: {model_lower}, using default sonnet-4.5 pricing")
            # Don't fail, just warn
        
        return model_lower
    
    def _validate_tokens(self, input_tokens: int, output_tokens: int) -> Tuple[int, int]:
        """Validate token counts."""
        if input_tokens < 0:
            raise ValueError(f"Input tokens cannot be negative: {input_tokens}")
        if output_tokens < 0:
            raise ValueError(f"Output tokens cannot be negative: {output_tokens}")
        
        # Sanity check: no single session should exceed 10M tokens
        MAX_TOKENS = 10_000_000
        if input_tokens > MAX_TOKENS:
            raise ValueError(f"Input tokens exceed maximum ({MAX_TOKENS}): {input_tokens}")
        if output_tokens > MAX_TOKENS:
            raise ValueError(f"Output tokens exceed maximum ({MAX_TOKENS}): {output_tokens}")
        
        return input_tokens, output_tokens
    
    def _validate_month(self, month: str) -> str:
        """Validate month format (YYYY-MM)."""
        import re
        if not re.match(r'^\d{4}-\d{2}$', month):
            raise ValueError(f"Invalid month format (use YYYY-MM): {month}")
        
        # Parse to ensure valid date
        try:
            year, mon = month.split('-')
            year_int, mon_int = int(year), int(mon)
            if not (1900 <= year_int <= 2100):
                raise ValueError(f"Year out of range: {year_int}")
            if not (1 <= mon_int <= 12):
                raise ValueError(f"Month out of range: {mon_int}")
        except Exception as e:
            raise ValueError(f"Invalid month format: {month} ({e})")
        
        return month
    
    def _validate_budget(self, amount: float) -> float:
        """Validate budget amount."""
        if amount < 0:
            raise ValueError(f"Budget cannot be negative: {amount}")
        if amount > 100000:  # Sanity check
            raise ValueError(f"Budget exceeds reasonable limit ($100k): {amount}")
        
        return amount
    
    def _init_database(self):
        """Initialize SQLite database with required tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Token usage log table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS usage_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                agent TEXT NOT NULL,
                model TEXT NOT NULL,
                input_tokens INTEGER NOT NULL,
                output_tokens INTEGER NOT NULL,
                total_tokens INTEGER NOT NULL,
                cost_usd REAL NOT NULL,
                session_id TEXT,
                notes TEXT
            )
        """)
        
        # Budget tracking table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS budget (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month TEXT NOT NULL UNIQUE,
                budget_usd REAL NOT NULL,
                spent_usd REAL NOT NULL DEFAULT 0.0
            )
        """)
        
        # Agent profiles table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agents (
                agent_name TEXT PRIMARY KEY,
                model TEXT NOT NULL,
                description TEXT,
                active INTEGER DEFAULT 1
            )
        """)
        
        conn.commit()
        conn.close()
    
    def log_usage(
        self,
        agent: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        session_id: Optional[str] = None,
        notes: Optional[str] = None
    ) -> int:
        """
        Log token usage for an agent
        
        Args:
            agent: Agent name (FORGE, ATLAS, etc.)
            model: Model used (opus-4.5, sonnet-4.5, etc.)
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            session_id: Optional session identifier
            notes: Optional notes about this usage
        
        Returns:
            Log entry ID
        
        Raises:
            ValueError: If validation fails
        """
        # Validate all inputs
        agent = self._validate_agent(agent)
        model = self._validate_model(model)
        input_tokens, output_tokens = self._validate_tokens(input_tokens, output_tokens)
        
        # Truncate notes if too long
        if notes and len(notes) > 1000:
            notes = notes[:997] + "..."
            print("[WARNING] Notes truncated to 1000 characters")
        
        # Calculate cost
        total_tokens = input_tokens + output_tokens
        cost = self._calculate_cost(model, input_tokens, output_tokens)
        
        # Insert log entry
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        timestamp = datetime.now().isoformat()
        
        cursor.execute("""
            INSERT INTO usage_log 
            (timestamp, agent, model, input_tokens, output_tokens, total_tokens, cost_usd, session_id, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (timestamp, agent, model, input_tokens, output_tokens, total_tokens, cost, session_id, notes))
        
        log_id = cursor.lastrowid
        
        # Update monthly budget spent
        current_month = datetime.now().strftime("%Y-%m")
        cursor.execute("""
            INSERT INTO budget (month, budget_usd, spent_usd)
            VALUES (?, ?, ?)
            ON CONFLICT(month) DO UPDATE SET spent_usd = spent_usd + ?
        """, (current_month, self.DEFAULT_BUDGET, cost, cost))
        
        conn.commit()
        conn.close()
        
        print(f"[OK] Logged {total_tokens:,} tokens ({model}) for {agent} - ${cost:.4f}")
        return log_id
    
    def _calculate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost in USD for given token usage."""
        if model not in self.TOKEN_COSTS:
            print(f"[WARNING] Unknown model: {model}, using default sonnet-4.5 pricing")
            model = "sonnet-4.5"
        
        pricing = self.TOKEN_COSTS[model]
        input_cost = (input_tokens / 1_000_000) * pricing["input"]
        output_cost = (output_tokens / 1_000_000) * pricing["output"]
        
        return input_cost + output_cost
    
    def get_usage_summary(self, period: str = "today") -> Dict:
        """
        Get usage summary for specified period
        
        Args:
            period: "today", "week", "month", or "all"
        
        Returns:
            Dictionary with usage statistics
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Determine time filter
        if period == "today":
            start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        elif period == "week":
            start_date = datetime.now() - timedelta(days=7)
        elif period == "month":
            start_date = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:  # all
            start_date = datetime(2020, 1, 1)
        
        start_date_str = start_date.isoformat()
        
        # Get total usage
        cursor.execute("""
            SELECT 
                COUNT(*) as sessions,
                SUM(input_tokens) as total_input,
                SUM(output_tokens) as total_output,
                SUM(total_tokens) as total_tokens,
                SUM(cost_usd) as total_cost
            FROM usage_log
            WHERE timestamp >= ?
        """, (start_date_str,))
        
        row = cursor.fetchone()
        
        # Get per-agent breakdown
        cursor.execute("""
            SELECT 
                agent,
                COUNT(*) as sessions,
                SUM(total_tokens) as total_tokens,
                SUM(cost_usd) as total_cost
            FROM usage_log
            WHERE timestamp >= ?
            GROUP BY agent
            ORDER BY total_cost DESC
        """, (start_date_str,))
        
        agents = []
        for row2 in cursor.fetchall():
            agents.append({
                "agent": row2[0],
                "sessions": row2[1],
                "tokens": row2[2],
                "cost": row2[3]
            })
        
        # Get per-model breakdown
        cursor.execute("""
            SELECT 
                model,
                SUM(total_tokens) as total_tokens,
                SUM(cost_usd) as total_cost
            FROM usage_log
            WHERE timestamp >= ?
            GROUP BY model
            ORDER BY total_cost DESC
        """, (start_date_str,))
        
        models = []
        for row3 in cursor.fetchall():
            models.append({
                "model": row3[0],
                "tokens": row3[1],
                "cost": row3[2]
            })
        
        conn.close()
        
        return {
            "period": period,
            "start_date": start_date_str,
            "sessions": row[0] or 0,
            "input_tokens": row[1] or 0,
            "output_tokens": row[2] or 0,
            "total_tokens": row[3] or 0,
            "total_cost": row[4] or 0.0,
            "agents": agents,
            "models": models
        }
    
    def get_budget_status(self) -> Dict:
        """Get current month's budget status."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        current_month = datetime.now().strftime("%Y-%m")
        
        cursor.execute("""
            SELECT budget_usd, spent_usd
            FROM budget
            WHERE month = ?
        """, (current_month,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            budget, spent = row
        else:
            budget = self.DEFAULT_BUDGET
            spent = 0.0
        
        remaining = budget - spent
        percent_used = (spent / budget * 100) if budget > 0 else 0
        
        return {
            "month": current_month,
            "budget": budget,
            "spent": spent,
            "remaining": remaining,
            "percent_used": percent_used,
            "on_track": percent_used < 80  # Alert if over 80%
        }
    
    def set_budget(self, month: str, amount: float):
        """
        Set budget for a specific month (format: YYYY-MM).
        
        Raises:
            ValueError: If validation fails
        """
        month = self._validate_month(month)
        amount = self._validate_budget(amount)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO budget (month, budget_usd, spent_usd)
            VALUES (?, ?, 0.0)
            ON CONFLICT(month) DO UPDATE SET budget_usd = ?
        """, (month, amount, amount))
        
        conn.commit()
        conn.close()
        
        print(f"[OK] Set budget for {month}: ${amount:.2f}")
    
    def export_report(self, period: str = "month", format: str = "json") -> str:
        """
        Export usage report
        
        Args:
            period: "today", "week", "month", or "all"
            format: "json" or "text"
        
        Returns:
            Formatted report string
        """
        summary = self.get_usage_summary(period)
        budget = self.get_budget_status()
        
        if format == "json":
            report = {
                "usage": summary,
                "budget": budget,
                "generated_at": datetime.now().isoformat()
            }
            return json.dumps(report, indent=2)
        
        else:  # text format
            lines = []
            lines.append("=" * 60)
            lines.append(f"TOKEN TRACKER REPORT - {period.upper()}")
            lines.append("=" * 60)
            lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            lines.append("")
            
            lines.append("BUDGET STATUS:")
            lines.append(f"  Month: {budget['month']}")
            lines.append(f"  Budget: ${budget['budget']:.2f}")
            lines.append(f"  Spent: ${budget['spent']:.2f}")
            lines.append(f"  Remaining: ${budget['remaining']:.2f}")
            lines.append(f"  Usage: {budget['percent_used']:.1f}%")
            lines.append(f"  Status: {'[OK] On Track' if budget['on_track'] else '[WARNING] Over Budget!'}")
            lines.append("")
            
            lines.append("USAGE SUMMARY:")
            lines.append(f"  Sessions: {summary['sessions']}")
            lines.append(f"  Input Tokens: {summary['input_tokens']:,}")
            lines.append(f"  Output Tokens: {summary['output_tokens']:,}")
            lines.append(f"  Total Tokens: {summary['total_tokens']:,}")
            lines.append(f"  Total Cost: ${summary['total_cost']:.2f}")
            lines.append("")
            
            if summary['agents']:
                lines.append("BY AGENT:")
                for agent in summary['agents']:
                    lines.append(f"  {agent['agent']:10} | {agent['tokens']:>12,} tokens | ${agent['cost']:>8.2f} | {agent['sessions']:>3} sessions")
                lines.append("")
            
            if summary['models']:
                lines.append("BY MODEL:")
                for model in summary['models']:
                    lines.append(f"  {model['model']:12} | {model['tokens']:>12,} tokens | ${model['cost']:>8.2f}")
                lines.append("")
            
            lines.append("=" * 60)
            
            return "\n".join(lines)


def main():
    """CLI interface for TokenTracker."""
    
    if len(sys.argv) < 2:
        print("""
TokenTracker v1.0 - Token Usage Monitor for Team Brain

USAGE:
  tokentracker.py log <agent> <model> <input_tokens> <output_tokens> [notes]
  tokentracker.py summary [today|week|month|all]
  tokentracker.py budget
  tokentracker.py set-budget <YYYY-MM> <amount>
  tokentracker.py report [today|week|month|all] [json|text]

EXAMPLES:
  # Log token usage
  tokentracker.py log ATLAS sonnet-4.5 50000 15000 "Built TokenTracker"
  
  # View summary
  tokentracker.py summary today
  tokentracker.py summary month
  
  # Check budget
  tokentracker.py budget
  
  # Set budget
  tokentracker.py set-budget 2026-01 60.00
  
  # Export report
  tokentracker.py report month text
  tokentracker.py report all json > report.json

AGENTS: FORGE, ATLAS, CLIO, NEXUS, BOLT, GEMINI
MODELS: opus-4.5, sonnet-4.5, sonnet-3.5, haiku-3.5, grok, gemini
""")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    tracker = TokenTracker()
    
    if command == "log":
        if len(sys.argv) < 6:
            print("[ERROR] Usage: tokentracker.py log <agent> <model> <input_tokens> <output_tokens> [notes]")
            sys.exit(1)
        
        agent = sys.argv[2]
        model = sys.argv[3]
        input_tokens = int(sys.argv[4])
        output_tokens = int(sys.argv[5])
        notes = " ".join(sys.argv[6:]) if len(sys.argv) > 6 else None
        
        tracker.log_usage(agent, model, input_tokens, output_tokens, notes=notes)
    
    elif command == "summary":
        period = sys.argv[2] if len(sys.argv) > 2 else "month"
        summary = tracker.get_usage_summary(period)
        
        print(f"\n=== TOKEN USAGE SUMMARY ({period.upper()}) ===")
        print(f"Sessions: {summary['sessions']}")
        print(f"Total Tokens: {summary['total_tokens']:,}")
        print(f"Total Cost: ${summary['total_cost']:.2f}")
        print()
        
        if summary['agents']:
            print("BY AGENT:")
            for agent in summary['agents']:
                print(f"  {agent['agent']:10} | {agent['tokens']:>12,} tokens | ${agent['cost']:>8.2f}")
            print()
    
    elif command == "budget":
        budget = tracker.get_budget_status()
        
        print(f"\n=== BUDGET STATUS ({budget['month']}) ===")
        print(f"Budget: ${budget['budget']:.2f}")
        print(f"Spent: ${budget['spent']:.2f}")
        print(f"Remaining: ${budget['remaining']:.2f}")
        print(f"Usage: {budget['percent_used']:.1f}%")
        print(f"Status: {'[OK] On Track' if budget['on_track'] else '[WARNING] Over Budget!'}")
        print()
    
    elif command == "set-budget":
        if len(sys.argv) < 4:
            print("[ERROR] Usage: tokentracker.py set-budget <YYYY-MM> <amount>")
            sys.exit(1)
        
        month = sys.argv[2]
        amount = float(sys.argv[3])
        tracker.set_budget(month, amount)
    
    elif command == "report":
        period = sys.argv[2] if len(sys.argv) > 2 else "month"
        format = sys.argv[3] if len(sys.argv) > 3 else "text"
        
        report = tracker.export_report(period, format)
        print(report)
    
    else:
        print(f"[ERROR] Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
