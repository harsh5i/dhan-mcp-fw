"""
Narrator — sends trade events to a local LLM (Gemma4 via Ollama) for commentary.
The narrator does NOT make trading decisions. It only provides human-readable
color commentary on what the strategy is doing and why.
"""

import json
import subprocess
from datetime import datetime
from typing import Optional

from framework.database import StrategyDB


class Narrator:
    """Generates trade commentary via a local LLM (Ollama)."""

    def __init__(self, strategy: dict, db: StrategyDB, model: str = "gemma3:4b"):
        self.strategy = strategy
        self.db = db
        self.model = model
        self.enabled = True

    def comment_on_entry(self, signal: dict, snapshot: dict, option: dict) -> str:
        """Generate commentary when a trade is entered."""
        prompt = self._build_prompt(
            event="ENTRY",
            details={
                "signal": signal["signal"],
                "reason": signal["reason"],
                "spot_price": snapshot.get("close"),
                "strike": option.get("strike"),
                "option_type": option.get("option_type"),
                "premium": option.get("ltp"),
                "indicators": snapshot.get("indicators", {}),
                "strategy_name": self.strategy.get("name"),
            },
        )
        return self._ask_llm(prompt)

    def comment_on_exit(self, signal: dict, position: dict, pnl: float) -> str:
        """Generate commentary when a trade is exited."""
        prompt = self._build_prompt(
            event="EXIT",
            details={
                "exit_type": signal["signal"],
                "reason": signal["reason"],
                "entry_price": position.get("entry_price"),
                "strike": position.get("strike"),
                "option_type": position.get("option_type"),
                "pnl": pnl,
                "hold_duration": self._hold_duration(position),
                "strategy_name": self.strategy.get("name"),
            },
        )
        return self._ask_llm(prompt)

    def comment_on_hold(self, snapshot: dict, position: dict) -> str:
        """Generate brief commentary on why we're holding."""
        entry_price = position.get("entry_price", 0)
        close = snapshot.get("close", 0)
        unrealized = close - entry_price if position.get("action") == "BUY" else entry_price - close

        prompt = self._build_prompt(
            event="HOLD",
            details={
                "spot_price": close,
                "entry_price": entry_price,
                "unrealized_pnl_per_unit": round(unrealized, 2),
                "indicators": snapshot.get("indicators", {}),
                "strategy_name": self.strategy.get("name"),
            },
        )
        return self._ask_llm(prompt)

    def daily_summary(self) -> str:
        """Generate end-of-day summary."""
        perf = self.db.compute_performance()
        trades = self.db.get_trades(limit=20)
        today = datetime.now().strftime("%Y-%m-%d")
        today_trades = [t for t in trades if t.get("timestamp", "").startswith(today)]

        prompt = self._build_prompt(
            event="DAILY_SUMMARY",
            details={
                "date": today,
                "strategy_name": self.strategy.get("name"),
                "total_trades_today": len(today_trades),
                "performance": perf,
                "trades": today_trades[:10],
            },
        )
        return self._ask_llm(prompt)

    def _build_prompt(self, event: str, details: dict) -> str:
        """Build a prompt for the narrator LLM."""
        system = (
            "You are a concise options trading commentator. "
            "Provide brief, insightful commentary on trade events. "
            "Keep responses under 3 sentences. Use plain language. "
            "Focus on what happened and why it matters."
        )

        user_msg = f"Event: {event}\n{json.dumps(details, indent=2, default=str)}"

        return json.dumps({
            "system": system,
            "user": user_msg,
        })

    def _ask_llm(self, prompt_json: str) -> str:
        """Call Ollama API via subprocess."""
        if not self.enabled:
            return ""

        try:
            prompt = json.loads(prompt_json)
            result = subprocess.run(
                [
                    "ollama", "run", self.model,
                    "--system", prompt["system"],
                    prompt["user"],
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            commentary = result.stdout.strip()
            return commentary if commentary else "(no commentary)"
        except subprocess.TimeoutExpired:
            return "(narrator timeout)"
        except FileNotFoundError:
            return "(ollama not installed)"
        except Exception as e:
            return f"(narrator error: {e})"

    def _hold_duration(self, position: dict) -> str:
        """Calculate how long we've been in a position."""
        entered = position.get("entered_at")
        if not entered:
            return "unknown"
        try:
            entry_time = datetime.fromisoformat(entered)
            delta = datetime.now() - entry_time
            mins = int(delta.total_seconds() / 60)
            if mins < 60:
                return f"{mins}m"
            return f"{mins // 60}h {mins % 60}m"
        except Exception:
            return "unknown"
