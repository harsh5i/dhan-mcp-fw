"""
Risk governor — enforces daily loss limits, trade count, and cool-off periods.
The strategy cannot override these. Hard stops.
"""

from framework.database import StrategyDB


class RiskGovernor:
    """Checks risk limits before allowing any trade."""

    def __init__(self, strategy: dict, db: StrategyDB):
        self.risk = strategy.get("risk", {})
        self.db = db
        self.max_loss = self.risk.get("max_loss_per_day", 5000)
        self.max_trades = self.risk.get("max_trades_per_day", 5)
        self.cool_off = self.risk.get("cool_off_after_loss", 0)

    def can_trade(self) -> dict:
        """Check if trading is allowed. Returns {allowed: bool, reason: str}."""

        # Check daily P&L
        perf = self.db.compute_performance()
        today_pnl = self._get_today_pnl()
        if today_pnl <= -self.max_loss:
            return {
                "allowed": False,
                "reason": f"Daily loss limit hit: {today_pnl:.2f} <= -{self.max_loss}",
            }

        # Check trade count
        today_trades = self._get_today_trade_count()
        if today_trades >= self.max_trades:
            return {
                "allowed": False,
                "reason": f"Max trades for today: {today_trades}/{self.max_trades}",
            }

        # Check cool-off
        if self.cool_off > 0:
            recent_losses = self._get_recent_consecutive_losses()
            if recent_losses >= self.cool_off:
                return {
                    "allowed": False,
                    "reason": f"Cool-off active: {recent_losses} consecutive losses (limit: {self.cool_off})",
                }

        return {"allowed": True, "reason": "OK"}

    def _get_today_pnl(self) -> float:
        """Sum of P&L for today's closed trades."""
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        trades = self.db.get_trades(limit=100)
        total = 0
        for t in trades:
            if t.get("timestamp", "").startswith(today) and t.get("pnl") is not None:
                total += t["pnl"]
        return total

    def _get_today_trade_count(self) -> int:
        """Count trades placed today."""
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        trades = self.db.get_trades(limit=100)
        return sum(1 for t in trades if t.get("timestamp", "").startswith(today))

    def _get_recent_consecutive_losses(self) -> int:
        """Count consecutive losses from most recent trade backwards."""
        trades = self.db.get_trades(limit=20)
        count = 0
        for t in reversed(trades):
            if t.get("pnl") is not None and t["pnl"] <= 0:
                count += 1
            else:
                break
        return count
