"""
Backtester — replays historical candles through the strategy engine.
Simulates entries, exits, SL, and targets using historical prices.
"""

import pandas as pd
from datetime import datetime, timedelta

from framework.engine import StrategyEngine
from framework.data_manager import INDICATOR_REGISTRY, DERIVED_INDICATOR_TYPES, _compute_derived


class BacktestResult:
    """Holds backtest output."""

    def __init__(self):
        self.trades = []
        self.equity_curve = []
        self.initial_capital = 0
        self.final_capital = 0
        self.options_warning = None

    def add_trade(self, trade: dict):
        self.trades.append(trade)

    def summary(self) -> dict:
        if not self.trades:
            return {"total_trades": 0, "message": "No trades triggered during backtest period"}

        pnls = [t["pnl"] for t in self.trades if t.get("pnl") is not None]
        if not pnls:
            return {"total_trades": len(self.trades), "message": "All trades still open at end"}

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        total_pnl = sum(pnls)

        # Max drawdown
        peak = 0
        max_dd = 0
        running = 0
        for p in pnls:
            running += p
            if running > peak:
                peak = running
            dd = peak - running
            if dd > max_dd:
                max_dd = dd

        # Profit factor
        gross_profit = sum(wins) if wins else 0
        gross_loss = abs(sum(losses)) if losses else 0
        profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else float("inf")

        summary = {
            "total_trades": len(pnls),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / len(pnls) * 100, 1),
            "total_pnl": round(total_pnl, 2),
            "avg_win": round(sum(wins) / len(wins), 2) if wins else 0,
            "avg_loss": round(sum(losses) / len(losses), 2) if losses else 0,
            "max_drawdown": round(max_dd, 2),
            "best_trade": round(max(pnls), 2),
            "worst_trade": round(min(pnls), 2),
            "profit_factor": profit_factor,
            "gross_profit": round(gross_profit, 2),
            "gross_loss": round(gross_loss, 2),
        }

        if self.options_warning:
            summary["warning"] = self.options_warning
            summary["pnl_basis"] = "spot_index (not option premium)"

        return summary


class Backtester:
    """Replays historical data through strategy rules."""

    def __init__(self, strategy: dict):
        self.strategy = strategy
        self.engine = StrategyEngine(strategy)
        self.lot_size = 75 if strategy["instrument"]["index"] == "NIFTY" else 30
        self.lots = strategy.get("entry", {}).get("lots", 1)
        self.quantity = self.lots * self.lot_size

    def run(self, candles_df: pd.DataFrame) -> BacktestResult:
        """
        Run backtest on a DataFrame of OHLCV candles.

        Args:
            candles_df: DataFrame with columns: timestamp, open, high, low, close, volume

        Returns:
            BacktestResult with trades and summary.
            For options strategies, returns signal-only analysis (no P&L)
            since historical option premium data is unavailable.
        """
        result = BacktestResult()
        df = candles_df.copy().reset_index(drop=True)

        if len(df) < 2:
            return result

        # Options strategies: backtest can validate signals but not P&L
        instrument = self.strategy.get("instrument", {})
        self._is_options = instrument.get("option_type") in ("CE", "PE", "BOTH")
        if self._is_options:
            result.options_warning = (
                "Options backtest limitation: P&L is computed on spot index movement, "
                "not actual option premiums. Real option P&L depends on delta, theta, "
                "and IV which vary per strike and expiry. Use paper trading for accurate "
                "options P&L. Signal timing and direction remain valid."
            )

        # Compute all indicators on the full dataset
        indicators_by_row = self._compute_all_indicators(df)

        position = None

        for i in range(len(df)):
            row = df.iloc[i]
            row_indicators = indicators_by_row.get(i, {})

            # Skip rows where indicators aren't ready (warmup period)
            if not row_indicators:
                continue

            snapshot = {
                "timestamp": row.get("timestamp"),
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row.get("volume", 0),
                "indicators": row_indicators,
            }

            # Track bars held for time stop
            if position:
                position["bars_held"] = i - position["entry_index"]

            signal = self.engine.evaluate(snapshot, position)

            # Entry
            if signal["signal"] in ("BUY", "SELL") and not position:
                position = {
                    "action": signal["signal"],
                    "entry_price": row["close"],
                    "entry_index": i,
                    "entry_time": row.get("timestamp"),
                    "reason": signal["reason"],
                }

            # Exit (SL, target, or rule)
            elif signal["signal"].startswith("EXIT") and position:
                entry_price = position["entry_price"]
                exit_price = row["close"]

                if position["action"] == "BUY":
                    pnl = (exit_price - entry_price) * self.quantity
                else:
                    pnl = (entry_price - exit_price) * self.quantity

                result.add_trade({
                    "entry_time": position["entry_time"],
                    "exit_time": row.get("timestamp"),
                    "action": position["action"],
                    "entry_price": round(entry_price, 2),
                    "exit_price": round(exit_price, 2),
                    "quantity": self.quantity,
                    "pnl": round(pnl, 2),
                    "exit_type": signal["signal"],
                    "entry_reason": position["reason"],
                    "exit_reason": signal["reason"],
                    "bars_held": i - position["entry_index"],
                })
                position = None

        # If still in position at end, force close at last candle
        if position:
            last = df.iloc[-1]
            entry_price = position["entry_price"]
            exit_price = last["close"]

            if position["action"] == "BUY":
                pnl = (exit_price - entry_price) * self.quantity
            else:
                pnl = (entry_price - exit_price) * self.quantity

            result.add_trade({
                "entry_time": position["entry_time"],
                "exit_time": last.get("timestamp"),
                "action": position["action"],
                "entry_price": round(entry_price, 2),
                "exit_price": round(exit_price, 2),
                "quantity": self.quantity,
                "pnl": round(pnl, 2),
                "exit_type": "EXIT_END_OF_DATA",
                "entry_reason": position["reason"],
                "exit_reason": "Backtest period ended — forced close",
                "bars_held": len(df) - 1 - position["entry_index"],
            })

        return result

    def _compute_all_indicators(self, df: pd.DataFrame) -> dict:
        """
        Compute all strategy indicators across the full DataFrame.
        Two-pass: base indicators first, then derived (LAG, CHANGE, SLOPE).
        Returns dict: {row_index: {indicator_name: value}}
        """
        indicator_series = {}

        # Separate base and derived
        base_configs = []
        derived_configs = []
        for ind_config in self.strategy.get("indicators", []):
            if ind_config["type"].upper() in DERIVED_INDICATOR_TYPES:
                derived_configs.append(ind_config)
            else:
                base_configs.append(ind_config)

        # Pass 1: base indicators
        for ind_config in base_configs:
            name = ind_config["name"]
            ind_type = ind_config["type"].upper()
            period = ind_config.get("period", 14)

            compute_fn = INDICATOR_REGISTRY.get(ind_type)
            if not compute_fn:
                continue

            try:
                values = compute_fn(df, period=period)
                indicator_series[name] = values
            except Exception:
                continue

        # Pass 2: derived indicators
        for ind_config in derived_configs:
            name = ind_config["name"]
            ind_type = ind_config["type"].upper()
            period = ind_config.get("period", 1)
            source = ind_config.get("source")

            if not source or source not in indicator_series:
                continue

            try:
                values = _compute_derived(indicator_series[source], ind_type, period)
                indicator_series[name] = values
            except Exception:
                continue

        # Build per-row indicator dicts
        result = {}
        for i in range(len(df)):
            row_ind = {}
            all_ready = True
            for name, series in indicator_series.items():
                val = series.iloc[i]
                if pd.notna(val):
                    row_ind[name] = round(float(val), 4)
                else:
                    all_ready = False

            if all_ready and row_ind:
                result[i] = row_ind

        return result
