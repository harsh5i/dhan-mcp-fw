"""
Scheduler — runs the strategy loop during market hours.
Runs as a background asyncio task inside the MCP server.
"""

import asyncio
import time
from datetime import datetime
from typing import Optional

from framework.schema import load_strategy
from framework.database import StrategyDB, init_db
from framework.data_manager import DataManager
from framework.engine import StrategyEngine
from framework.risk import RiskGovernor


class StrategyRunner:
    """Runs a single strategy in a background loop."""

    def __init__(self, strategy_id: str, dhan_client, mode: str = "paper"):
        self.strategy_id = strategy_id
        self.strategy = load_strategy(strategy_id)
        self.mode = mode  # "paper" or "live"
        self.client = dhan_client
        self.running = False
        self._task: Optional[asyncio.Task] = None

        # Initialize components
        init_db(strategy_id)
        self.db = StrategyDB(strategy_id)
        self.data_mgr = DataManager(self.strategy, self.db, dhan_client)
        self.engine = StrategyEngine(self.strategy)
        self.risk = RiskGovernor(self.strategy, self.db)

        # State
        self.cycle_count = 0
        self.last_signal = None

    def start(self, loop: asyncio.AbstractEventLoop = None):
        """Start the strategy loop as a background task."""
        if self.running:
            return {"status": "ALREADY_RUNNING"}

        self.running = True
        self.db.set_state("status", "running")
        self.db.set_state("mode", self.mode)
        self.db.set_state("started_at", datetime.now().isoformat())

        if loop:
            self._task = loop.create_task(self._run_loop())
        else:
            self._task = asyncio.ensure_future(self._run_loop())

        return {"status": "STARTED", "mode": self.mode, "interval": self.strategy.get("interval", 5)}

    def stop(self):
        """Stop the strategy loop."""
        self.running = False
        self.db.set_state("status", "stopped")
        self.db.set_state("stopped_at", datetime.now().isoformat())
        if self._task:
            self._task.cancel()
        return {"status": "STOPPED"}

    async def _run_loop(self):
        """Main strategy loop. Runs every N minutes during market hours."""
        interval_mins = self.strategy.get("interval", 5)
        interval_secs = interval_mins * 60

        # Initial data load
        self.db.set_state("phase", "loading_data")
        self.data_mgr.fetch_and_store_daily()

        while self.running:
            try:
                if not self._is_market_hours():
                    self.db.set_state("phase", "waiting_for_market")
                    await asyncio.sleep(60)
                    continue

                self.cycle_count += 1
                self.db.set_state("phase", "running_cycle")
                self.db.set_state("cycle_count", self.cycle_count)

                # Step 1: Fetch latest intraday data
                self.data_mgr.fetch_and_store_intraday()

                # Step 2: Compute indicators
                indicator_values = self.data_mgr.compute_indicators()

                # Step 3: Get current snapshot
                snapshot = self.data_mgr.get_current_snapshot()
                if not snapshot.get("close"):
                    await asyncio.sleep(interval_secs)
                    continue

                # Step 4: Check risk limits
                risk_check = self.risk.can_trade()

                # Step 5: Get current position
                position = self.db.get_state("current_position")

                # Step 5b: If in position, track bars held and fetch option LTP
                if position:
                    entry_cycle = position.get("entry_cycle", self.cycle_count)
                    position["bars_held"] = self.cycle_count - entry_cycle

                if position and position.get("security_id"):
                    try:
                        ltp_data = self.client.get_ltp(position["security_id"], "NSE_FNO")
                        opt_ltp = (ltp_data.get("data", {}).get("data", {})
                                   .get("NSE_FNO", {}).get(position["security_id"], {})
                                   .get("last_price"))
                        if opt_ltp:
                            snapshot["option_ltp"] = opt_ltp
                    except Exception:
                        pass  # Fall back to spot-based SL

                # Step 6: Evaluate strategy
                signal = self.engine.evaluate(snapshot, position)
                self.last_signal = signal
                self.db.set_state("last_signal", signal)
                self.db.set_state("last_snapshot", {
                    "close": snapshot.get("close"),
                    "indicators": indicator_values,
                    "timestamp": datetime.now().isoformat(),
                })

                # Step 7: Execute signal
                if signal["signal"] in ("BUY", "SELL") and not position:
                    if risk_check["allowed"]:
                        await self._execute_entry(signal, snapshot)
                    else:
                        self.db.set_state("last_risk_block", risk_check["reason"])

                elif signal["signal"].startswith("EXIT") and position:
                    await self._execute_exit(signal, position, snapshot)

                await asyncio.sleep(interval_secs)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.db.set_state("last_error", str(e))
                await asyncio.sleep(interval_secs)

    async def _execute_entry(self, signal: dict, snapshot: dict):
        """Execute an entry trade."""
        index = self.strategy["instrument"]["index"]
        expiry_pref = self.strategy["instrument"].get("expiry_preference", "nearest")

        # Get expiry
        from dhanhq import dhanhq as dhanhq_cls
        expiry_result = self.client._dhan.expiry_list(
            under_security_id=int(self.client._get_underlying_id(index)),
            under_exchange_segment=dhanhq_cls.INDEX,
        )
        expiries = expiry_result.get("data", {}).get("data", [])
        if not expiries:
            return

        if expiry_pref == "nearest":
            expiry = expiries[0]
        elif expiry_pref == "next_week" and len(expiries) > 1:
            expiry = expiries[1]
        elif expiry_pref == "monthly":
            expiry = expiries[-1] if expiries else expiries[0]
        else:
            expiry = expiries[0]

        # Get option chain and select strike
        # If signal specifies option_type (dual-direction), override strategy default
        signal_opt_type = signal.get("option_type")
        chain = self.client.get_option_chain(index, expiry)
        option = self.engine.select_option_strike(
            snapshot["close"], chain, option_type_override=signal_opt_type
        )
        if not option:
            return

        lots = signal.get("lots", 1)
        lot_size = 75 if index == "NIFTY" else 30
        quantity = lots * lot_size
        entry_price = option["ltp"]

        if self.mode == "paper":
            # Paper trade — simulate fill at LTP
            trade_id = self.db.record_trade(
                action=signal["signal"],
                symbol=index,
                security_id=option["security_id"],
                strike=option["strike"],
                option_type=option["option_type"],
                expiry=expiry,
                quantity=quantity,
                price=entry_price,
                order_type="MARKET",
                algo_reason=signal["reason"],
                mode="paper",
            )
        else:
            # Live trade — actually place order via client
            from models import get_lot_size
            result = self.client.place_order(
                security_id=option["security_id"],
                exchange_segment="NSE_FNO",
                transaction_type=signal["signal"],
                quantity=quantity,
                order_type="MARKET",
                product_type="INTRADAY",
            )
            order_id = result.get("data", {}).get("orderId")
            trade_id = self.db.record_trade(
                action=signal["signal"],
                symbol=index,
                security_id=option["security_id"],
                strike=option["strike"],
                option_type=option["option_type"],
                expiry=expiry,
                quantity=quantity,
                price=entry_price,
                order_type="MARKET",
                algo_reason=signal["reason"],
                mode="live",
                order_id=order_id,
            )

        # Save position state
        self.db.set_state("current_position", {
            "trade_id": trade_id,
            "action": signal["signal"],
            "entry_price": entry_price,
            "security_id": option["security_id"],
            "strike": option["strike"],
            "option_type": option["option_type"],
            "expiry": expiry,
            "quantity": quantity,
            "lots": lots,
            "entered_at": datetime.now().isoformat(),
            "entry_cycle": self.cycle_count,
        })

    async def _execute_exit(self, signal: dict, position: dict, snapshot: dict):
        """Execute an exit trade."""
        entry_price = position["entry_price"]
        security_id = position["security_id"]
        quantity = position["quantity"]
        action = position["action"]

        # Get current LTP
        ltp_data = self.client.get_ltp(security_id, "NSE_FNO")
        eq_data = ltp_data.get("data", {}).get("data", {}).get("NSE_FNO", {}).get(security_id, {})
        exit_price = eq_data.get("last_price", snapshot.get("close", 0))

        # Calculate P&L
        if action == "BUY":
            pnl = (exit_price - entry_price) * quantity
            exit_action = "SELL"
        else:
            pnl = (entry_price - exit_price) * quantity
            exit_action = "BUY"

        if self.mode == "paper":
            trade_id = self.db.record_trade(
                action=exit_action,
                symbol=self.strategy["instrument"]["index"],
                security_id=security_id,
                strike=position["strike"],
                option_type=position["option_type"],
                expiry=position["expiry"],
                quantity=quantity,
                price=exit_price,
                order_type="MARKET",
                algo_reason=signal["reason"],
                mode="paper",
            )
        else:
            result = self.client.place_order(
                security_id=security_id,
                exchange_segment="NSE_FNO",
                transaction_type=exit_action,
                quantity=quantity,
                order_type="MARKET",
                product_type="INTRADAY",
            )
            order_id = result.get("data", {}).get("orderId")
            trade_id = self.db.record_trade(
                action=exit_action,
                symbol=self.strategy["instrument"]["index"],
                security_id=security_id,
                strike=position["strike"],
                option_type=position["option_type"],
                expiry=position["expiry"],
                quantity=quantity,
                price=exit_price,
                order_type="MARKET",
                algo_reason=signal["reason"],
                mode="live",
                order_id=order_id,
            )

        # Update entry trade P&L
        self.db.update_trade_pnl(position["trade_id"], pnl)

        # Clear position
        self.db.set_state("current_position", None)
        self.db.set_state("last_exit", {
            "trade_id": trade_id,
            "exit_price": exit_price,
            "pnl": pnl,
            "reason": signal["reason"],
            "timestamp": datetime.now().isoformat(),
        })

    def _is_market_hours(self) -> bool:
        """Check if currently within market hours."""
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo

        ist = ZoneInfo("Asia/Kolkata")
        now = datetime.now(ist)

        if now.weekday() >= 5:
            return False

        market_open = now.replace(hour=9, minute=15, second=0)
        market_close = now.replace(hour=15, minute=30, second=0)

        return market_open <= now <= market_close

    def get_status(self) -> dict:
        """Get current runner status."""
        return {
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy.get("name"),
            "running": self.running,
            "mode": self.mode,
            "cycle_count": self.cycle_count,
            "last_signal": self.last_signal,
            "state": self.db.get_all_state(),
            "performance": self.db.compute_performance(),
        }


# Global runners registry (multiple strategies can run concurrently)
_active_runners: dict[str, StrategyRunner] = {}


def get_active_runner(strategy_id: str = None) -> Optional[StrategyRunner]:
    """Get a runner by ID, or the single runner if only one exists."""
    if strategy_id:
        return _active_runners.get(strategy_id)
    # Backward compat: return single runner if only one
    if len(_active_runners) == 1:
        return next(iter(_active_runners.values()))
    return None


def get_all_runners() -> dict[str, StrategyRunner]:
    """Get all active runners."""
    return _active_runners


def set_active_runner(runner: Optional[StrategyRunner]):
    """Add or remove a runner."""
    if runner is None:
        return
    _active_runners[runner.strategy_id] = runner


def remove_runner(strategy_id: str):
    """Remove a runner from registry."""
    _active_runners.pop(strategy_id, None)
