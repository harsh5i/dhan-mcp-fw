"""
Strategy engine — evaluates entry/exit/SL/target conditions.
Pure deterministic code. No AI judgment here.
"""

import re
from datetime import datetime


class StrategyEngine:
    """Evaluates strategy rules against current market snapshot."""

    def __init__(self, strategy: dict):
        self.strategy = strategy
        self.entry_conditions = strategy.get("entry", {}).get("conditions", [])
        self.entry_ce_conditions = strategy.get("entry", {}).get("conditions_ce", [])
        self.entry_pe_conditions = strategy.get("entry", {}).get("conditions_pe", [])
        self.exit_conditions = strategy.get("exit", {}).get("conditions", [])
        self.exit_ce_conditions = strategy.get("exit", {}).get("conditions_ce", [])
        self.exit_pe_conditions = strategy.get("exit", {}).get("conditions_pe", [])
        self.max_bars = strategy.get("exit", {}).get("max_bars")
        self.lots = strategy.get("entry", {}).get("lots", 1)
        self.sl_config = strategy.get("stop_loss", {})
        self.target_config = strategy.get("target", {})
        # Trailing SL state: tracks peak favorable price during a trade
        self._peak_price = None

    def evaluate(self, snapshot: dict, position: dict = None) -> dict:
        """
        Evaluate strategy against current snapshot.

        Args:
            snapshot: {close, open, high, low, volume, indicators: {name: value},
                       option_ltp: (optional) current option premium for SL/target}
            position: None if no position, or {entry_price, action, ...}

        Returns:
            {
                signal: "BUY" | "SELL" | "EXIT_SL" | "EXIT_TARGET" | "EXIT_RULE" | "HOLD",
                reason: "human-readable reason",
                details: {condition evaluations}
            }
        """
        close = snapshot.get("close", 0)
        indicators = snapshot.get("indicators", {})
        # For options: use option_ltp for SL/target, spot close for signals
        price_for_sl = snapshot.get("option_ltp") or close

        # Build evaluation context
        ctx = {
            "open": snapshot.get("open", 0),
            "high": snapshot.get("high", 0),
            "low": snapshot.get("low", 0),
            "close": close,
            "volume": snapshot.get("volume", 0),
        }
        ctx.update(indicators)

        # If we have a position, check SL and target first
        if position:
            entry_price = position.get("entry_price", 0)
            action = position.get("action", "BUY")

            # Update trailing peak (tracks option premium if available, else spot)
            if self._peak_price is None:
                self._peak_price = price_for_sl
            elif action == "BUY":
                self._peak_price = max(self._peak_price, price_for_sl)
            else:
                self._peak_price = min(self._peak_price, price_for_sl)

            sl_check = self._check_stop_loss(price_for_sl, entry_price, action, indicators)
            if sl_check:
                self._peak_price = None
                return {
                    "signal": "EXIT_SL",
                    "reason": sl_check,
                    "details": {"close": close, "option_ltp": price_for_sl, "entry_price": entry_price},
                }

            target_check = self._check_target(price_for_sl, entry_price, action)
            if target_check:
                self._peak_price = None
                return {
                    "signal": "EXIT_TARGET",
                    "reason": target_check,
                    "details": {"close": close, "option_ltp": price_for_sl, "entry_price": entry_price},
                }

            # Check exit conditions — use direction-specific if available
            option_type = position.get("option_type", "").upper()
            if option_type == "CE" and self.exit_ce_conditions:
                exit_conds = self.exit_ce_conditions
            elif option_type == "PE" and self.exit_pe_conditions:
                exit_conds = self.exit_pe_conditions
            else:
                exit_conds = self.exit_conditions

            exit_eval = self._evaluate_conditions(exit_conds, ctx, match_any=True)
            if exit_eval["triggered"]:
                self._peak_price = None
                return {
                    "signal": "EXIT_RULE",
                    "reason": f"Exit condition met: {exit_eval['matched']}",
                    "details": exit_eval,
                }

            # Time stop: exit after max bars held
            if self.max_bars is not None:
                bars_held = position.get("bars_held", 0)
                if bars_held >= self.max_bars:
                    self._peak_price = None
                    return {
                        "signal": "EXIT_RULE",
                        "reason": f"Time stop: held {bars_held} bars (max {self.max_bars})",
                        "details": {"bars_held": bars_held, "max_bars": self.max_bars},
                    }

            return {
                "signal": "HOLD",
                "reason": "Position open, no exit trigger",
                "details": {"close": close, "indicators": indicators},
            }

        # No position — check entry conditions
        self._peak_price = None
        trade_type = self.strategy.get("instrument", {}).get("trade_type", "BUY")

        # Dual-direction: separate CE/PE conditions
        if self.entry_ce_conditions and self.entry_pe_conditions:
            ce_eval = self._evaluate_conditions(self.entry_ce_conditions, ctx, match_any=False)
            pe_eval = self._evaluate_conditions(self.entry_pe_conditions, ctx, match_any=False)

            if ce_eval["triggered"]:
                return {
                    "signal": "BUY",
                    "reason": f"CE entry conditions met: {ce_eval['results']}",
                    "details": ce_eval,
                    "lots": self.lots,
                    "option_type": "CE",
                }
            if pe_eval["triggered"]:
                return {
                    "signal": "BUY",
                    "reason": f"PE entry conditions met: {pe_eval['results']}",
                    "details": pe_eval,
                    "lots": self.lots,
                    "option_type": "PE",
                }

            return {
                "signal": "HOLD",
                "reason": f"No entry: CE={ce_eval['results']}, PE={pe_eval['results']}",
                "details": {"ce": ce_eval, "pe": pe_eval},
            }

        # Single-direction: original behavior
        entry_eval = self._evaluate_conditions(self.entry_conditions, ctx, match_any=False)
        if entry_eval["triggered"]:
            return {
                "signal": trade_type,
                "reason": f"All entry conditions met: {entry_eval['results']}",
                "details": entry_eval,
                "lots": self.lots,
            }

        return {
            "signal": "HOLD",
            "reason": f"Entry conditions not met: {entry_eval['results']}",
            "details": entry_eval,
        }

    def _evaluate_conditions(self, conditions: list, ctx: dict, match_any: bool = False) -> dict:
        """
        Evaluate a list of conditions like "rsi < 30", "ema_fast > ema_slow".
        match_any=True: OR logic (any condition triggers)
        match_any=False: AND logic (all conditions must be true)
        """
        results = {}
        matched = []

        for cond in conditions:
            try:
                result = self._eval_single(cond, ctx)
                results[cond] = result
                if result:
                    matched.append(cond)
            except Exception as e:
                results[cond] = f"ERROR: {e}"

        if match_any:
            triggered = len(matched) > 0
        else:
            triggered = len(matched) == len(conditions) and len(conditions) > 0

        return {"triggered": triggered, "results": results, "matched": matched}

    def _eval_single(self, condition: str, ctx: dict) -> bool:
        """
        Safely evaluate a single condition string.
        Supports: <, >, <=, >=, ==, !=
        Operands can be indicator names, 'close', 'open', 'high', 'low', 'volume', or numbers.
        """
        # Parse: "rsi < 30" or "ema_fast > ema_slow"
        pattern = r'(\w+)\s*(<=|>=|!=|==|<|>)\s*(\w+\.?\d*)'
        match = re.match(pattern, condition.strip())
        if not match:
            raise ValueError(f"Cannot parse condition: {condition}")

        left_name, operator, right_name = match.groups()

        left = self._resolve_value(left_name, ctx)
        right = self._resolve_value(right_name, ctx)

        if left is None or right is None:
            return False

        ops = {
            "<": lambda a, b: a < b,
            ">": lambda a, b: a > b,
            "<=": lambda a, b: a <= b,
            ">=": lambda a, b: a >= b,
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
        }
        return ops[operator](left, right)

    def _resolve_value(self, name: str, ctx: dict):
        """Resolve a name to a numeric value from context."""
        # Try as number first
        try:
            return float(name)
        except ValueError:
            pass
        # Look up in context
        val = ctx.get(name)
        if val is not None:
            return float(val)
        return None

    def _check_stop_loss(self, current_price: float, entry_price: float,
                         action: str, indicators: dict = None) -> str:
        """
        Check if stop loss is hit. Returns reason string or None.

        Supported types:
          - percentage: fixed % from entry
          - points: fixed points from entry
          - atr: ATR-based dynamic SL. Uses indicator named by 'atr_indicator'
                 (default 'atr'). SL = entry ± (ATR × multiplier).
                 Optional 'min' and 'max' to clamp.
          - trailing: trails from peak favorable price by value (points or %).
                      Uses 'trail_type' (points|percentage, default points).
        """
        sl_type = self.sl_config.get("type", "percentage")
        sl_value = self.sl_config.get("value", 20)
        indicators = indicators or {}

        if sl_type == "atr":
            atr_name = self.sl_config.get("atr_indicator", "atr")
            atr_val = indicators.get(atr_name)
            if atr_val is None or atr_val <= 0:
                return None  # Can't evaluate without ATR, skip
            multiplier = self.sl_config.get("multiplier", 2.0)
            sl_distance = atr_val * multiplier
            sl_min = self.sl_config.get("min")
            sl_max = self.sl_config.get("max")
            if sl_min is not None:
                sl_distance = max(sl_distance, sl_min)
            if sl_max is not None:
                sl_distance = min(sl_distance, sl_max)
            sl_price = (entry_price - sl_distance) if action == "BUY" else (entry_price + sl_distance)
            label = f"atr({atr_val:.1f})×{multiplier}={sl_distance:.1f}"

        elif sl_type == "trailing":
            trail_type = self.sl_config.get("trail_type", "points")
            activate_after = self.sl_config.get("activate_after")
            peak = self._peak_price or entry_price

            # Check if trail is activated (profit threshold reached)
            if activate_after is not None:
                if action == "BUY":
                    current_profit = current_price - entry_price
                else:
                    current_profit = entry_price - current_price
                if current_profit < activate_after:
                    # Not yet activated — use no SL (or fall through)
                    return None

            if trail_type == "percentage":
                sl_distance = peak * sl_value / 100
            else:
                sl_distance = sl_value
            sl_price = (peak - sl_distance) if action == "BUY" else (peak + sl_distance)
            activate_label = f", activated after +{activate_after}" if activate_after else ""
            label = f"trailing {sl_value}{' %' if trail_type == 'percentage' else 'pt'} from peak {peak:.2f}{activate_label}"

        elif sl_type == "percentage":
            sl_distance = entry_price * sl_value / 100
            sl_price = (entry_price - sl_distance) if action == "BUY" else (entry_price + sl_distance)
            label = f"percentage {sl_value}%"

        else:  # points
            sl_price = (entry_price - sl_value) if action == "BUY" else (entry_price + sl_value)
            label = f"points {sl_value}"

        if action == "BUY" and current_price <= sl_price:
            return f"Stop loss hit: price {current_price} <= SL {sl_price:.2f} ({label})"
        elif action != "BUY" and current_price >= sl_price:
            return f"Stop loss hit: price {current_price} >= SL {sl_price:.2f} ({label})"

        return None

    def _check_target(self, current_price: float, entry_price: float, action: str) -> str:
        """Check if target is hit. Returns reason string or None."""
        target_type = self.target_config.get("type", "percentage")
        target_value = self.target_config.get("value", 40)

        if action == "BUY":
            if target_type == "percentage":
                target_price = entry_price * (1 + target_value / 100)
            else:
                target_price = entry_price + target_value

            if current_price >= target_price:
                return f"Target hit: price {current_price} >= target {target_price:.2f} ({target_type} {target_value})"
        else:
            if target_type == "percentage":
                target_price = entry_price * (1 - target_value / 100)
            else:
                target_price = entry_price - target_value

            if current_price <= target_price:
                return f"Target hit: price {current_price} <= target {target_price:.2f} ({target_type} {target_value})"

        return None

    def select_option_strike(self, spot_price: float, option_chain: dict,
                             option_type_override: str = None) -> dict:
        """
        Select the right option strike based on strategy preference.
        Returns {strike, option_type, security_id} or None.
        """
        pref = self.strategy.get("instrument", {}).get("option_preference", "ATM")
        opt_type = (option_type_override or self.strategy.get("instrument", {}).get("option_type", "CE")).lower()

        oc = option_chain.get("data", {}).get("data", {}).get("oc", {})
        if not oc:
            return None

        # Find ATM strike (nearest to spot)
        strikes = sorted([float(k) for k in oc.keys()])
        atm_strike = min(strikes, key=lambda s: abs(s - spot_price))

        # Apply offset
        if pref == "ATM":
            target_strike = atm_strike
        elif pref == "ITM":
            if opt_type == "ce":
                target_strike = max([s for s in strikes if s < atm_strike], default=atm_strike)
            else:
                target_strike = min([s for s in strikes if s > atm_strike], default=atm_strike)
        elif pref == "OTM":
            if opt_type == "ce":
                target_strike = min([s for s in strikes if s > atm_strike], default=atm_strike)
            else:
                target_strike = max([s for s in strikes if s < atm_strike], default=atm_strike)
        elif pref.startswith("ATM"):
            # ATM+100, ATM-200, etc.
            try:
                offset = float(pref.replace("ATM", ""))
                target_strike = min(strikes, key=lambda s: abs(s - (atm_strike + offset)))
            except ValueError:
                target_strike = atm_strike
        else:
            target_strike = atm_strike

        # Find the strike in option chain
        for strike_key, strike_data in oc.items():
            if float(strike_key) == target_strike and opt_type in strike_data:
                entry = strike_data[opt_type]
                return {
                    "strike": target_strike,
                    "option_type": opt_type.upper(),
                    "security_id": str(entry.get("security_id", "")),
                    "ltp": entry.get("last_price"),
                    "iv": entry.get("implied_volatility"),
                    "oi": entry.get("oi"),
                }

        return None
