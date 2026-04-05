"""
Safety layer for dhan-nifty-mcp.
Every order passes through validate_order() before reaching Dhan.
"""

from datetime import datetime, time
from typing import Optional
from models import OrderRequest, SafetyResult, get_lot_size


def validate_order(
    order: OrderRequest,
    config: dict,
    current_ltp: Optional[float] = None,
    open_position_count: int = 0,
) -> SafetyResult:
    """
    Run all safety checks on an order request.
    Returns SafetyResult with pass/fail and per-check details.
    """
    safety = config.get("safety", {})
    result = SafetyResult(passed=True)

    # ── 1. Instrument whitelist ────────────────────────
    allowed = [s.upper() for s in safety.get("allowed_instruments", [])]
    if order.symbol.upper() not in allowed:
        result.passed = False
        result.instrument_allowed = False
        result.rejection_reason = f"Instrument '{order.symbol}' not in allowed list: {allowed}"
        return result

    # ── 2. Market hours ───────────────────────────────
    now = datetime.now().time()
    hours = safety.get("market_hours", {})
    market_start = _parse_time(hours.get("start", "09:15"))
    market_end = _parse_time(hours.get("end", "15:30"))

    if not (market_start <= now <= market_end):
        result.passed = False
        result.within_market_hours = False
        result.rejection_reason = (
            f"Outside market hours. Current: {now.strftime('%H:%M')}, "
            f"Allowed: {hours.get('start')}-{hours.get('end')}"
        )
        return result

    # ── 3. Lot limit ──────────────────────────────────
    max_lots = safety.get("max_lots_per_order", 2)
    if order.lots > max_lots:
        result.passed = False
        result.within_lot_limit = False
        result.rejection_reason = f"Lots ({order.lots}) exceeds max ({max_lots})"
        return result

    # ── 4. Open position limit ────────────────────────
    max_positions = safety.get("max_open_positions", 5)
    if open_position_count >= max_positions:
        result.passed = False
        result.within_position_limit = False
        result.rejection_reason = (
            f"Open positions ({open_position_count}) "
            f"already at max ({max_positions})"
        )
        return result

    # ── 5. Order value cap ────────────────────────────
    max_value = safety.get("max_order_value", 50000)
    if current_ltp is not None:
        lot_size = get_lot_size(order.symbol)
        total_qty = order.lots * lot_size
        estimated_value = current_ltp * total_qty
        if estimated_value > max_value:
            result.passed = False
            result.within_value_limit = False
            result.rejection_reason = (
                f"Estimated order value Rs.{estimated_value:,.2f} "
                f"exceeds max Rs.{max_value:,.2f}"
            )
            return result

    # ── 6. Price sanity (limit orders only) ───────────
    if order.price is not None and current_ltp is not None:
        deviation_pct = safety.get("price_deviation_pct", 20)
        deviation = abs(order.price - current_ltp) / current_ltp * 100
        if deviation > deviation_pct:
            result.passed = False
            result.price_sane = False
            result.rejection_reason = (
                f"Limit price Rs.{order.price} deviates {deviation:.1f}% "
                f"from LTP Rs.{current_ltp} (max allowed: {deviation_pct}%)"
            )
            return result

    return result


def _parse_time(t: str) -> time:
    """Parse 'HH:MM' string to time object."""
    parts = t.strip().split(":")
    return time(int(parts[0]), int(parts[1]))
