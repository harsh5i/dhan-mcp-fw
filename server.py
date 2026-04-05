"""
dhan-nifty-mcp: Personal MCP server for Nifty options trading via Dhan.

Usage with Claude Code:
  claude mcp add dhan-nifty -- python /path/to/server.py

Usage standalone:
  python server.py
"""

import os
import time
import yaml
import base64
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from mcp.server.fastmcp import FastMCP

from models import (
    OrderRequest, OrderAction, OrderType, OptionType, ProductType,
    ServerMode, SafetyResult, DryRunResponse, LiveResponse, ErrorResponse,
    get_lot_size,
)
from safety import validate_order
from logger import AuditLogger
from dhan_client import DhanClient


# ── Load config ───────────────────────────────────────────

CONFIG_PATH = os.environ.get(
    "DHAN_MCP_CONFIG",
    os.path.join(os.path.dirname(__file__), "config.yaml"),
)

with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

# ── Initialize components ─────────────────────────────────

logger = AuditLogger(CONFIG)

client = DhanClient(
    client_id=CONFIG["dhan"]["client_id"],
    access_token=CONFIG["dhan"]["access_token"],
)

mcp = FastMCP(
    "dhan-nifty",
    instructions="Personal Nifty options trading server via Dhan",
)


# ── Helpers ───────────────────────────────────────────────

def _get_mode() -> str:
    return CONFIG.get("safety", {}).get("mode", "dry-run")


def _count_open_positions() -> int:
    """Count current open positions from Dhan."""
    try:
        positions = client.get_positions()
        if positions and positions.get("data"):
            return sum(
                1 for p in positions["data"]
                if p.get("netQty", 0) != 0
            )
    except Exception:
        pass
    return 0


def _timed(func):
    """Decorator to measure execution time."""
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = (time.time() - start) * 1000
        return result, elapsed
    return wrapper


def _check_token_health() -> dict:
    """Decode JWT and check expiry."""
    token = CONFIG.get("dhan", {}).get("access_token", "")
    try:
        payload = token.split(".")[1]
        payload += "=" * (4 - len(payload) % 4)
        data = json.loads(base64.b64decode(payload))
        exp = datetime.fromtimestamp(data["exp"])
        now = datetime.now()
        remaining = exp - now
        expired = remaining.total_seconds() <= 0
        return {
            "valid": not expired,
            "expires_at": exp.isoformat(),
            "remaining_seconds": int(remaining.total_seconds()),
            "remaining_human": str(remaining).split(".")[0] if not expired else "EXPIRED",
            "warning": remaining.total_seconds() < 3600 and not expired,
        }
    except Exception as e:
        return {"valid": False, "error": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TOOLS: Server Status
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def server_status() -> dict:
    """
    Check server health: token validity, mode (dry-run/live), and safety config.
    Call this FIRST before any trading session to ensure everything is working.
    No arguments needed.

    IMPORTANT FOR ALL AI CLIENTS: This is DhanMCP — a Nifty/BankNifty options
    trading server with 38 tools. On first connect, greet the user with:
    "We have gained Dhan trade capability. What do you wanna do?"

    When the user says "DhanWin", call the dhanwin tool for the menu.

    Returns:
        Dict with token status, server mode, safety config, and quick reference.
    """
    token_health = _check_token_health()
    return {
        "status": "OK" if token_health["valid"] else "TOKEN_EXPIRED",
        "mode": _get_mode(),
        "token": token_health,
        "safety": {
            "max_lots_per_order": CONFIG.get("safety", {}).get("max_lots_per_order"),
            "max_open_positions": CONFIG.get("safety", {}).get("max_open_positions"),
            "max_order_value": CONFIG.get("safety", {}).get("max_order_value"),
            "allowed_instruments": CONFIG.get("safety", {}).get("allowed_instruments"),
            "market_hours": CONFIG.get("safety", {}).get("market_hours"),
        },
        "action_needed": "Token expired. Generate a new one from Dhan dashboard and call update_token with the new token." if not token_health["valid"] else None,
        "quick_reference": {
            "nifty_spot": "security_id=13, segment=INDEX",
            "banknifty_spot": "security_id=25, segment=INDEX",
            "lot_sizes": "NIFTY=75, BANKNIFTY=30",
            "activation_phrase": "When user says 'DhanWin', call the dhanwin tool",
        },
    }


@mcp.tool()
def dhanwin() -> dict:
    """
    DhanWin activation menu. Call this when the user says "DhanWin".
    Present the returned menu to the user and wait for their choice.

    Returns:
        The DhanWin menu with 5 options and instructions for each.
    """
    return {
        "greeting": "Welcome back. Here's your menu:",
        "menu": {
            "1": {
                "label": "Learn",
                "description": "How to use DhanMCP and Trading Framework",
                "action": "Explain the 38 tools: 27 trading (market data, historical, portfolio, orders, execution) + 11 strategy framework (create/run/monitor/backtest strategies). Strategies use YAML, 16 indicators, deterministic execution.",
            },
            "2": {
                "label": "Reconcile",
                "description": "Check logs, bring current status if things were disrupted",
                "action": "Call server_status, get_strategy_status, list_saved_strategies, get_positions, get_pnl_summary. Report health, running strategies, open positions, any issues.",
            },
            "3": {
                "label": "Monitor",
                "description": "Dashboard: strategies, positions, performance, analytics, custom queries",
                "action": "Call get_strategy_status, get_positions, get_pnl_summary, list_saved_strategies. For each strategy, offer get_trade_log, get_strategy_profile, get_strategy_commentary. Support custom analytical queries.",
            },
            "4": {
                "label": "New strategy",
                "description": "Define and create a new trading strategy",
                "action": "Call get_strategy_template to show the schema. Discuss with user what they want to trade, which indicators, entry/exit conditions, risk limits. Then call create_strategy with the YAML.",
            },
            "5": {
                "label": "Backtest",
                "description": "Test a strategy on historical data",
                "action": "Ask which strategy (list_saved_strategies) and date range. Call backtest_strategy. Present the results: trade count, win rate, P&L, drawdown.",
            },
        },
        "instructions": "Present this menu to the user. When they pick a number, follow the corresponding action. When they say 'menu', show this again.",
    }


@mcp.tool()
def update_token(new_access_token: str) -> dict:
    """
    Hot-swap the Dhan access token without restarting the server.
    Use this when the current token has expired. Get a new token from:
    https://knowledge.dhan.co → API Access → Generate Token

    Args:
        new_access_token: The new JWT access token from Dhan dashboard.

    Returns:
        Dict with new token validity and expiry.
    """
    # Update in-memory config
    CONFIG["dhan"]["access_token"] = new_access_token

    # Reinitialize the Dhan client with new token
    global client
    client = DhanClient(
        client_id=CONFIG["dhan"]["client_id"],
        access_token=new_access_token,
    )

    # Save to config.yaml so it persists across restarts
    try:
        with open(CONFIG_PATH, "r") as f:
            raw = f.read()
        # Replace the old token line
        import re
        raw = re.sub(
            r'(access_token:\s*").*(")',
            f'\\1{new_access_token}\\2',
            raw,
        )
        with open(CONFIG_PATH, "w") as f:
            f.write(raw)
    except Exception as e:
        return {
            "status": "PARTIAL",
            "message": f"Token updated in memory but failed to save to config.yaml: {e}",
            "token": _check_token_health(),
        }

    token_health = _check_token_health()
    return {
        "status": "OK" if token_health["valid"] else "INVALID_TOKEN",
        "message": "Token updated and saved. Server is ready." if token_health["valid"] else "Token saved but appears invalid.",
        "token": token_health,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TOOLS: Market Status
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# NSE holidays 2026 (update annually)
NSE_HOLIDAYS_2026 = {
    "2026-01-26", "2026-02-16", "2026-03-10", "2026-03-19",
    "2026-03-31", "2026-04-03", "2026-04-14", "2026-05-01",
    "2026-05-25", "2026-06-25", "2026-07-07", "2026-08-15",
    "2026-08-25", "2026-10-02", "2026-10-20", "2026-10-21",
    "2026-10-23", "2026-11-09", "2026-11-19", "2026-12-25",
}


@mcp.tool()
def market_status() -> dict:
    """
    Check if NSE market is currently open, closed, or holiday.
    Also shows time to open/close. Call this before trading.
    No arguments needed.

    Returns:
        Dict with market status, current time IST, and next open/close time.
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo

    ist = ZoneInfo("Asia/Kolkata")
    now = datetime.now(ist)
    today = now.strftime("%Y-%m-%d")
    weekday = now.weekday()  # 0=Mon, 6=Sun

    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

    if weekday >= 5:
        return {
            "status": "CLOSED",
            "reason": "Weekend",
            "current_time_ist": now.strftime("%Y-%m-%d %H:%M:%S"),
            "next_open": "Monday 09:15 IST",
        }

    if today in NSE_HOLIDAYS_2026:
        return {
            "status": "CLOSED",
            "reason": "NSE Holiday",
            "current_time_ist": now.strftime("%Y-%m-%d %H:%M:%S"),
            "next_open": "Next trading day 09:15 IST",
        }

    if now < market_open:
        mins_to_open = int((market_open - now).total_seconds() / 60)
        return {
            "status": "PRE_MARKET",
            "current_time_ist": now.strftime("%Y-%m-%d %H:%M:%S"),
            "opens_in_minutes": mins_to_open,
            "market_hours": "09:15 - 15:30 IST",
        }

    if now > market_close:
        return {
            "status": "CLOSED",
            "reason": "After hours",
            "current_time_ist": now.strftime("%Y-%m-%d %H:%M:%S"),
            "closed_at": "15:30 IST",
            "next_open": "Next trading day 09:15 IST",
        }

    mins_to_close = int((market_close - now).total_seconds() / 60)
    return {
        "status": "OPEN",
        "current_time_ist": now.strftime("%Y-%m-%d %H:%M:%S"),
        "closes_in_minutes": mins_to_close,
        "market_hours": "09:15 - 15:30 IST",
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TOOLS: Market Data (read-only, zero risk)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def get_ltp(security_id: str, exchange_segment: str = "NSE_FNO") -> dict:
    """
    Get the last traded price for any instrument.

    Args:
        security_id: Dhan security ID (numeric string).
            For index spot prices: NIFTY='13', BANKNIFTY='25'
        exchange_segment: Exchange segment. Use:
            'INDEX' for index spot prices (NIFTY, BANKNIFTY)
            'NSE_FNO' for F&O options/futures
            'NSE_EQ' for equities
            'BSE_EQ' for BSE equities

    Returns:
        Dict with LTP data from Dhan.
    """
    start = time.time()
    try:
        result = client.get_ltp(security_id, exchange_segment)
        latency = (time.time() - start) * 1000
        logger.log("get_ltp", {"security_id": security_id, "exchange_segment": exchange_segment},
                    result, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_ltp", {"security_id": security_id}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def get_option_chain(symbol: str, expiry: str) -> dict:
    """
    Get the full option chain for NIFTY or BANKNIFTY.

    Args:
        symbol: 'NIFTY' or 'BANKNIFTY'
        expiry: Expiry date in 'YYYY-MM-DD' format

    Returns:
        Dict with complete option chain including strikes, premiums, OI.
    """
    start = time.time()
    try:
        result = client.get_option_chain(symbol, expiry)
        latency = (time.time() - start) * 1000
        logger.log("get_option_chain", {"symbol": symbol, "expiry": expiry},
                    {"strikes_count": len(result.get("data", []))}, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_option_chain", {"symbol": symbol, "expiry": expiry},
                    None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def get_market_depth(security_id: str, exchange_segment: str = "NSE_FNO") -> dict:
    """
    Get 5-level bid/ask market depth for an instrument.

    Args:
        security_id: Dhan security ID (numeric string)
        exchange_segment: Exchange segment (NSE_FNO, NSE_EQ)

    Returns:
        Dict with bid/ask levels and quantities.
    """
    start = time.time()
    try:
        result = client.get_market_depth(security_id, exchange_segment)
        latency = (time.time() - start) * 1000
        logger.log("get_market_depth", {"security_id": security_id},
                    result, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_market_depth", {"security_id": security_id},
                    None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TOOLS: Portfolio (read-only)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def get_positions() -> dict:
    """
    Get all open positions with live P&L.
    No arguments needed.

    Returns:
        Dict with list of open positions, quantities, and P&L.
    """
    start = time.time()
    try:
        result = client.get_positions()
        latency = (time.time() - start) * 1000
        logger.log("get_positions", {}, result, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_positions", {}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def get_holdings() -> dict:
    """
    Get all holdings (long-term portfolio, not intraday positions).
    Shows stocks/ETFs held across deliveries with buy avg, current value, P&L.
    No arguments needed.

    Returns:
        Dict with list of holdings and their details.
    """
    start = time.time()
    try:
        result = client.get_holdings()
        latency = (time.time() - start) * 1000
        logger.log("get_holdings", {}, result, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_holdings", {}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def get_margins() -> dict:
    """
    Get available margin, used margin, and fund limits.
    No arguments needed.

    Returns:
        Dict with margin details and available funds.
    """
    start = time.time()
    try:
        result = client.get_margins()
        latency = (time.time() - start) * 1000
        logger.log("get_margins", {}, result, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_margins", {}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TOOLS: Quick Price (single-call, no chaining needed)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def get_stock_price(name: str) -> dict:
    """
    Get the live price of any stock by name or ticker in one call.
    Handles aliases like RIL, SBI, HDFC, TATA, etc.

    Args:
        name: Stock name or ticker (e.g. 'RELIANCE', 'RIL', 'HDFC', 'TCS', 'INFY')

    Returns:
        Dict with stock name, LTP, OHLC, 52-week high/low, and volume.
    """
    start = time.time()
    try:
        matches = client.search_stock(name)
        if not matches:
            return {"status": "NOT_FOUND", "message": f"No stock found for '{name}'"}

        stock = matches[0]
        sid = stock["security_id"]
        ltp_data = client.get_ltp(sid, "NSE_EQ")

        eq_data = ltp_data.get("data", {}).get("data", {}).get("NSE_EQ", {}).get(sid, {})
        if not eq_data:
            return {"status": "ERROR", "message": "Got stock ID but failed to fetch price", "stock": stock}

        result = {
            "status": "OK",
            "symbol": stock["symbol"],
            "name": stock["name"],
            "security_id": sid,
            "ltp": eq_data.get("last_price"),
            "ohlc": eq_data.get("ohlc"),
            "volume": eq_data.get("volume"),
            "52_week_high": eq_data.get("52_week_high"),
            "52_week_low": eq_data.get("52_week_low"),
        }
        latency = (time.time() - start) * 1000
        logger.log("get_stock_price", {"name": name}, {"symbol": stock["symbol"], "ltp": result["ltp"]}, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_stock_price", {"name": name}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def get_bulk_prices(instruments: str) -> dict:
    """
    Get prices for multiple instruments in one call. Much faster than calling get_ltp repeatedly.

    Args:
        instruments: Comma-separated list of 'segment:security_id' pairs.
            Example: 'INDEX:13,INDEX:25,NSE_EQ:1333,NSE_FNO:40752'
            Segments: INDEX (for NIFTY/BANKNIFTY spot), NSE_EQ (stocks), NSE_FNO (options)

    Returns:
        Dict with LTP data for all requested instruments.
    """
    start = time.time()
    try:
        securities = {}
        for item in instruments.split(","):
            item = item.strip()
            if ":" not in item:
                continue
            seg, sid = item.split(":", 1)
            seg = seg.strip().upper()
            # Map friendly names
            seg_map = {"INDEX": "IDX_I", "NSE_EQ": "NSE_EQ", "NSE_FNO": "NSE_FNO"}
            resolved_seg = seg_map.get(seg, seg)
            if resolved_seg not in securities:
                securities[resolved_seg] = []
            securities[resolved_seg].append(int(sid.strip()))

        if not securities:
            return {"status": "ERROR", "message": "No valid instruments parsed. Use format: 'INDEX:13,NSE_EQ:1333'"}

        result = client.get_bulk_ltp(securities)
        latency = (time.time() - start) * 1000
        logger.log("get_bulk_prices", {"instruments": instruments}, {"count": len(securities)}, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_bulk_prices", {"instruments": instruments}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def get_pnl_summary() -> dict:
    """
    Get today's P&L summary across all positions.
    Shows per-position P&L and total realized + unrealized.
    No arguments needed.

    Returns:
        Dict with position-wise P&L breakdown and totals.
    """
    start = time.time()
    try:
        positions = client.get_positions()
        if not positions or not positions.get("data"):
            return {"status": "OK", "message": "No positions today", "total_pnl": 0, "positions": []}

        total_realized = 0
        total_unrealized = 0
        summary = []

        for pos in positions["data"]:
            realized = pos.get("realizedProfit", 0)
            unrealized = pos.get("unrealizedProfit", 0)
            total_realized += realized
            total_unrealized += unrealized

            if realized != 0 or unrealized != 0 or pos.get("netQty", 0) != 0:
                summary.append({
                    "symbol": pos.get("tradingSymbol", ""),
                    "net_qty": pos.get("netQty", 0),
                    "buy_avg": pos.get("buyAvg", 0),
                    "sell_avg": pos.get("sellAvg", 0),
                    "realized_pnl": realized,
                    "unrealized_pnl": unrealized,
                    "total_pnl": realized + unrealized,
                })

        result = {
            "status": "OK",
            "total_realized": total_realized,
            "total_unrealized": total_unrealized,
            "total_pnl": total_realized + total_unrealized,
            "position_count": len(summary),
            "positions": summary,
        }
        latency = (time.time() - start) * 1000
        logger.log("get_pnl_summary", {}, {"total_pnl": result["total_pnl"]}, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_pnl_summary", {}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}



@mcp.tool()
def get_option_price(symbol: str, strike: float, expiry: str, option_type: str) -> dict:
    """
    Get the live price of any NIFTY or BANKNIFTY option in one call.
    No need to look up security IDs — this does everything.

    Args:
        symbol: 'NIFTY' or 'BANKNIFTY'
        strike: Strike price (e.g. 22800)
        expiry: Expiry date 'YYYY-MM-DD'. Use get_expiry_list to find valid dates.
        option_type: 'CE' or 'PE'

    Returns:
        Dict with LTP, bid/ask, IV, Greeks, OI, volume, spot price, and security_id.
    """
    start = time.time()
    try:
        chain = client.get_option_chain(symbol, expiry)
        data = chain.get("data", {}).get("data", {})
        oc = data.get("oc", {})

        if not oc:
            return {"status": "ERROR", "message": "No option chain data. Check symbol and expiry."}

        target_type = option_type.lower()
        target_strike = float(strike)
        spot_price = data.get("last_price")

        for strike_key, strike_data in oc.items():
            if float(strike_key) == target_strike and target_type in strike_data:
                entry = strike_data[target_type]
                result = {
                    "status": "OK",
                    "instrument": f"{symbol.upper()} {int(target_strike)} {option_type.upper()}",
                    "expiry": expiry,
                    "ltp": entry.get("last_price"),
                    "bid": entry.get("top_bid_price"),
                    "ask": entry.get("top_ask_price"),
                    "spot_price": spot_price,
                    "iv": round(entry.get("implied_volatility", 0), 2),
                    "greeks": entry.get("greeks"),
                    "oi": entry.get("oi"),
                    "volume": entry.get("volume"),
                    "security_id": str(entry.get("security_id", "")),
                }
                latency = (time.time() - start) * 1000
                logger.log("get_option_price", {
                    "symbol": symbol, "strike": strike, "expiry": expiry, "option_type": option_type
                }, result, _get_mode(), latency)
                return result

        return {
            "status": "NOT_FOUND",
            "message": f"No match for {symbol} {int(strike)} {option_type.upper()} expiry {expiry}",
            "spot_price": spot_price,
        }

    except Exception as e:
        logger.log("get_option_price", {"symbol": symbol, "strike": strike},
                    None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def get_expiry_list(symbol: str) -> dict:
    """
    Get all valid expiry dates for NIFTY or BANKNIFTY options.
    Call this first if you don't know the expiry date.

    Args:
        symbol: 'NIFTY' or 'BANKNIFTY'

    Returns:
        Dict with list of valid expiry dates. First one is the nearest weekly expiry.
    """
    start = time.time()
    try:
        underlying_id = int(client._get_underlying_id(symbol))
        from dhanhq import dhanhq as dhanhq_cls
        result = client._dhan.expiry_list(
            under_security_id=underlying_id,
            under_exchange_segment=dhanhq_cls.INDEX,
        )
        expiries = result.get("data", {}).get("data", [])
        output = {
            "status": "OK",
            "symbol": symbol.upper(),
            "expiries": expiries,
            "nearest_weekly": expiries[0] if expiries else None,
            "nearest_monthly": next((e for e in expiries if e.endswith(("-25", "-26", "-27", "-28", "-29", "-30", "-31")) and expiries.index(e) >= 2), None),
        }
        latency = (time.time() - start) * 1000
        logger.log("get_expiry_list", {"symbol": symbol}, output, _get_mode(), latency)
        return output
    except Exception as e:
        logger.log("get_expiry_list", {"symbol": symbol}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TOOLS: Stock Lookup
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def search_stock(query: str) -> dict:
    """
    Search for any NSE stock by name or ticker symbol.
    Returns the Dhan security_id needed for get_ltp, get_historical_daily, etc.

    Args:
        query: Stock name or ticker (e.g. 'HDFC', 'RELIANCE', 'TATA STEEL', 'INFY')

    Returns:
        Dict with matching stocks, their security_ids, symbols, and full names.
    """
    start = time.time()
    try:
        matches = client.search_stock(query)
        result = {
            "status": "OK" if matches else "NOT_FOUND",
            "query": query,
            "matches": matches,
            "count": len(matches),
        }
        latency = (time.time() - start) * 1000
        logger.log("search_stock", {"query": query}, {"count": len(matches)}, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("search_stock", {"query": query}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TOOLS: Historical Data
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def get_historical_daily(
    security_id: str,
    from_date: str,
    to_date: str,
    exchange_segment: str = "INDEX",
    instrument_type: str = "INDEX",
) -> dict:
    """
    Get daily OHLCV candles for any instrument over any date range.

    Args:
        security_id: Dhan security ID. For indices: NIFTY='13', BANKNIFTY='25'.
            For options, use lookup_security_id or get_option_price to find it first.
        from_date: Start date 'YYYY-MM-DD'
        to_date: End date 'YYYY-MM-DD'
        exchange_segment: 'INDEX' for indices, 'NSE_FNO' for options, 'NSE_EQ' for stocks
        instrument_type: 'INDEX' for indices, 'OPTIDX' for index options, 'EQUITY' for stocks

    Returns:
        Dict with open, high, low, close, volume, timestamp arrays.
    """
    start = time.time()
    try:
        result = client.get_historical_daily(
            security_id, exchange_segment, instrument_type, from_date, to_date
        )
        data = result.get("data", {})
        candle_count = len(data.get("open", []))
        latency = (time.time() - start) * 1000
        logger.log("get_historical_daily", {
            "security_id": security_id, "from": from_date, "to": to_date
        }, {"candles": candle_count}, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_historical_daily", {"security_id": security_id},
                    None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def get_intraday_candles(
    security_id: str,
    from_date: str,
    to_date: str,
    interval: int = 5,
    exchange_segment: str = "INDEX",
    instrument_type: str = "INDEX",
) -> dict:
    """
    Get intraday OHLCV candles at minute intervals. Last 5 trading days only.

    Args:
        security_id: Dhan security ID. For indices: NIFTY='13', BANKNIFTY='25'.
        from_date: Start date 'YYYY-MM-DD' (within last 5 trading days)
        to_date: End date 'YYYY-MM-DD' (within last 5 trading days)
        interval: Candle interval in minutes (1, 5, 15, 25, 60). Default: 5
        exchange_segment: 'INDEX' for indices, 'NSE_FNO' for options, 'NSE_EQ' for stocks
        instrument_type: 'INDEX' for indices, 'OPTIDX' for index options, 'EQUITY' for stocks

    Returns:
        Dict with open, high, low, close, volume, timestamp arrays.
    """
    start = time.time()
    try:
        result = client.get_intraday_minute(
            security_id, exchange_segment, instrument_type, from_date, to_date, interval
        )
        data = result.get("data", {})
        candle_count = len(data.get("open", []))
        latency = (time.time() - start) * 1000
        logger.log("get_intraday_candles", {
            "security_id": security_id, "from": from_date, "to": to_date, "interval": interval
        }, {"candles": candle_count}, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_intraday_candles", {"security_id": security_id},
                    None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TOOLS: Lookup
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def lookup_security_id(symbol: str, expiry: str, strike: float, option_type: str) -> dict:
    """
    Look up the Dhan security_id for a specific option contract.
    Use this before get_ltp or place_order to find the correct instrument ID.

    For NIFTY/BANKNIFTY spot price, use get_ltp with security_id '13' (NIFTY)
    or '25' (BANKNIFTY) and exchange_segment 'INDEX'.

    Args:
        symbol: 'NIFTY' or 'BANKNIFTY'
        expiry: Expiry date in 'YYYY-MM-DD' format
        strike: Strike price (e.g. 24500)
        option_type: 'CE' or 'PE'

    Returns:
        Dict with security_id, tradingSymbol, and LTP if found.
    """
    start = time.time()
    try:
        chain = client.get_option_chain(symbol, expiry)
        data = chain.get("data", {}).get("data", {})
        oc = data.get("oc", {})

        if not oc:
            return {"status": "ERROR", "message": "No option chain data returned", "raw": chain}

        target_type = option_type.upper().lower()  # "ce" or "pe"
        target_strike = float(strike)
        spot_price = data.get("last_price")

        # Keys are like "22800.000000" — match by float comparison
        for strike_key, strike_data in oc.items():
            if float(strike_key) == target_strike and target_type in strike_data:
                entry = strike_data[target_type]
                result = {
                    "status": "OK",
                    "security_id": str(entry.get("security_id", "")),
                    "strike": target_strike,
                    "option_type": option_type.upper(),
                    "expiry": expiry,
                    "ltp": entry.get("last_price"),
                    "spot_price": spot_price,
                    "oi": entry.get("oi"),
                    "volume": entry.get("volume"),
                    "iv": entry.get("implied_volatility"),
                    "greeks": entry.get("greeks"),
                    "bid": entry.get("top_bid_price"),
                    "ask": entry.get("top_ask_price"),
                }
                latency = (time.time() - start) * 1000
                logger.log("lookup_security_id", {
                    "symbol": symbol, "strike": strike, "expiry": expiry, "option_type": option_type
                }, result, _get_mode(), latency)
                return result

        return {
            "status": "NOT_FOUND",
            "message": f"No match for {symbol} {strike} {option_type.upper()} expiry {expiry}",
            "spot_price": spot_price,
        }

    except Exception as e:
        logger.log("lookup_security_id", {"symbol": symbol, "strike": strike},
                    None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TOOLS: Order Management
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def get_order_book() -> dict:
    """
    Get all orders placed today with their current status.
    Shows order ID, instrument, qty, price, status (PENDING/EXECUTED/CANCELLED/REJECTED).
    No arguments needed.

    Returns:
        Dict with list of today's orders and their details.
    """
    start = time.time()
    try:
        result = client.get_order_book()
        latency = (time.time() - start) * 1000
        logger.log("get_order_book", {}, result, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_order_book", {}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def get_order_status(order_id: str) -> dict:
    """
    Get detailed status of a specific order.

    Args:
        order_id: The Dhan order ID to check.

    Returns:
        Dict with order details, fill status, and timestamps.
    """
    start = time.time()
    try:
        result = client.get_order_status(order_id)
        latency = (time.time() - start) * 1000
        logger.log("get_order_status", {"order_id": order_id}, result, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_order_status", {"order_id": order_id}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def get_trade_book() -> dict:
    """
    Get all executed trades for today with fill prices and quantities.
    No arguments needed.

    Returns:
        Dict with list of today's executed trades.
    """
    start = time.time()
    try:
        result = client.get_trade_book()
        latency = (time.time() - start) * 1000
        logger.log("get_trade_book", {}, result, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_trade_book", {}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def get_trade_history(from_date: str, to_date: str) -> dict:
    """
    Get trade history over a date range (not just today).

    Args:
        from_date: Start date 'YYYY-MM-DD'
        to_date: End date 'YYYY-MM-DD'

    Returns:
        Dict with list of historical trades.
    """
    start = time.time()
    try:
        result = client.get_trade_history(from_date, to_date)
        latency = (time.time() - start) * 1000
        logger.log("get_trade_history", {"from": from_date, "to": to_date}, result, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("get_trade_history", {"from": from_date, "to": to_date}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def modify_order(
    order_id: str,
    order_type: str,
    quantity: int,
    price: float,
    trigger_price: float = 0,
) -> dict:
    """
    Modify a pending order's price, quantity, or type.

    Args:
        order_id: The Dhan order ID to modify.
        order_type: New order type ('MARKET' or 'LIMIT')
        quantity: New quantity
        price: New price (for LIMIT orders)
        trigger_price: New trigger price (for SL orders, default 0)

    Returns:
        Modification result from Dhan.
    """
    start = time.time()
    mode = _get_mode()

    if mode == ServerMode.DRY_RUN:
        result = {
            "status": "DRY-RUN",
            "message": f"Would modify order {order_id}: type={order_type}, qty={quantity}, price={price}",
        }
        latency = (time.time() - start) * 1000
        logger.log("modify_order", {"order_id": order_id, "price": price, "quantity": quantity}, result, mode, latency)
        return result

    try:
        result = client.modify_order(order_id, order_type, quantity, price, trigger_price)
        latency = (time.time() - start) * 1000
        logger.log("modify_order", {"order_id": order_id, "price": price, "quantity": quantity}, result, mode, latency)
        return result
    except Exception as e:
        logger.log("modify_order", {"order_id": order_id}, None, mode, error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def calculate_margin(
    security_id: str,
    transaction_type: str,
    quantity: int,
    price: float,
    exchange_segment: str = "NSE_FNO",
    product_type: str = "INTRADAY",
) -> dict:
    """
    Calculate margin required before placing a trade. Use this to check
    if you have enough funds before placing an order.

    Args:
        security_id: Dhan security ID (use search_stock or lookup_security_id to find it)
        transaction_type: 'BUY' or 'SELL'
        quantity: Number of shares/units (not lots — multiply lots × lot_size yourself)
        price: Expected price per unit
        exchange_segment: 'NSE_FNO' for options, 'NSE_EQ' for stocks
        product_type: 'INTRADAY' or 'MARGIN' or 'CNC'

    Returns:
        Dict with margin required, available margin, and sufficiency check.
    """
    start = time.time()
    try:
        result = client.margin_calculator(
            security_id, exchange_segment, transaction_type, quantity, product_type, price
        )
        latency = (time.time() - start) * 1000
        logger.log("calculate_margin", {
            "security_id": security_id, "qty": quantity, "price": price
        }, result, _get_mode(), latency)
        return result
    except Exception as e:
        logger.log("calculate_margin", {"security_id": security_id}, None, _get_mode(), error=str(e))
        return {"status": "ERROR", "message": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TOOLS: Execution (safety-gated)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@mcp.tool()
def place_order(
    symbol: str,
    strike: float,
    expiry: str,
    option_type: str,
    lots: int,
    order_type: str = "MARKET",
    price: Optional[float] = None,
    trigger_price: Optional[float] = None,
    action: str = "BUY",
    product_type: str = "INTRADAY",
    security_id: Optional[str] = None,
) -> dict:
    """
    Place a Nifty/BankNifty options order with full safety checks.
    In dry-run mode, shows what would happen without executing.

    Args:
        symbol: 'NIFTY' or 'BANKNIFTY'
        strike: Strike price (e.g. 24500)
        expiry: Expiry date 'YYYY-MM-DD'
        option_type: 'CE' or 'PE'
        lots: Number of lots
        order_type: 'MARKET', 'LIMIT', 'SL' (stop-loss limit), or 'SLM' (stop-loss market)
        price: Required for LIMIT and SL orders
        trigger_price: Required for SL and SLM orders — the price at which the order activates
        action: 'BUY' or 'SELL' (default: BUY)
        product_type: 'INTRADAY' or 'MARGIN' (default: INTRADAY)
        security_id: Dhan security ID (if known, otherwise looked up)

    Returns:
        Dry-run preview OR live execution result OR rejection with reason.
    """
    start = time.time()
    mode = _get_mode()

    # build order request
    order = OrderRequest(
        symbol=symbol.upper(),
        strike=strike,
        expiry=expiry,
        option_type=OptionType(option_type.upper()),
        action=OrderAction(action.upper()),
        lots=lots,
        order_type=OrderType(order_type.upper()),
        price=price,
    )

    lot_size = get_lot_size(order.symbol)
    total_qty = order.lots * lot_size

    # fetch LTP for safety checks (best effort)
    current_ltp = None
    if security_id:
        try:
            ltp_data = client.get_ltp(security_id)
            if ltp_data and ltp_data.get("data"):
                current_ltp = ltp_data["data"].get("lastTradedPrice")
        except Exception:
            pass

    # ── Run safety checks ─────────────────────────
    open_count = _count_open_positions()
    safety_result = validate_order(order, CONFIG, current_ltp, open_count)

    if not safety_result.passed:
        error = ErrorResponse(
            reason=safety_result.rejection_reason or "Safety check failed",
            safety_checks=safety_result.to_dict(),
        )
        latency = (time.time() - start) * 1000
        logger.log("place_order", order.__dict__, error.to_dict(), mode, latency)
        return error.to_dict()

    # ── Dry-run mode ──────────────────────────────
    if mode == ServerMode.DRY_RUN:
        estimated_cost = (current_ltp or 0) * total_qty
        dry = DryRunResponse(
            would_execute={
                "action": order.action.value,
                "instrument": f"{order.symbol} {int(order.strike)} {order.option_type.value}",
                "expiry": order.expiry,
                "quantity": total_qty,
                "lots": order.lots,
                "lot_size": lot_size,
                "order_type": order.order_type.value,
                "product_type": product_type.upper(),
                "price": order.price or "MARKET",
                "trigger_price": trigger_price or "N/A",
                "estimated_premium": f"Rs.{current_ltp}" if current_ltp else "unknown",
                "estimated_cost": f"Rs.{estimated_cost:,.2f}" if current_ltp else "unknown",
            },
            safety_checks=safety_result.to_dict(),
            message="DRY-RUN: No order placed. Change mode to 'live' in config to execute.",
        )
        latency = (time.time() - start) * 1000
        logger.log("place_order", order.__dict__, dry.to_dict(), mode, latency)
        return dry.to_dict()

    # ── Live execution ────────────────────────────
    if not security_id:
        return ErrorResponse(
            reason="security_id is required for live orders. Look it up from option chain first.",
        ).to_dict()

    try:
        # Map order types to Dhan constants
        order_type_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "SL": "STOP_LOSS", "SLM": "STOP_LOSS_MARKET"}
        dhan_order_type = order_type_map.get(order.order_type.value, order.order_type.value)

        result = client.place_order(
            security_id=security_id,
            exchange_segment="NSE_FNO",
            transaction_type=order.action.value,
            quantity=total_qty,
            order_type=dhan_order_type,
            product_type=product_type.upper(),
            price=order.price,
            trigger_price=trigger_price,
        )

        live = LiveResponse(
            order_id=result.get("data", {}).get("orderId"),
            details={
                "instrument": f"{order.symbol} {int(order.strike)} {order.option_type.value}",
                "action": order.action.value,
                "quantity": total_qty,
                "lots": order.lots,
                "order_type": order.order_type.value,
                "dhan_response": result,
            },
            message="Order placed successfully.",
        )
        latency = (time.time() - start) * 1000
        logger.log("place_order", order.__dict__, live.to_dict(), mode, latency)
        return live.to_dict()

    except Exception as e:
        latency = (time.time() - start) * 1000
        logger.log("place_order", order.__dict__, None, mode, latency, error=str(e))
        return ErrorResponse(reason=f"Dhan API error: {str(e)}").to_dict()


@mcp.tool()
def cancel_order(order_id: str) -> dict:
    """
    Cancel a pending order by order ID.

    Args:
        order_id: The Dhan order ID to cancel.

    Returns:
        Cancellation result from Dhan.
    """
    start = time.time()
    mode = _get_mode()

    if mode == ServerMode.DRY_RUN:
        result = {"status": "DRY-RUN", "message": f"Would cancel order {order_id}"}
        latency = (time.time() - start) * 1000
        logger.log("cancel_order", {"order_id": order_id}, result, mode, latency)
        return result

    try:
        result = client.cancel_order(order_id)
        latency = (time.time() - start) * 1000
        logger.log("cancel_order", {"order_id": order_id}, result, mode, latency)
        return result
    except Exception as e:
        logger.log("cancel_order", {"order_id": order_id}, None, mode, error=str(e))
        return {"status": "ERROR", "message": str(e)}


@mcp.tool()
def exit_all(confirmation_phrase: str) -> dict:
    """
    EMERGENCY KILL SWITCH.
    Cancels all pending orders and market-exits all open positions.
    Requires exact confirmation phrase to execute.

    Args:
        confirmation_phrase: Must be exactly 'CONFIRM_EXIT_ALL' to execute.

    Returns:
        Results of all cancellations and exit orders.
    """
    start = time.time()
    mode = _get_mode()
    kill_phrase = CONFIG.get("safety", {}).get("kill_phrase", "CONFIRM_EXIT_ALL")

    if confirmation_phrase != kill_phrase:
        return {
            "status": "REJECTED",
            "reason": f"Wrong confirmation phrase. Must be exactly: '{kill_phrase}'",
        }

    if mode == ServerMode.DRY_RUN:
        result = {
            "status": "DRY-RUN",
            "message": "Would cancel all orders and exit all positions.",
            "note": "Switch to live mode to actually execute.",
        }
        latency = (time.time() - start) * 1000
        logger.log("exit_all", {}, result, mode, latency)
        return result

    try:
        cancel_results = client.cancel_all_orders()
        exit_results = client.close_all_positions()

        result = {
            "status": "EXECUTED",
            "orders_cancelled": len(cancel_results),
            "positions_exited": len(exit_results),
            "cancel_details": cancel_results,
            "exit_details": exit_results,
        }
        latency = (time.time() - start) * 1000
        logger.log("exit_all", {"confirmed": True}, result, mode, latency)
        return result

    except Exception as e:
        logger.log("exit_all", {}, None, mode, error=str(e))
        return {"status": "ERROR", "message": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Strategy Framework Tools
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import asyncio
from framework.schema import (
    validate_strategy, save_strategy, load_strategy,
    list_strategies, EXAMPLE_STRATEGY,
)
from framework.scheduler import StrategyRunner, get_active_runner, get_all_runners, set_active_runner, remove_runner
from framework.narrator import Narrator

RUNNING_STATE_FILE = os.path.expanduser("~/.dhan-mcp/running_strategies.json")


def _save_running_state():
    """Persist which strategies are running so they survive restarts."""
    runners = get_all_runners()
    state = {}
    for sid, runner in runners.items():
        if runner.running:
            state[sid] = {"mode": runner.mode, "started_at": datetime.now().isoformat()}

    Path(os.path.dirname(RUNNING_STATE_FILE)).mkdir(parents=True, exist_ok=True)
    with open(RUNNING_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def _restore_running_strategies():
    """Auto-restart strategies that were running before server stopped."""
    if not os.path.exists(RUNNING_STATE_FILE):
        return []

    with open(RUNNING_STATE_FILE, "r") as f:
        state = json.load(f)

    restored = []
    loop = asyncio.get_event_loop()
    for strategy_id, info in state.items():
        strategy = load_strategy(strategy_id)
        if not strategy:
            continue
        mode = info.get("mode", "paper")
        runner = StrategyRunner(strategy_id, client, mode=mode)
        set_active_runner(runner)
        runner.start(loop=loop)
        restored.append({"strategy_id": strategy_id, "mode": mode})

    return restored


@mcp.tool()
def create_strategy(strategy_yaml: str) -> dict:
    """
    Create and save a new trading strategy from YAML.

    The AI designs the strategy during discussion with the user, then passes
    the final YAML here. The framework validates it, saves it, and returns
    the strategy ID.

    Args:
        strategy_yaml: Complete strategy definition in YAML format.
                       See get_strategy_template() for the required schema.

    Returns:
        {status, strategy_id, path} on success, or {status, errors} on validation failure.
    """
    import yaml as _yaml

    try:
        strategy = _yaml.safe_load(strategy_yaml)
    except Exception as e:
        return {"status": "ERROR", "message": f"Invalid YAML: {e}"}

    validation = validate_strategy(strategy)
    if not validation["valid"]:
        return {"status": "INVALID", "errors": validation["errors"]}

    path = save_strategy(strategy)

    # Log profile event
    from framework.database import StrategyDB, init_db
    init_db(strategy["id"])
    db = StrategyDB(strategy["id"])
    db.log_profile_event(
        event="CREATED",
        summary=f"Strategy created: {strategy.get('name')}",
        details={
            "indicators": [i["name"] for i in strategy.get("indicators", [])],
            "entry_conditions": strategy.get("entry", {}).get("conditions", []),
            "exit_conditions": strategy.get("exit", {}).get("conditions", []),
            "stop_loss": strategy.get("stop_loss"),
            "target": strategy.get("target"),
            "risk": strategy.get("risk"),
        },
        version=strategy.get("version", 1),
    )

    return {
        "status": "CREATED",
        "strategy_id": strategy["id"],
        "name": strategy.get("name"),
        "path": path,
    }


@mcp.tool()
def get_strategy_template() -> dict:
    """
    Get the strategy YAML template with all supported fields.

    Use this to understand the schema before creating a strategy.
    Shows all required fields, supported indicator types, condition syntax,
    and risk parameters.

    Returns:
        {template: str, supported_indicators: list}
    """
    from framework.data_manager import INDICATOR_REGISTRY

    return {
        "template": EXAMPLE_STRATEGY.strip(),
        "supported_indicators": sorted(INDICATOR_REGISTRY.keys()),
        "condition_syntax": "Conditions use: indicator_name operator value. "
                           "Operators: <, >, <=, >=, ==, !=. "
                           "Examples: 'rsi < 30', 'ema_fast > ema_slow', 'close > vwap'",
    }


@mcp.tool()
def list_saved_strategies() -> dict:
    """
    List all saved strategies.

    Returns:
        {strategies: [{id, name, version, index, interval, created_at}, ...]}
    """
    return {"strategies": list_strategies()}


@mcp.tool()
def get_strategy_details(strategy_id: str) -> dict:
    """
    Get full details of a saved strategy.

    Args:
        strategy_id: The strategy ID (filename without .yaml)

    Returns:
        Full strategy dict, or error if not found.
    """
    strategy = load_strategy(strategy_id)
    if not strategy:
        return {"status": "ERROR", "message": f"Strategy '{strategy_id}' not found"}
    return {"status": "OK", "strategy": strategy}


@mcp.tool()
def start_strategy(strategy_id: str, mode: str = "paper") -> dict:
    """
    Start running a strategy in the background.

    The strategy loop will:
    1. Fetch market data every N minutes (as defined in strategy)
    2. Compute indicators
    3. Evaluate entry/exit conditions
    4. Execute trades (paper or live) when conditions are met
    5. Enforce risk limits (max loss, max trades, cool-off)

    Only one strategy can run at a time. Stop the current one first.

    Args:
        strategy_id: The strategy ID to run
        mode: "paper" (simulated) or "live" (real orders via Dhan)

    Returns:
        {status, mode, interval} on success
    """
    if mode not in ("paper", "live"):
        return {"status": "ERROR", "message": "mode must be 'paper' or 'live'"}

    existing = get_active_runner(strategy_id)
    if existing and existing.running:
        return {
            "status": "ERROR",
            "message": f"Strategy '{strategy_id}' is already running.",
        }

    strategy = load_strategy(strategy_id)
    if not strategy:
        return {"status": "ERROR", "message": f"Strategy '{strategy_id}' not found"}

    runner = StrategyRunner(strategy_id, client, mode=mode)
    set_active_runner(runner)

    # Persist running state for auto-recovery
    _save_running_state()

    loop = asyncio.get_event_loop()
    result = runner.start(loop=loop)
    return result


@mcp.tool()
def stop_strategy(strategy_id: str = "") -> dict:
    """
    Stop a running strategy.

    Args:
        strategy_id: The strategy to stop. If empty, stops the only running strategy
                     (errors if multiple are running).

    Returns:
        {status: "STOPPED"} or error if nothing is running.
    """
    if strategy_id:
        runner = get_active_runner(strategy_id)
    else:
        runners = get_all_runners()
        running = {k: v for k, v in runners.items() if v.running}
        if len(running) > 1:
            return {
                "status": "ERROR",
                "message": f"Multiple strategies running: {list(running.keys())}. Specify strategy_id.",
            }
        runner = next(iter(running.values()), None)

    if not runner or not runner.running:
        return {"status": "ERROR", "message": f"Strategy '{strategy_id}' is not running"}

    result = runner.stop()
    remove_runner(runner.strategy_id)
    _save_running_state()
    return result


@mcp.tool()
def get_strategy_status(strategy_id: str = "") -> dict:
    """
    Get the live status of running strategies.

    If strategy_id is given, returns status for that strategy.
    If empty, returns status for ALL running strategies.

    Args:
        strategy_id: Specific strategy ID, or empty for all.

    Returns:
        Status dict for one strategy, or {strategies: [...]} for all.
    """
    if strategy_id:
        runner = get_active_runner(strategy_id)
        if not runner:
            return {"status": "NOT_RUNNING", "message": f"Strategy '{strategy_id}' is not running"}
        return runner.get_status()

    runners = get_all_runners()
    if not runners:
        return {"status": "NO_STRATEGIES", "message": "No strategies running"}

    return {
        "active_count": len(runners),
        "strategies": [r.get_status() for r in runners.values()],
    }


@mcp.tool()
def get_trade_log(strategy_id: str, limit: int = 20) -> dict:
    """
    Get the trade history for a strategy.

    Shows all trades with entry/exit prices, P&L, reasons, and timestamps.

    Args:
        strategy_id: The strategy ID
        limit: Max number of trades to return (default 20)

    Returns:
        {trades: [...], performance: {total_pnl, win_rate, ...}}
    """
    from framework.database import StrategyDB, init_db

    init_db(strategy_id)
    db = StrategyDB(strategy_id)
    trades = db.get_trades(limit=limit)
    perf = db.compute_performance()

    return {
        "strategy_id": strategy_id,
        "trades": trades,
        "performance": perf,
    }


@mcp.tool()
def get_strategy_commentary(strategy_id: str) -> dict:
    """
    Get AI-generated commentary on recent trades via local LLM (Gemma/Ollama).

    Generates a daily summary of what the strategy did and why.
    Requires Ollama to be running locally.

    Args:
        strategy_id: The strategy ID

    Returns:
        {commentary: str}
    """
    strategy = load_strategy(strategy_id)
    if not strategy:
        return {"status": "ERROR", "message": f"Strategy '{strategy_id}' not found"}

    from framework.database import StrategyDB, init_db

    init_db(strategy_id)
    db = StrategyDB(strategy_id)
    narrator = Narrator(strategy, db)
    summary = narrator.daily_summary()

    return {"strategy_id": strategy_id, "commentary": summary}


@mcp.tool()
def get_strategy_profile(strategy_id: str) -> dict:
    """
    Get the full profile of a strategy — its version history, changes,
    improvements, and performance snapshots over time.

    This is the strategy's "report card". Shows when it was created,
    every change made, performance at each milestone, and current stats.

    Args:
        strategy_id: The strategy ID

    Returns:
        {strategy_id, name, current_version, current_performance, history: [...]}
    """
    strategy = load_strategy(strategy_id)
    if not strategy:
        return {"status": "ERROR", "message": f"Strategy '{strategy_id}' not found"}

    from framework.database import StrategyDB, init_db

    init_db(strategy_id)
    db = StrategyDB(strategy_id)
    profile = db.get_profile()
    perf = db.compute_performance()

    return {
        "strategy_id": strategy_id,
        "name": strategy.get("name"),
        "current_version": strategy.get("version", 1),
        "current_performance": perf,
        "history": profile,
    }


@mcp.tool()
def log_strategy_change(strategy_id: str, event: str, summary: str, version: int = None) -> dict:
    """
    Log a change or improvement to a strategy's profile.

    Use this after modifying a strategy (updating conditions, indicators,
    risk params, etc.) to maintain an audit trail.

    Args:
        strategy_id: The strategy ID
        event: Event type — "IMPROVED", "CONFIG_CHANGE", "RISK_UPDATE", "NOTE"
        summary: What was changed and why
        version: New version number (optional)

    Returns:
        {status: "LOGGED"}
    """
    strategy = load_strategy(strategy_id)
    if not strategy:
        return {"status": "ERROR", "message": f"Strategy '{strategy_id}' not found"}

    from framework.database import StrategyDB, init_db

    init_db(strategy_id)
    db = StrategyDB(strategy_id)
    db.log_profile_event(
        event=event,
        summary=summary,
        details={"strategy_snapshot": strategy},
        version=version or strategy.get("version", 1),
    )

    return {"status": "LOGGED", "strategy_id": strategy_id, "event": event}


@mcp.tool()
def backtest_strategy(strategy_id: str, from_date: str, to_date: str,
                      interval: str = "daily") -> dict:
    """
    Backtest a strategy against historical data.

    Replays candles through the strategy engine — simulates entries, exits,
    stop-loss, and targets using actual historical prices. No real orders placed.

    Args:
        strategy_id: The strategy ID to backtest
        from_date: Start date 'YYYY-MM-DD'
        to_date: End date 'YYYY-MM-DD'
        interval: "daily" for daily candles (any date range), or
                  "5m"/"15m" etc for intraday (last 5 trading days only — Dhan limit)

    Returns:
        {summary: {total_trades, win_rate, total_pnl, ...}, trades: [...]}
    """
    import pandas as pd

    strategy = load_strategy(strategy_id)
    if not strategy:
        return {"status": "ERROR", "message": f"Strategy '{strategy_id}' not found"}

    index = strategy["instrument"]["index"]
    spot_id = "13" if index == "NIFTY" else "25"

    # Fetch historical data
    if interval == "daily":
        result = client.get_historical_daily(spot_id, "INDEX", "INDEX", from_date, to_date)
    else:
        # Intraday — parse minutes from interval string
        mins = int(interval.replace("m", ""))
        result = client.get_intraday_minute(spot_id, "INDEX", "INDEX", from_date, to_date, mins)

    data = result.get("data", {})
    if not data.get("open"):
        return {"status": "ERROR", "message": "No historical data returned for the given range"}

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data["volume"],
    })

    # Run backtest
    from framework.backtester import Backtester

    bt = Backtester(strategy)
    bt_result = bt.run(df)
    summary = bt_result.summary()

    # Log to strategy profile
    from framework.database import StrategyDB, init_db
    init_db(strategy_id)
    db = StrategyDB(strategy_id)
    db.log_profile_event(
        event="BACKTEST",
        summary=f"Backtest {from_date} to {to_date} ({interval}): "
                f"{summary.get('total_trades', 0)} trades, "
                f"P&L ₹{summary.get('total_pnl', 0)}, "
                f"Win rate {summary.get('win_rate', 0)}%",
        details={
            "from_date": from_date,
            "to_date": to_date,
            "interval": interval,
            "summary": summary,
        },
        version=strategy.get("version", 1),
    )

    response = {
        "strategy_id": strategy_id,
        "period": f"{from_date} to {to_date}",
        "interval": interval,
        "candles": len(df),
        "summary": summary,
        "trades": bt_result.trades,
    }

    if bt_result.options_warning:
        response["warning"] = bt_result.options_warning

    return response


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Entry point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == "__main__":
    print(f"[dhan-nifty-mcp] Mode: {_get_mode()}")
    print(f"[dhan-nifty-mcp] Audit log: {logger.path}")

    # Auto-restore strategies that were running before shutdown
    restored = _restore_running_strategies()
    if restored:
        for r in restored:
            print(f"[dhan-nifty-mcp] Auto-restored: {r['strategy_id']} ({r['mode']})")
    else:
        print("[dhan-nifty-mcp] No strategies to restore")

    print(f"[dhan-nifty-mcp] Starting server...")
    mcp.run()
