"""
Data models for dhan-nifty-mcp.
Plain dataclasses, no external dependencies.
"""

from dataclasses import dataclass, field, asdict
from typing import Optional
from enum import Enum


# ── Enums ──────────────────────────────────────────────────

class OrderAction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SL = "SL"
    SLM = "SLM"


class OptionType(str, Enum):
    CE = "CE"
    PE = "PE"


class ProductType(str, Enum):
    INTRADAY = "INTRADAY"
    MARGIN = "MARGIN"
    CNC = "CNC"


class ServerMode(str, Enum):
    DRY_RUN = "dry-run"
    LIVE = "live"


# ── Order request ─────────────────────────────────────────

@dataclass
class OrderRequest:
    symbol: str               # "NIFTY" or "BANKNIFTY"
    strike: float             # 24500, 25000, etc.
    expiry: str               # "2026-04-09"
    option_type: OptionType   # CE or PE
    action: OrderAction       # BUY or SELL
    lots: int                 # number of lots (1 lot = 75 for NIFTY, 30 for BANKNIFTY)
    order_type: OrderType     # MARKET or LIMIT
    price: Optional[float] = None  # required for LIMIT orders


# ── Safety check result ───────────────────────────────────

@dataclass
class SafetyResult:
    passed: bool
    instrument_allowed: bool = True
    within_market_hours: bool = True
    within_lot_limit: bool = True
    within_position_limit: bool = True
    within_value_limit: bool = True
    price_sane: bool = True
    rejection_reason: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


# ── Responses ─────────────────────────────────────────────

@dataclass
class DryRunResponse:
    status: str = "DRY-RUN"
    would_execute: dict = field(default_factory=dict)
    safety_checks: dict = field(default_factory=dict)
    message: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class LiveResponse:
    status: str = "EXECUTED"
    order_id: Optional[str] = None
    details: dict = field(default_factory=dict)
    message: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ErrorResponse:
    status: str = "REJECTED"
    reason: str = ""
    safety_checks: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


# ── Lot size lookup ───────────────────────────────────────

LOT_SIZES = {
    "NIFTY": 75,
    "BANKNIFTY": 30,
}


def get_lot_size(symbol: str) -> int:
    return LOT_SIZES.get(symbol.upper(), 75)
