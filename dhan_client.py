"""
Dhan API client for dhan-nifty-mcp.
Thin wrapper around dhanhq SDK. All Dhan HTTP calls live here.
"""

from typing import Optional
import csv
import io
import requests
from dhanhq import dhanhq


class DhanClient:
    """
    Single point of contact with Dhan's API.
    All methods return raw dicts from the SDK.
    """

    # Dhan exchange segment constants
    NSE_FNO = dhanhq.FNO

    def __init__(self, client_id: str, access_token: str):
        self._dhan = dhanhq(client_id, access_token)

    # ── Market data ───────────────────────────────────

    def get_ltp(self, security_id: str, exchange_segment: str = "NSE_FNO") -> dict:
        """Get last traded price for a single instrument."""
        segment = self._resolve_segment(exchange_segment)
        response = self._dhan.quote_data(
            securities={segment: [int(security_id)]}
        )
        return response

    def get_option_chain(self, symbol: str, expiry: str) -> dict:
        """
        Get full option chain for a symbol and expiry.
        symbol: 'NIFTY' or 'BANKNIFTY'
        expiry: '2026-04-09' format
        """
        response = self._dhan.option_chain(
            under_security_id=int(self._get_underlying_id(symbol)),
            under_exchange_segment=dhanhq.INDEX,
            expiry=expiry,
        )
        return response

    def get_market_depth(self, security_id: str, exchange_segment: str = "NSE_FNO") -> dict:
        """Get 5-level bid/ask depth."""
        segment = self._resolve_segment(exchange_segment)
        response = self._dhan.quote_data(
            securities={segment: [int(security_id)]}
        )
        return response

    # ── Portfolio ─────────────────────────────────────

    def get_positions(self) -> dict:
        """Get all open positions with live P&L."""
        return self._dhan.get_positions()

    def get_margins(self) -> dict:
        """Get available and used margin."""
        return self._dhan.get_fund_limits()

    def get_holdings(self) -> dict:
        """Get all holdings (long-term portfolio, not intraday positions)."""
        return self._dhan.get_holdings()

    def get_historical_daily(self, security_id: str, exchange_segment: str,
                              instrument_type: str, from_date: str, to_date: str) -> dict:
        """Get daily OHLCV candles for any date range."""
        segment = self._resolve_segment(exchange_segment)
        return self._dhan.historical_daily_data(
            security_id=security_id,
            exchange_segment=segment,
            instrument_type=instrument_type,
            from_date=from_date,
            to_date=to_date,
        )

    def get_intraday_minute(self, security_id: str, exchange_segment: str,
                             instrument_type: str, from_date: str, to_date: str,
                             interval: int = 5) -> dict:
        """Get intraday minute candles (last 5 trading days only)."""
        segment = self._resolve_segment(exchange_segment)
        return self._dhan.intraday_minute_data(
            security_id=security_id,
            exchange_segment=segment,
            instrument_type=instrument_type,
            from_date=from_date,
            to_date=to_date,
            interval=interval,
        )

    # Common aliases that traders use
    STOCK_ALIASES = {
        "RIL": "RELIANCE",
        "HDFC": "HDFCBANK",
        "SBI": "SBIN",
        "TATA": "TATASTEEL",
        "TCS": "TCS",
        "INFY": "INFY",
        "BAJAJ": "BAJFINANCE",
        "ICICI": "ICICIBANK",
        "KOTAK": "KOTAKBANK",
        "AXIS": "AXISBANK",
        "LT": "LT",
        "ITC": "ITC",
        "WIPRO": "WIPRO",
        "HUL": "HINDUNILVR",
        "MARUTI": "MARUTI",
        "ADANI": "ADANIENT",
        "BHARTI": "BHARTIARTL",
        "AIRTEL": "BHARTIARTL",
        "SUNPHARMA": "SUNPHARMA",
        "TITAN": "TITAN",
        "TECHM": "TECHM",
        "HCLTECH": "HCLTECH",
        "ONGC": "ONGC",
        "NTPC": "NTPC",
        "POWERGRID": "POWERGRID",
        "JIOFINANCE": "JIOFIN",
        "JIOFIN": "JIOFIN",
        "ZOMATO": "ZOMATO",
        "PAYTM": "PAYTM",
    }

    def search_stock(self, query: str) -> list:
        """Search for a stock by name or symbol. Returns matching NSE equities."""
        query_upper = query.upper()
        # Resolve alias first
        resolved = self.STOCK_ALIASES.get(query_upper, query_upper)

        r = requests.get(dhanhq.COMPACT_CSV_URL, timeout=15)
        reader = csv.DictReader(io.StringIO(r.text))
        exact = []
        starts_with = []
        partial = []
        for row in reader:
            if row["SEM_EXM_EXCH_ID"] != "NSE" or row["SEM_SEGMENT"] != "E" or row["SEM_SERIES"] != "EQ":
                continue
            symbol = row.get("SEM_TRADING_SYMBOL", "").upper()
            name = row.get("SM_SYMBOL_NAME", "").upper()
            entry = {
                "security_id": row["SEM_SMST_SECURITY_ID"],
                "symbol": row.get("SEM_TRADING_SYMBOL", ""),
                "name": row.get("SM_SYMBOL_NAME", ""),
            }
            # Exact symbol match (resolved alias or original query)
            if symbol == resolved:
                exact.insert(0, entry)
            elif symbol == query_upper and query_upper != resolved:
                exact.append(entry)
            elif symbol.startswith(resolved) or name.startswith(resolved):
                starts_with.append(entry)
            elif resolved in symbol or resolved in name:
                partial.append(entry)
        return (exact + starts_with + partial)[:10]

    def get_order_book(self) -> dict:
        """Get all orders for today."""
        return self._dhan.get_order_list()

    def get_order_status(self, order_id: str) -> dict:
        """Get details and status of a specific order."""
        return self._dhan.get_order_by_id(order_id)

    def get_trade_book(self, order_id: str = None) -> dict:
        """Get all executed trades for today, or for a specific order."""
        return self._dhan.get_trade_book(order_id)

    def get_trade_history(self, from_date: str, to_date: str, page: int = 0) -> dict:
        """Get trade history for a date range."""
        return self._dhan.get_trade_history(from_date, to_date, page)

    def modify_order(self, order_id: str, order_type: str, quantity: int,
                      price: float, trigger_price: float = 0) -> dict:
        """Modify a pending order."""
        return self._dhan.modify_order(
            order_id=order_id,
            order_type=order_type,
            leg_name="ENTRY_LEG",
            quantity=quantity,
            price=price,
            trigger_price=trigger_price,
            disclosed_quantity=0,
            validity="DAY",
        )

    def margin_calculator(self, security_id: str, exchange_segment: str,
                           transaction_type: str, quantity: int, product_type: str,
                           price: float) -> dict:
        """Calculate margin required for a trade."""
        segment = self._resolve_segment(exchange_segment)
        return self._dhan.margin_calculator(
            security_id=security_id,
            exchange_segment=segment,
            transaction_type=transaction_type,
            quantity=quantity,
            product_type=product_type,
            price=price,
        )

    def get_bulk_ltp(self, securities: dict) -> dict:
        """
        Get LTP for multiple instruments in one call.
        securities: {"NSE_EQ": [1333, 2885], "IDX_I": [13], "NSE_FNO": [40752]}
        """
        return self._dhan.quote_data(securities=securities)

    # ── Order execution ──────────────────────────────

    def place_order(
        self,
        security_id: str,
        exchange_segment: str,
        transaction_type: str,
        quantity: int,
        order_type: str,
        product_type: str,
        price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        bo_profit_value: Optional[float] = None,
        bo_stop_loss_value: Optional[float] = None,
    ) -> dict:
        """
        Place a single order.
        Returns Dhan's order response with order_id.
        """
        segment = self._resolve_segment(exchange_segment)

        params = {
            "security_id": security_id,
            "exchange_segment": segment,
            "transaction_type": transaction_type,
            "quantity": quantity,
            "order_type": order_type.upper(),
            "product_type": product_type.upper(),
        }

        if price is not None:
            params["price"] = price
        if trigger_price is not None:
            params["trigger_price"] = trigger_price
        if bo_profit_value is not None:
            params["bo_profit_value"] = bo_profit_value
        if bo_stop_loss_value is not None:
            params["bo_stop_loss_Value"] = bo_stop_loss_value

        return self._dhan.place_order(**params)

    def cancel_order(self, order_id: str) -> dict:
        """Cancel a pending order."""
        return self._dhan.cancel_order(order_id)

    def cancel_all_orders(self) -> list:
        """Cancel every open/pending order. Returns list of results."""
        orders = self._dhan.get_order_list()
        results = []
        if orders and orders.get("data"):
            for order in orders["data"]:
                status = order.get("orderStatus", "").upper()
                if status in ("PENDING", "TRANSIT", "OPEN"):
                    oid = order.get("orderId")
                    if oid:
                        r = self._dhan.cancel_order(str(oid))
                        results.append({"order_id": oid, "result": r})
        return results

    def close_all_positions(self) -> list:
        """
        Market-sell every open position.
        Returns list of exit order results.
        """
        positions = self._dhan.get_positions()
        results = []
        if positions and positions.get("data"):
            for pos in positions["data"]:
                qty = pos.get("netQty", 0)
                if qty == 0:
                    continue

                # determine exit direction
                exit_type = "SELL" if qty > 0 else "BUY"
                abs_qty = abs(qty)

                r = self._dhan.place_order(
                    security_id=str(pos.get("securityId")),
                    exchange_segment=pos.get("exchangeSegment", "NSE_FNO"),
                    transaction_type=exit_type,
                    quantity=abs_qty,
                    order_type="MARKET",
                    product_type=pos.get("productType", "INTRADAY"),
                )
                results.append({
                    "security_id": pos.get("securityId"),
                    "exit_qty": abs_qty,
                    "direction": exit_type,
                    "result": r,
                })
        return results

    # ── Helpers ───────────────────────────────────────

    def _resolve_segment(self, segment: str) -> str:
        """Map friendly names to Dhan segment constants."""
        mapping = {
            "NSE_FNO": dhanhq.FNO,
            "NSE_EQ": dhanhq.NSE,
            "BSE_EQ": dhanhq.BSE,
            "NSE": dhanhq.NSE,
            "IDX_I": dhanhq.INDEX,
            "INDEX": dhanhq.INDEX,
        }
        return mapping.get(segment.upper(), segment)

    def _get_underlying_id(self, symbol: str) -> str:
        """
        Return the Dhan underlying security ID for index options.
        These are fixed IDs assigned by Dhan.
        """
        ids = {
            "NIFTY": "13",
            "BANKNIFTY": "25",
        }
        return ids.get(symbol.upper(), symbol)
