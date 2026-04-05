"""
Data manager — fetches OHLC via DhanClient, stores in SQLite, computes indicators.
"""

import pandas as pd
import ta
from datetime import datetime, timedelta

from framework.database import StrategyDB


# Supported indicator types and their computation
INDICATOR_REGISTRY = {
    "EMA": lambda df, period, **kw: ta.trend.ema_indicator(df["close"], window=period),
    "SMA": lambda df, period, **kw: ta.trend.sma_indicator(df["close"], window=period),
    "RSI": lambda df, period, **kw: ta.momentum.rsi(df["close"], window=period),
    "MACD": lambda df, period, **kw: ta.trend.macd(df["close"]),
    "MACD_SIGNAL": lambda df, period, **kw: ta.trend.macd_signal(df["close"]),
    "MACD_DIFF": lambda df, period, **kw: ta.trend.macd_diff(df["close"]),
    "BOLLINGER_HIGH": lambda df, period, **kw: ta.volatility.bollinger_hband(df["close"], window=period),
    "BOLLINGER_LOW": lambda df, period, **kw: ta.volatility.bollinger_lband(df["close"], window=period),
    "BOLLINGER_MID": lambda df, period, **kw: ta.volatility.bollinger_mavg(df["close"], window=period),
    "ATR": lambda df, period, **kw: ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=period),
    "VWAP": lambda df, **kw: _compute_vwap(df),
    "SUPERTREND": lambda df, period, **kw: _compute_supertrend(df, period),
    "ADX": lambda df, period, **kw: ta.trend.adx(df["high"], df["low"], df["close"], window=period),
    "STOCH_K": lambda df, period, **kw: ta.momentum.stoch(df["high"], df["low"], df["close"], window=period),
    "STOCH_D": lambda df, period, **kw: ta.momentum.stoch_signal(df["high"], df["low"], df["close"], window=period),
    "OBV": lambda df, **kw: ta.volume.on_balance_volume(df["close"], df["volume"]),
    # Custom indicators for DPO-Volume-Vortex strategy
    "DPO": lambda df, period, **kw: _compute_dpo(df, period),
    "DPO_SIGNAL": lambda df, period, **kw: _compute_dpo_signal(df, period),
    "OBV_HULL": lambda df, period, **kw: _compute_obv_hull_ratio(df, period),
    "OBV_HULL_SIGNAL": lambda df, period, **kw: _compute_obv_hull_signal(df, period),
    "VORTEX_POS": lambda df, period, **kw: _compute_vortex(df, period, positive=True),
    "VORTEX_NEG": lambda df, period, **kw: _compute_vortex(df, period, positive=False),
    "VORTEX_SIGNAL": lambda df, period, **kw: _compute_vortex_signal(df, period),
    "HULL_MA": lambda df, period, **kw: _compute_hull_ma(df["close"], period),
    "CONFIDENCE": lambda df, period, **kw: _compute_confidence(df, period),
}

# Derived indicator types — computed from other indicators (second pass)
DERIVED_INDICATOR_TYPES = {"LAG", "CHANGE", "SLOPE"}


def _compute_derived(source_series: pd.Series, ind_type: str, period: int) -> pd.Series:
    """
    Compute a derived indicator from an already-computed indicator series.

    LAG: value N bars ago. Usage: compare indicator vs its lagged self.
    CHANGE: current - previous (1 bar delta). Positive = rising.
    SLOPE: (current - N bars ago) / N. Rate of change over period.
    """
    if ind_type == "LAG":
        return source_series.shift(period)
    elif ind_type == "CHANGE":
        return source_series - source_series.shift(1)
    elif ind_type == "SLOPE":
        return (source_series - source_series.shift(period)) / period
    else:
        raise ValueError(f"Unknown derived type: {ind_type}")


def _compute_vwap(df: pd.DataFrame) -> pd.Series:
    """Compute VWAP from OHLCV data."""
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    cumulative_tp_vol = (typical_price * df["volume"]).cumsum()
    cumulative_vol = df["volume"].cumsum()
    return cumulative_tp_vol / cumulative_vol


def _compute_supertrend(df: pd.DataFrame, period: int = 10, multiplier: float = 3.0) -> pd.Series:
    """Basic Supertrend indicator."""
    atr = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=period)
    hl2 = (df["high"] + df["low"]) / 2
    upper = hl2 + multiplier * atr
    lower = hl2 - multiplier * atr

    supertrend = pd.Series(index=df.index, dtype=float)
    direction = pd.Series(index=df.index, dtype=int)
    supertrend.iloc[0] = upper.iloc[0]
    direction.iloc[0] = -1

    for i in range(1, len(df)):
        if df["close"].iloc[i] > upper.iloc[i - 1]:
            direction.iloc[i] = 1
        elif df["close"].iloc[i] < lower.iloc[i - 1]:
            direction.iloc[i] = -1
        else:
            direction.iloc[i] = direction.iloc[i - 1]

        if direction.iloc[i] == 1:
            supertrend.iloc[i] = lower.iloc[i]
        else:
            supertrend.iloc[i] = upper.iloc[i]

    return supertrend


def _compute_dpo(df: pd.DataFrame, period: int = 15) -> pd.Series:
    """Detrended Price Oscillator. Matches TradingView's DPO."""
    barsback = int(period / 2 + 1)
    ma = df["close"].rolling(window=period).mean()
    return df["close"] - ma.shift(barsback)


def _compute_dpo_signal(df: pd.DataFrame, period: int = 15) -> pd.Series:
    """DPO crossover signal. +1 = crosses positive, -1 = crosses negative, 0 = no change."""
    dpo = _compute_dpo(df, period)
    signal = pd.Series(0.0, index=df.index)
    for i in range(1, len(dpo)):
        if pd.notna(dpo.iloc[i]) and pd.notna(dpo.iloc[i - 1]):
            if dpo.iloc[i] > 0 and dpo.iloc[i - 1] <= 0:
                signal.iloc[i] = 1.0  # Crossed positive
            elif dpo.iloc[i] < 0 and dpo.iloc[i - 1] >= 0:
                signal.iloc[i] = -1.0  # Crossed negative
    return signal


def _compute_hull_ma(series: pd.Series, period: int = 17) -> pd.Series:
    """Hull Moving Average — faster, less lag than EMA."""
    import numpy as np
    half = int(period / 2)
    sqrt_period = int(np.floor(np.sqrt(period)))
    wma_half = series.rolling(window=half).apply(
        lambda x: np.average(x, weights=range(1, half + 1)), raw=True
    )
    wma_full = series.rolling(window=period).apply(
        lambda x: np.average(x, weights=range(1, period + 1)), raw=True
    )
    diff = 2 * wma_half - wma_full
    hull = diff.rolling(window=sqrt_period).apply(
        lambda x: np.average(x, weights=range(1, sqrt_period + 1)), raw=True
    )
    return hull


def _compute_obv_hull_ratio(df: pd.DataFrame, period: int = 17) -> pd.Series:
    """OBV Hull MA / OBV SMA ratio. >1 = bullish volume, <1 = bearish volume."""
    obv = ta.volume.on_balance_volume(df["close"], df["volume"])
    hull = _compute_hull_ma(obv, period)
    sma_len = 26  # Fixed SMA length as in original Pine script
    sma = obv.rolling(window=sma_len).mean()
    # Ratio: hull/sma, normalized. Avoid division by zero.
    ratio = pd.Series(index=df.index, dtype=float)
    for i in range(len(df)):
        if pd.notna(hull.iloc[i]) and pd.notna(sma.iloc[i]) and sma.iloc[i] != 0:
            ratio.iloc[i] = hull.iloc[i] / sma.iloc[i]
    return ratio


def _compute_obv_hull_signal(df: pd.DataFrame, period: int = 17) -> pd.Series:
    """OBV Hull ratio crossover signal. +1 = crosses above 1, -1 = crosses below 1."""
    ratio = _compute_obv_hull_ratio(df, period)
    signal = pd.Series(0.0, index=df.index)
    for i in range(1, len(ratio)):
        if pd.notna(ratio.iloc[i]) and pd.notna(ratio.iloc[i - 1]):
            if ratio.iloc[i] > 1 and ratio.iloc[i - 1] <= 1:
                signal.iloc[i] = 1.0
            elif ratio.iloc[i] < 1 and ratio.iloc[i - 1] >= 1:
                signal.iloc[i] = -1.0
    return signal


def _compute_vortex(df: pd.DataFrame, period: int = 14, positive: bool = True) -> pd.Series:
    """Vortex Indicator — VI+ or VI-. Matches TradingView."""
    high = df["high"]
    low = df["low"]
    close = df["close"]

    # True Range components
    vm_plus = (high - low.shift(1)).abs()
    vm_minus = (low - high.shift(1)).abs()

    # ATR(1) = max(high-low, abs(high-prev_close), abs(low-prev_close))
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs(),
    ], axis=1).max(axis=1)

    vm_plus_sum = vm_plus.rolling(window=period).sum()
    vm_minus_sum = vm_minus.rolling(window=period).sum()
    tr_sum = tr.rolling(window=period).sum()

    if positive:
        return vm_plus_sum / tr_sum
    else:
        return vm_minus_sum / tr_sum


def _compute_vortex_signal(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Vortex crossover signal. +1 = VI+ crosses above VI-, -1 = VI- crosses above VI+."""
    vi_plus = _compute_vortex(df, period, positive=True)
    vi_minus = _compute_vortex(df, period, positive=False)

    signal = pd.Series(0.0, index=df.index)
    for i in range(1, len(df)):
        prev_diff = vi_plus.iloc[i - 1] - vi_minus.iloc[i - 1] if pd.notna(vi_plus.iloc[i - 1]) else None
        curr_diff = vi_plus.iloc[i] - vi_minus.iloc[i] if pd.notna(vi_plus.iloc[i]) else None

        if prev_diff is not None and curr_diff is not None:
            if curr_diff > 0 and prev_diff <= 0:
                signal.iloc[i] = 1.0  # VI+ crossed above VI-
            elif curr_diff < 0 and prev_diff >= 0:
                signal.iloc[i] = -1.0  # VI- crossed above VI+
    return signal


def _compute_confidence(df: pd.DataFrame, period: int = 15) -> pd.Series:
    """
    Confidence score: counts how many of the 3 indicators are bullish.
    +3 = all bullish, -3 = all bearish, 0 = mixed.
    Uses current state (not crossovers) — DPO>0, OBV_Hull_ratio>1, VI+>VI-.
    """
    dpo = _compute_dpo(df, period)
    obv_ratio = _compute_obv_hull_ratio(df, 17)
    vi_plus = _compute_vortex(df, 14, positive=True)
    vi_minus = _compute_vortex(df, 14, positive=False)

    confidence = pd.Series(0.0, index=df.index)
    for i in range(len(df)):
        score = 0
        if pd.notna(dpo.iloc[i]):
            score += 1 if dpo.iloc[i] > 0 else -1
        if pd.notna(obv_ratio.iloc[i]):
            score += 1 if obv_ratio.iloc[i] > 1 else -1
        if pd.notna(vi_plus.iloc[i]) and pd.notna(vi_minus.iloc[i]):
            score += 1 if vi_plus.iloc[i] > vi_minus.iloc[i] else -1
        confidence.iloc[i] = score
    return confidence


class DataManager:
    """Fetches data via DhanClient, stores in DB, computes indicators."""

    def __init__(self, strategy: dict, db: StrategyDB, dhan_client):
        self.strategy = strategy
        self.db = db
        self.client = dhan_client
        self.index = strategy["instrument"]["index"]
        self.interval = strategy.get("interval", 5)
        self.lookback = strategy.get("data_lookback", 30)

        # Underlying security ID
        self._spot_id = "13" if self.index == "NIFTY" else "25"

    def fetch_and_store_daily(self):
        """Fetch daily OHLCV for the index and store in DB."""
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=self.lookback)).strftime("%Y-%m-%d")

        result = self.client.get_historical_daily(
            self._spot_id, "INDEX", "INDEX", from_date, to_date
        )
        data = result.get("data", {})
        if not data.get("open"):
            return {"status": "ERROR", "message": "No daily data returned"}

        self.db.save_candles(
            security_id=self._spot_id,
            symbol=self.index,
            interval="1d",
            timestamps=data["timestamp"],
            opens=data["open"],
            highs=data["high"],
            lows=data["low"],
            closes=data["close"],
            volumes=data["volume"],
        )
        return {"status": "OK", "candles": len(data["open"])}

    def fetch_and_store_intraday(self):
        """Fetch intraday candles for today and store in DB."""
        today = datetime.now().strftime("%Y-%m-%d")

        result = self.client.get_intraday_minute(
            self._spot_id, "INDEX", "INDEX", today, today, self.interval
        )
        data = result.get("data", {})
        if not data.get("open"):
            return {"status": "ERROR", "message": "No intraday data returned"}

        self.db.save_candles(
            security_id=self._spot_id,
            symbol=self.index,
            interval=f"{self.interval}m",
            timestamps=data["timestamp"],
            opens=data["open"],
            highs=data["high"],
            lows=data["low"],
            closes=data["close"],
            volumes=data["volume"],
        )
        return {"status": "OK", "candles": len(data["open"])}

    def compute_indicators(self, interval: str = None) -> dict:
        """Compute all strategy indicators from stored candles. Two-pass: base then derived."""
        if interval is None:
            interval = f"{self.interval}m"

        candles = self.db.get_candles(self._spot_id, interval, limit=500)
        if not candles:
            return {"status": "ERROR", "message": "No candles in DB"}

        df = pd.DataFrame(candles)
        timestamps = df["timestamp"].tolist()
        results = {}
        computed_series = {}  # name → pd.Series, for derived indicators

        # Pass 1: base indicators
        base_configs = []
        derived_configs = []
        for ind_config in self.strategy.get("indicators", []):
            if ind_config["type"].upper() in DERIVED_INDICATOR_TYPES:
                derived_configs.append(ind_config)
            else:
                base_configs.append(ind_config)

        for ind_config in base_configs:
            name = ind_config["name"]
            ind_type = ind_config["type"].upper()
            period = ind_config.get("period", 14)

            compute_fn = INDICATOR_REGISTRY.get(ind_type)
            if not compute_fn:
                results[name] = {"status": "UNSUPPORTED", "type": ind_type}
                continue

            try:
                values = compute_fn(df, period=period)
                computed_series[name] = values
                results[name] = self._save_and_get_latest(
                    name, values, timestamps, interval
                )
            except Exception as e:
                results[name] = {"status": "ERROR", "error": str(e)}

        # Pass 2: derived indicators (reference other computed indicators)
        for ind_config in derived_configs:
            name = ind_config["name"]
            ind_type = ind_config["type"].upper()
            period = ind_config.get("period", 1)
            source = ind_config.get("source")

            if not source or source not in computed_series:
                results[name] = {"status": "ERROR", "error": f"Source '{source}' not found"}
                continue

            try:
                values = _compute_derived(computed_series[source], ind_type, period)
                computed_series[name] = values
                results[name] = self._save_and_get_latest(
                    name, values, timestamps, interval
                )
            except Exception as e:
                results[name] = {"status": "ERROR", "error": str(e)}

        return results

    def _save_and_get_latest(self, name, values, timestamps, interval):
        """Save indicator to DB and return latest non-null value."""
        values_list = values.tolist()
        self.db.save_indicators(
            security_id=self._spot_id,
            interval=interval,
            timestamps=timestamps,
            name=name,
            values=values_list,
        )
        for v in reversed(values_list):
            if v is not None and pd.notna(v):
                return round(v, 4)
        return None

    def get_current_snapshot(self) -> dict:
        """Get latest candle + all indicator values. This is what the engine evaluates."""
        interval = f"{self.interval}m"
        candles = self.db.get_candles(self._spot_id, interval, limit=5)
        indicators = self.db.get_latest_indicators(self._spot_id, interval)

        latest_candle = candles[-1] if candles else {}

        return {
            "timestamp": latest_candle.get("timestamp"),
            "open": latest_candle.get("open"),
            "high": latest_candle.get("high"),
            "low": latest_candle.get("low"),
            "close": latest_candle.get("close"),
            "volume": latest_candle.get("volume"),
            "indicators": indicators,
        }
