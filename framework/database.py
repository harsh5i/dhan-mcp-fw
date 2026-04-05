"""
SQLite database for strategy framework.
One DB per strategy. Stores candles, indicator values, trades, and state.
"""

import sqlite3
import json
import os
from datetime import datetime
from pathlib import Path


DB_DIR = os.path.expanduser("~/.dhan-mcp/strategies")


def get_db_path(strategy_id: str) -> str:
    Path(DB_DIR).mkdir(parents=True, exist_ok=True)
    return os.path.join(DB_DIR, f"{strategy_id}.db")


def init_db(strategy_id: str) -> str:
    """Create tables for a new strategy. Returns DB path."""
    path = get_db_path(strategy_id)
    conn = sqlite3.connect(path)
    c = conn.cursor()

    # OHLCV candles (intraday and daily)
    c.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            security_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            interval TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            UNIQUE(security_id, timestamp, interval)
        )
    """)

    # Computed indicator values
    c.execute("""
        CREATE TABLE IF NOT EXISTS indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            security_id TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            interval TEXT NOT NULL,
            name TEXT NOT NULL,
            value REAL,
            UNIQUE(security_id, timestamp, interval, name)
        )
    """)

    # Trades taken by the strategy
    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            action TEXT NOT NULL,
            symbol TEXT NOT NULL,
            security_id TEXT,
            strike REAL,
            option_type TEXT,
            expiry TEXT,
            quantity INTEGER,
            price REAL,
            order_type TEXT,
            trigger_price REAL,
            order_id TEXT,
            status TEXT DEFAULT 'PENDING',
            pnl REAL,
            algo_reason TEXT,
            commentary TEXT,
            mode TEXT DEFAULT 'paper'
        )
    """)

    # Strategy state (current position, SL, target, etc.)
    c.execute("""
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        )
    """)

    # Performance snapshots
    c.execute("""
        CREATE TABLE IF NOT EXISTS performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            total_trades INTEGER,
            wins INTEGER,
            losses INTEGER,
            total_pnl REAL,
            max_drawdown REAL,
            win_rate REAL,
            avg_win REAL,
            avg_loss REAL,
            sharpe_ratio REAL,
            data TEXT
        )
    """)

    # Strategy profile — version history, changes, performance snapshots
    c.execute("""
        CREATE TABLE IF NOT EXISTS profile (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            event TEXT NOT NULL,
            version INTEGER,
            summary TEXT,
            details TEXT,
            performance_snapshot TEXT
        )
    """)

    conn.commit()
    conn.close()
    return path


class StrategyDB:
    """Interface to a strategy's SQLite database."""

    def __init__(self, strategy_id: str):
        self.strategy_id = strategy_id
        self.path = get_db_path(strategy_id)

    def _conn(self):
        return sqlite3.connect(self.path)

    # ── Candles ──────────────────────────────────────

    def save_candles(self, security_id: str, symbol: str, interval: str,
                     timestamps: list, opens: list, highs: list, lows: list,
                     closes: list, volumes: list):
        """Bulk insert/replace candles."""
        conn = self._conn()
        c = conn.cursor()
        rows = []
        for i in range(len(timestamps)):
            rows.append((
                security_id, symbol, int(timestamps[i]), interval,
                opens[i], highs[i], lows[i], closes[i], volumes[i],
            ))
        c.executemany("""
            INSERT OR REPLACE INTO candles
            (security_id, symbol, timestamp, interval, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        conn.close()

    def get_candles(self, security_id: str, interval: str, limit: int = 100) -> list:
        """Get recent candles for an instrument."""
        conn = self._conn()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("""
            SELECT * FROM candles
            WHERE security_id = ? AND interval = ?
            ORDER BY timestamp DESC LIMIT ?
        """, (security_id, interval, limit))
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        return list(reversed(rows))

    # ── Indicators ───────────────────────────────────

    def save_indicators(self, security_id: str, interval: str,
                        timestamps: list, name: str, values: list):
        """Bulk insert/replace indicator values."""
        conn = self._conn()
        c = conn.cursor()
        rows = []
        for i in range(len(timestamps)):
            if values[i] is not None:
                rows.append((security_id, int(timestamps[i]), interval, name, values[i]))
        c.executemany("""
            INSERT OR REPLACE INTO indicators
            (security_id, timestamp, interval, name, value)
            VALUES (?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        conn.close()

    def get_latest_indicators(self, security_id: str, interval: str) -> dict:
        """Get latest value for each indicator."""
        conn = self._conn()
        c = conn.cursor()
        c.execute("""
            SELECT name, value, MAX(timestamp) as ts FROM indicators
            WHERE security_id = ? AND interval = ?
            GROUP BY name
        """, (security_id, interval))
        result = {row[0]: row[1] for row in c.fetchall()}
        conn.close()
        return result

    # ── Trades ───────────────────────────────────────

    def record_trade(self, action: str, symbol: str, security_id: str,
                     strike: float, option_type: str, expiry: str,
                     quantity: int, price: float, order_type: str,
                     algo_reason: str, mode: str = "paper",
                     order_id: str = None, trigger_price: float = None) -> int:
        """Record a trade. Returns trade ID."""
        conn = self._conn()
        c = conn.cursor()
        c.execute("""
            INSERT INTO trades
            (timestamp, action, symbol, security_id, strike, option_type, expiry,
             quantity, price, order_type, trigger_price, order_id, status, algo_reason, mode)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'EXECUTED', ?, ?)
        """, (
            datetime.now().isoformat(), action, symbol, security_id,
            strike, option_type, expiry, quantity, price, order_type,
            trigger_price, order_id, algo_reason, mode,
        ))
        trade_id = c.lastrowid
        conn.commit()
        conn.close()
        return trade_id

    def update_trade_commentary(self, trade_id: int, commentary: str):
        """Add Gemma4 commentary to a trade."""
        conn = self._conn()
        c = conn.cursor()
        c.execute("UPDATE trades SET commentary = ? WHERE id = ?", (commentary, trade_id))
        conn.commit()
        conn.close()

    def update_trade_pnl(self, trade_id: int, pnl: float, status: str = "CLOSED"):
        """Update trade P&L when position is exited."""
        conn = self._conn()
        c = conn.cursor()
        c.execute("UPDATE trades SET pnl = ?, status = ? WHERE id = ?", (pnl, status, trade_id))
        conn.commit()
        conn.close()

    def get_trades(self, limit: int = 50) -> list:
        """Get recent trades."""
        conn = self._conn()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        return list(reversed(rows))

    def get_open_trades(self) -> list:
        """Get trades that haven't been closed yet."""
        conn = self._conn()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM trades WHERE status = 'EXECUTED' ORDER BY id DESC")
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        return rows

    # ── State ────────────────────────────────────────

    def set_state(self, key: str, value):
        """Set a state key-value pair."""
        conn = self._conn()
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO state (key, value, updated_at)
            VALUES (?, ?, ?)
        """, (key, json.dumps(value), datetime.now().isoformat()))
        conn.commit()
        conn.close()

    def get_state(self, key: str, default=None):
        """Get a state value."""
        conn = self._conn()
        c = conn.cursor()
        c.execute("SELECT value FROM state WHERE key = ?", (key,))
        row = c.fetchone()
        conn.close()
        if row:
            return json.loads(row[0])
        return default

    def get_all_state(self) -> dict:
        """Get all state as a dict."""
        conn = self._conn()
        c = conn.cursor()
        c.execute("SELECT key, value FROM state")
        result = {row[0]: json.loads(row[1]) for row in c.fetchall()}
        conn.close()
        return result

    # ── Profile ──────────────────────────────────────

    def log_profile_event(self, event: str, summary: str, details: dict = None,
                          version: int = None):
        """Log a profile event (creation, improvement, config change, performance snapshot)."""
        perf = self.compute_performance()
        conn = self._conn()
        c = conn.cursor()
        c.execute("""
            INSERT INTO profile (timestamp, event, version, summary, details, performance_snapshot)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().isoformat(), event, version, summary,
            json.dumps(details) if details else None,
            json.dumps(perf),
        ))
        conn.commit()
        conn.close()

    def get_profile(self, limit: int = 50) -> list:
        """Get strategy profile history."""
        conn = self._conn()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM profile ORDER BY id DESC LIMIT ?", (limit,))
        rows = []
        for r in c.fetchall():
            row = dict(r)
            if row.get("details"):
                row["details"] = json.loads(row["details"])
            if row.get("performance_snapshot"):
                row["performance_snapshot"] = json.loads(row["performance_snapshot"])
            rows.append(row)
        conn.close()
        return list(reversed(rows))

    # ── Performance ──────────────────────────────────

    def compute_performance(self) -> dict:
        """Compute performance stats from trades."""
        conn = self._conn()
        c = conn.cursor()
        c.execute("SELECT * FROM trades WHERE pnl IS NOT NULL")
        rows = c.fetchall()
        conn.close()

        if not rows:
            return {"total_trades": 0, "message": "No closed trades yet"}

        pnls = [r[15] for r in rows]  # pnl column
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        total_pnl = sum(pnls)
        cumulative = []
        running = 0
        peak = 0
        max_dd = 0
        for p in pnls:
            running += p
            cumulative.append(running)
            if running > peak:
                peak = running
            dd = peak - running
            if dd > max_dd:
                max_dd = dd

        return {
            "total_trades": len(pnls),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / len(pnls) * 100, 1) if pnls else 0,
            "total_pnl": round(total_pnl, 2),
            "avg_win": round(sum(wins) / len(wins), 2) if wins else 0,
            "avg_loss": round(sum(losses) / len(losses), 2) if losses else 0,
            "max_drawdown": round(max_dd, 2),
            "best_trade": round(max(pnls), 2) if pnls else 0,
            "worst_trade": round(min(pnls), 2) if pnls else 0,
        }
