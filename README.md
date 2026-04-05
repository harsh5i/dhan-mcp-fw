# DhanMCP-FW — NSE Stock Analysis & Options Trading via MCP

> **Disclaimer:** This is an experimental project built for personal use and learning. It is **not** financial advice, investment guidance, or a trading recommendation of any kind. Any real trades or trading decisions made using this tool are entirely at your own risk. The authors accept no responsibility for financial losses incurred through the use of this software. This project is **not affiliated with, endorsed by, or associated with Dhan, DhanHQ, or any other financial institution or organisation.** Always do your own research and consult a qualified financial advisor before trading. Feedback and contributions are welcome!

An MCP (Model Context Protocol) server for NSE stock analysis and NIFTY/BANKNIFTY options trading via [Dhan](https://dhan.co) broker, with a built-in **strategy framework** for creating, backtesting, and running automated trading strategies. Built from scratch. Any MCP-compatible AI client — Claude Code, OpenClaw, Codex CLI, Gemini CLI, Cursor, VS Code Copilot — can connect, analyse, and trade.

> **39 tools** covering stock analysis, market data, historical charts, technical indicators, portfolio, order management, options execution, and a full strategy framework — all safety-gated.

## Architecture

```
Any AI Client (Claude Code, OpenClaw/Olive, Codex, Gemini CLI)
    ↕ MCP Protocol (stdio)
dhan-nifty-mcp (this server) — 39 tools (27 trading + 12 framework)
    ├── Safety Layer (validates every write order)
    ├── Audit Logger (JSONL, every tool call)
    └── Dhan Client (thin SDK wrapper)
        ↕ HTTP/REST
    Dhan API (api.dhan.co/v2)
```

## Files

| File | Purpose |
|------|---------|
| `server.py` | FastMCP server, all 39 tool definitions, entry point |
| `safety.py` | 6-step validation chain: instrument whitelist, market hours, lot limit, position limit, value cap, price sanity |
| `dhan_client.py` | Thin wrapper around dhanhq SDK. All Dhan API calls live here. Stock alias map for common names (RIL→RELIANCE, SBI→SBIN, etc.) |
| `models.py` | Dataclasses: OrderRequest, SafetyResult, DryRunResponse, LiveResponse, ErrorResponse. Enums for OrderAction, OrderType (MARKET/LIMIT/SL/SLM), OptionType. Lot size lookup. |
| `logger.py` | AuditLogger class. Writes every tool call as JSONL to ~/.dhan-mcp/logs/trades.jsonl |
| `config.yaml` | Credentials, safety rules (mode, limits, whitelist, market hours), logging paths |
| `ollama_bridge.py` | Standalone bridge for connecting Ollama models to this MCP server |
| `requirements.txt` | dhanhq, mcp, pyyaml |

## Tools (27)

### Server & Auth (3)
| Tool | Purpose |
|------|---------|
| `server_status` | Check token validity, server mode, safety config. **Call this first in every session.** |
| `update_token` | Hot-swap expired Dhan access token without restarting. Saves to config.yaml. |
| `market_status` | Is market open/closed/holiday/pre-market? Time to open/close. Knows NSE holidays 2026. |

### Market Data — Quick Price (3, single-call, no chaining needed)
| Tool | Purpose |
|------|---------|
| `get_stock_price(name)` | Get any stock's live price by name/ticker. Handles aliases (RIL, SBI, HDFC, TATA, etc). Returns LTP, OHLC, 52wk range, volume. |
| `get_option_price(symbol, strike, expiry, option_type)` | Get any NIFTY/BANKNIFTY option price in one call. Returns LTP, bid/ask, IV, Greeks, OI, volume, spot price, security_id. |
| `get_bulk_prices(instruments)` | Multiple instruments in one API call. Format: `"INDEX:13,NSE_EQ:1333,NSE_FNO:40752"` |

### Market Data — Raw (3)
| Tool | Purpose |
|------|---------|
| `get_ltp(security_id, exchange_segment)` | Last traded price for any instrument by security_id. |
| `get_option_chain(symbol, expiry)` | Full option chain — all strikes, premiums, OI, Greeks, IV. |
| `get_market_depth(security_id, exchange_segment)` | 5-level bid/ask depth. |

### Historical Data (2)
| Tool | Purpose |
|------|---------|
| `get_historical_daily(security_id, from_date, to_date, ...)` | Daily OHLCV candles, any date range. |
| `get_intraday_candles(security_id, from_date, to_date, interval, ...)` | Minute candles (1/5/15/25/60 min). Last 5 trading days only. |

### Portfolio (4)
| Tool | Purpose |
|------|---------|
| `get_positions` | Open intraday/F&O positions with live P&L. |
| `get_holdings` | Long-term portfolio (stocks, ETFs, MFs). |
| `get_margins` | Available/used margin and fund limits. |
| `get_pnl_summary` | Today's total P&L — realized + unrealized, per-position breakdown. |

### Lookup (3)
| Tool | Purpose |
|------|---------|
| `search_stock(query)` | Find any NSE stock by name/ticker. Has alias map for 30+ popular shorthands. |
| `lookup_security_id(symbol, expiry, strike, option_type)` | Find Dhan security_id for a specific option contract. |
| `get_expiry_list(symbol)` | Get all valid expiry dates for NIFTY/BANKNIFTY. |

### Order Management (6)
| Tool | Purpose |
|------|---------|
| `get_order_book` | All orders placed today + status (PENDING/EXECUTED/CANCELLED/REJECTED). |
| `get_order_status(order_id)` | Detailed status of a specific order. |
| `get_trade_book` | All executed trades today with fill prices. |
| `get_trade_history(from_date, to_date)` | Historical trades over any date range. |
| `modify_order(order_id, order_type, quantity, price, ...)` | Change pending order's price, qty, or type. |
| `calculate_margin(security_id, transaction_type, quantity, price, ...)` | Check margin required before placing a trade. |

### Execution (3, safety-gated)
| Tool | Purpose |
|------|---------|
| `place_order(symbol, strike, expiry, option_type, lots, ...)` | Place options order. Supports MARKET, LIMIT, SL (stop-loss limit), SLM (stop-loss market). Goes through 6 safety checks. |
| `cancel_order(order_id)` | Cancel a pending order. |
| `exit_all(confirmation_phrase)` | **EMERGENCY KILL SWITCH.** Cancels all orders + market-exits all positions. Requires exact phrase `CONFIRM_EXIT_ALL`. |

## Common Workflows

### 1. Start of session (always do this first)
```
server_status → check token valid + mode
market_status → check if market is open
```

### 2. Get NIFTY option price (one call)
```
get_option_price(symbol="NIFTY", strike=22800, expiry="2026-04-07", option_type="CE")
→ LTP, bid/ask, IV, Greeks, OI, security_id — everything in one response
```

### 3. Get any stock price (one call)
```
get_stock_price(name="RIL")
→ RELIANCE LTP, OHLC, volume, 52wk range
```
Aliases work: RIL, SBI, HDFC, TATA, ICICI, KOTAK, AIRTEL, ADANI, etc.

### 4. Don't know the expiry date?
```
get_expiry_list(symbol="NIFTY")
→ list of all valid expiries, nearest weekly highlighted
```

### 5. Check NIFTY/BANKNIFTY spot price
```
get_ltp(security_id="13", exchange_segment="INDEX")    # NIFTY
get_ltp(security_id="25", exchange_segment="INDEX")    # BANKNIFTY
```

### 6. Historical analysis
```
# Daily candles — any date range
get_historical_daily(security_id="13", from_date="2026-01-01", to_date="2026-04-04", exchange_segment="INDEX", instrument_type="INDEX")

# Intraday 5-min candles — last 5 days only
get_intraday_candles(security_id="13", from_date="2026-04-02", to_date="2026-04-02", interval=5, exchange_segment="INDEX", instrument_type="INDEX")

# For stocks: use search_stock first to get security_id, then:
get_historical_daily(security_id="1333", ..., exchange_segment="NSE_EQ", instrument_type="EQUITY")

# For options: use get_option_price or lookup_security_id first, then:
get_historical_daily(security_id="40752", ..., exchange_segment="NSE_FNO", instrument_type="OPTIDX")
```

### 7. Place a trade (full flow)
```
market_status                         → confirm market is open
get_option_price(...)                 → get price + security_id
calculate_margin(security_id, ...)    → check if enough funds
place_order(symbol, strike, expiry, option_type, lots, security_id=...)  → execute
get_order_status(order_id)            → confirm fill
```

### 8. Place a stop-loss order
```
place_order(
    symbol="NIFTY", strike=22800, expiry="2026-04-07", option_type="CE",
    lots=1, action="SELL", order_type="SLM", trigger_price=180,
    security_id="40752"
)
→ Sells when price drops to 180 (stop-loss market)
```

### 9. Monitor during the day
```
get_pnl_summary     → total P&L
get_positions        → open positions detail
get_order_book       → pending orders
get_trade_book       → what filled today
```

### 10. Watch multiple instruments at once
```
get_bulk_prices(instruments="INDEX:13,INDEX:25,NSE_EQ:1333,NSE_FNO:40752")
→ NIFTY spot, BANKNIFTY spot, HDFC Bank, and a specific option — all in one call
```

### 11. Token expired mid-session
```
server_status           → sees TOKEN_EXPIRED
→ AI asks user to generate new token from Dhan dashboard
update_token("eyJ...")  → hot-swaps token, saves to config.yaml, no restart needed
server_status           → confirms new token is valid
```

### 12. Emergency exit
```
exit_all(confirmation_phrase="CONFIRM_EXIT_ALL")
→ Cancels ALL pending orders + market-exits ALL positions
```

## Instrument ID Reference

| Instrument | Security ID | Segment |
|-----------|-------------|---------|
| NIFTY 50 (spot) | 13 | INDEX |
| BANKNIFTY (spot) | 25 | INDEX |
| NIFTY/BANKNIFTY options | Use `get_option_price` or `lookup_security_id` | NSE_FNO |
| Any stock | Use `get_stock_price` or `search_stock` | NSE_EQ |

## Safety Rules (config.yaml)

- **mode**: `dry-run` (default) or `live` — dry-run logs what would happen, never hits Dhan
- **max_lots_per_order**: configurable (default 2)
- **max_open_positions**: configurable (default 5)
- **max_order_value**: INR cap per order (default 50000)
- **allowed_instruments**: NIFTY, BANKNIFTY only
- **market_hours**: 09:15-15:30 IST
- **price_deviation_pct**: 20% max from LTP for limit orders
- **kill_phrase**: `CONFIRM_EXIT_ALL`

## Key Design Decisions

1. **Single MCP server** (not per-tool) for shared state and speed
2. **Safety layer is non-bypassable**: every write tool goes through safety.py
3. **Dry-run first**: server starts safe, must explicitly switch to live
4. **Kill switch requires passphrase**: prevents accidental LLM triggers
5. **Audit everything**: every tool call logged with params, result, latency, errors
6. **One-call tools**: `get_stock_price`, `get_option_price` — no chaining needed for common tasks
7. **Stock aliases**: RIL→RELIANCE, SBI→SBIN, etc. — natural language works
8. **Hot-swap auth**: `update_token` replaces expired tokens without restart
9. **Market awareness**: `market_status` knows holidays, weekends, pre/post market
10. **Lot sizes**: NIFTY=75, BANKNIFTY=30
11. **Multi-strategy**: concurrent execution with independent state per strategy
12. **Auto-recovery**: running strategies survive server restarts via persistent state file
13. **Honest backtesting**: options backtests clearly disclose spot-based P&L limitation

## Dhan Specifics

- Underlying IDs: NIFTY=`13`, BANKNIFTY=`25`
- Exchange segments: `IDX_I` (index), `NSE_FNO` (options), `NSE_EQ` (equities)
- SDK: dhanhq Python package
- Access tokens: 24-hour validity, generated from Dhan dashboard, no auto-refresh
- Option chain: keyed by strike price (e.g. `"22800.000000"`), nested `ce`/`pe` objects
- Security master CSV: `https://images.dhan.co/api-data/api-scrip-master.csv`

## Setup

### Prerequisites
- Python 3.10+
- [Dhan](https://dhan.co) trading account with API access
- An MCP-compatible AI client

### Installation

```bash
git clone https://github.com/youruser/dhan-nifty-mcp.git
cd dhan-nifty-mcp
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### Configuration

Copy and edit the config:
```bash
cp config.yaml.example config.yaml
# Edit config.yaml with your Dhan client_id and access_token
```

Generate an access token from [Dhan API Dashboard](https://knowledge.dhan.co). Tokens are valid for 24 hours.

### Register with your AI client

```bash
# Claude Code
claude mcp add dhan-nifty -- python3 /path/to/dhan-nifty-mcp/server.py

# OpenClaw
mcporter config add dhan-nifty --stdio "python3 /path/to/dhan-nifty-mcp/server.py"

# Or run standalone
python3 server.py
```

### Ollama Bridge (optional)

Connect local Ollama models to the MCP server:
```bash
.venv/bin/python3 ollama_bridge.py --model qwen3:8b
```

### View Audit Logs

Every tool call is logged:
```bash
cat ~/.dhan-mcp/logs/trades.jsonl | python3 -m json.tool
```

## Roadmap

- [x] MCP server with 27 tools
- [x] Safety layer (6-step validation)
- [x] Dry-run mode
- [x] Audit logging
- [x] Stock alias resolution
- [x] Token hot-swap
- [x] Market status awareness
- [x] Ollama bridge
- [x] Strategy framework (exposed as additional MCP tools)
- [x] Multi-strategy concurrent execution
- [x] Auto-recovery on server restart
- [x] ATR-based and trailing stop loss types (with activate_after threshold)
- [x] Options-aware SL/target (uses live option premium)
- [x] Backtest with options limitations disclosure
- [x] Derived indicators (LAG, CHANGE, SLOPE) — two-pass computation
- [x] Direction-specific exits (conditions_ce / conditions_pe)
- [x] Time stop (max_bars)
- [ ] Partial exits (staged profit-taking)
- [ ] Multi-leg orders (straddles, strangles, spreads)
- [ ] WebSocket live feed integration

## Strategy Framework

The framework adds 12 strategy management tools to this same MCP server. Any connected AI can create, run, monitor, and improve trading strategies — all through the same MCP interface.

### How it works

```
Any AI client connects to DhanMCP
    → sees 27 trading tools + 12 framework tools
    → creates strategy via create_strategy tool (YAML)
    → starts strategy via start_strategy (paper or live)
    → framework runs internally: scheduler → data → indicators → engine → executor
    → strategy engine is pure code (deterministic, no AI judgment)
    → Gemma4/local LLM writes commentary on each trade (narrator role)
    → main AI analyses performance + commentary → improves strategy
```

### Framework tools

| Tool | Purpose |
|------|---------|
| `get_strategy_template` | Get YAML schema, supported indicators, condition syntax |
| `create_strategy` | Validate and save a strategy from YAML |
| `list_saved_strategies` | List all saved strategies |
| `get_strategy_details` | Get full config of a strategy |
| `start_strategy` | Start background scheduler (paper/live mode). Multiple strategies can run concurrently. |
| `stop_strategy` | Stop a running strategy by ID (or the only running one) |
| `get_strategy_status` | Live status for one or all strategies: phase, signals, position, P&L |
| `get_trade_log` | Trade history with reasons and performance stats |
| `get_strategy_commentary` | AI-generated daily summary via Ollama |
| `get_strategy_profile` | Version history, changes, performance snapshots |
| `log_strategy_change` | Record improvements or config changes to profile |
| `backtest_strategy` | Replay historical candles through strategy, simulate trades |
| `dhanwin` | Activation menu — returns the 5-option menu for any AI client |

### Three AI roles

1. **Main AI** (Claude, Gemini, etc.) — designs strategy with user, analyses performance, improves strategy
2. **Strategy engine** (pure Python code) — deterministic rules: `if rsi < 30 and ema_cross: BUY`. No AI judgment.
3. **Narrator AI** (Gemma4 via Ollama) — writes real-time commentary on each trade for later analysis

### Supported indicators (24 base + 3 derived)

**Base:** `EMA` `SMA` `RSI` `MACD` `MACD_SIGNAL` `MACD_DIFF` `BOLLINGER_HIGH` `BOLLINGER_LOW` `BOLLINGER_MID` `ATR` `VWAP` `SUPERTREND` `ADX` `STOCH_K` `STOCH_D` `OBV` `DPO` `DPO_SIGNAL` `OBV_HULL` `OBV_HULL_SIGNAL` `VORTEX_POS` `VORTEX_NEG` `VORTEX_SIGNAL` `HULL_MA` `CONFIDENCE`

**Derived** (reference other indicators, computed in second pass):
- `LAG` — value N bars ago. Config: `{name: adx_prev, type: LAG, source: adx, period: 1}`
- `CHANGE` — 1-bar delta. Config: `{name: ema_change, type: CHANGE, source: ema_trend}`
- `SLOPE` — rate of change over N bars. Config: `{name: ema_slope, type: SLOPE, source: ema_trend, period: 3}`

Use derived indicators to express "rising", "falling", or "compare to previous" conditions like `adx > adx_prev`.

### Strategy lifecycle

```
User discusses with AI → AI calls get_strategy_template
    → AI creates strategy YAML → create_strategy (validates + saves)
    → start_strategy mode=paper → monitor via get_strategy_status
    → get_trade_log for results → get_strategy_commentary for AI analysis
    → AI improves strategy → create new version → repeat
    → user approves → start_strategy mode=live
```

### Strategy YAML example

```yaml
id: rsi_ema_nifty_v1
name: "RSI + EMA Crossover on NIFTY"
instrument:
  index: NIFTY
  option_preference: ATM
  option_type: CE
  trade_type: BUY
  expiry_preference: nearest
interval: 5
indicators:
  - {name: ema_fast, type: EMA, period: 9}
  - {name: ema_slow, type: EMA, period: 21}
  - {name: rsi, type: RSI, period: 14}
  - {name: atr, type: ATR, period: 14}
  - {name: ema_slope, type: SLOPE, source: ema_fast, period: 3}  # derived
entry:
  conditions: ["rsi < 30", "ema_fast > ema_slow", "ema_slope > 0"]
  lots: 1
exit:
  conditions: ["rsi > 70", "ema_fast < ema_slow"]
  max_bars: 6  # time stop
stop_loss: {type: atr, multiplier: 2.0, atr_indicator: atr, min: 50, max: 120}
target: {type: points, value: 200}
risk:
  max_loss_per_day: 5000
  max_trades_per_day: 5
  cool_off_after_loss: 2
```

### Framework architecture

```
framework/
├── schema.py        # Strategy YAML validation + storage
├── database.py      # SQLite: candles, indicators, trades, state
├── data_manager.py  # OHLC fetch + 24 base indicators + 3 derived types (two-pass)
├── engine.py        # Condition evaluator + SL types (%, pts, ATR, trailing) + time stop + strike selector
├── risk.py          # Risk governor (daily loss, trade count, cool-off)
├── scheduler.py     # Multi-strategy background loop + trade execution + auto-recovery
├── narrator.py      # Gemma4/Ollama commentary bridge
└── backtester.py    # Historical replay + signal validation (options P&L disclaimer)
```

### Stop loss types

| Type | Config | Description |
|------|--------|-------------|
| `percentage` | `{type: percentage, value: 20}` | Fixed % from entry price |
| `points` | `{type: points, value: 150}` | Fixed points from entry price |
| `atr` | `{type: atr, multiplier: 2.0, atr_indicator: atr, min: 50, max: 120}` | Dynamic ATR-based SL with optional min/max clamps |
| `trailing` | `{type: trailing, value: 40, trail_type: points, activate_after: 80}` | Trails from peak favorable price. Optional `activate_after`: only starts trailing after N points profit. |

### Exit features

- **Direction-specific exits**: Use `exit.conditions_ce` and `exit.conditions_pe` for separate CE/PE exit logic
- **Time stop**: `exit.max_bars: 6` — force exit after N candles if trade hasn't moved

### Multi-strategy & persistence

- Multiple strategies can run concurrently
- Running strategies persist to `~/.dhan-mcp/running_strategies.json`
- Auto-restored on server restart — no manual re-deployment needed

### Backtesting limitations

Backtesting replays historical spot index candles through the strategy engine. For options strategies, the P&L is computed on spot movement, not actual option premiums (historical option premium data is unavailable via Dhan API). **Use paper trading for accurate options P&L.** Signal timing and direction from backtests remain valid.

### Risk controls (hard limits, strategy cannot override)

- **Max daily loss** — stops trading when cumulative P&L hits the limit
- **Max trades per day** — prevents overtrading
- **Cool-off period** — skips N signals after consecutive losses

## License

LGPL-3.0 — see [LICENSE](LICENSE)
