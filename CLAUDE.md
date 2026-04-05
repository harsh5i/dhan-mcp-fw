# dhan-mcp-fw

MCP server for NSE stock analysis and NIFTY/BANKNIFTY options trading via Dhan.
39 tools: 27 trading + 12 strategy framework.
Full documentation: see README.md in this directory.

## Onboarding protocol

### First time connecting
When an AI connects to this MCP for the first time (no prior conversation about Dhan trading), respond with:

> "We have gained Dhan trade capability. What do you wanna do?"

Then wait for the user's instructions.

### Returning sessions — activation phrase: "DhanWin"
When the user says **"DhanWin"**, it means they want to enter Dhan trading mode. Present this menu:

1. **Learn** — How to use DhanMCP and the Trading framework
2. **Reconcile** — Check logs and bring current status if things were disrupted
3. **Dashboard** — View all active strategies, positions, P&L, and performance
4. **Strategies** — Browse, inspect, and manage saved strategies
5. **Deploy** — Start a strategy (paper or live mode)
6. **New strategy** — Define and create a new trading strategy
7. **Backtest** — Backtest on spot data (signals only — options P&L requires paper trade)

Wait for the user to pick a number or describe what they want.

### Dashboard mode (option 3) details
Show a combined view:
- All running strategies with status (`get_strategy_status` with no strategy_id)
- Current/recent positions and trades
- Performance stats (today, this week, this month)
- Use `get_trade_log`, `get_strategy_profile`, `get_pnl_summary`, `get_positions`, `get_order_book`

### Strategies mode (option 4) details
- `list_saved_strategies` — show all strategies in the pool
- `get_strategy_details` — inspect a specific strategy's config
- `get_strategy_profile` — version history and performance over time
- `get_strategy_commentary` — AI-generated analysis via Ollama

### Deploy mode (option 5) details
- Show available strategies from pool (`list_saved_strategies`)
- User picks one, then chooses mode: paper or live
- `start_strategy(strategy_id, mode)` — deploys it
- Multiple strategies can run simultaneously
- Strategies auto-restore on server restart


## Quick reference
- NIFTY spot: security_id=13, segment=INDEX
- BANKNIFTY spot: security_id=25, segment=INDEX
- Mode: check config.yaml → safety.mode (dry-run or live)
- Token: 24hr validity, use update_token tool to hot-swap
- Lot sizes: NIFTY=75, BANKNIFTY=30

## Strategy framework
- Strategies are YAML files at ~/.dhan-mcp/strategies/
- Per-strategy SQLite DB at ~/.dhan-mcp/strategies/{id}.db
- 24 base indicators + 3 derived types (LAG, CHANGE, SLOPE)
- Derived indicators reference other indicators: `{name: adx_prev, type: LAG, source: adx, period: 1}`
- Conditions syntax: "rsi < 30", "ema_fast > ema_slow", "adx > adx_prev"
- Direction-specific entry AND exit: conditions_ce/conditions_pe for both entry and exit blocks
- Stop loss types: percentage, points, atr (dynamic with min/max clamp), trailing (with activate_after threshold)
- Time stop: `exit.max_bars` — force exit after N candles
- Risk governor: max_loss_per_day, max_trades_per_day, cool_off_after_loss
- Narrator: Gemma4 via Ollama for trade commentary
- Strategy profiles: version history, changes, performance snapshots over time
- Framework tools: get_strategy_template, create_strategy, list_saved_strategies, get_strategy_details, start_strategy, stop_strategy, get_strategy_status, get_trade_log, get_strategy_commentary, get_strategy_profile, log_strategy_change, backtest_strategy
- Backtest: replays spot candles through strategy engine. For options strategies, P&L is approximate (spot-based) — use paper trading for accurate options P&L
- Daily candles: any date range. Intraday: last 5 trading days (Dhan limit)
- Multi-strategy: multiple strategies can run concurrently
- Auto-recovery: running strategies persist across server restarts
- Options-aware: SL/target checks use live option premium when in position
