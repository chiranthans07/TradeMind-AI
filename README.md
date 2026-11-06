# MANTHAN 2026 – Autonomous Trading Bot

Python bot for the JNNCE *Autonomous Trading Systems Challenge*. It follows the endpoints, message
formats, limits and rules in the problem-statement PDF. Ideas such as EWMA/momentum signals,
cross-sectional z-scores, volume confirmation, regime scaling and event decay were borrowed from the
`WhitePaper` reference repo; its websocket/MVO plumbing is for a different contest and is not used.

## Quick start (on venue Wi-Fi)

```bash
pip install -r requirements.txt
cp .env.example .env          # fill SERVER_IP, BOT_ACCESS_TOKEN, TEAM_USERNAME
python run_bot.py             # live: trades only when stage == simulation and status == RUNNING
python run_bot.py --sandbox   # also trades during the 'development' practice loop
```

Order of events the bot handles on its own: start Kafka consumers (group `group-<TEAM_USERNAME>`) ->
ACK `preflight_tick` via `POST /bot/heartbeat` -> `GET /stocks` -> warm-start indicators from
`GET /stocks/:ticker/history` -> poll `GET /stage` -> trade when the simulation is RUNNING.
Logs go to `logs/bot.log` and every order to `logs/orders.jsonl` (with the reason for each trade,
useful for the jury defence).

## Endpoint / rule map (from the PDF)

| PDF requirement | Where |
|---|---|
| Kafka `<SERVER_IP>:9094`, PLAINTEXT, topics `market.ticks`, `market.news` | `bot/feed.py` |
| `group-<team>`, `auto_offset_reset=latest`, `enable_auto_commit=False`, up to 10 consumers | `bot/feed.py`, `NUM_CONSUMERS` |
| `preflight_tick` -> `POST /bot/heartbeat {"seq"}` (>= 1 per 15 s; we ACK every ~4 s) | `Engine.handle_preflight` |
| `GET /stocks` – never hard-code tickers | `Engine.bootstrap` |
| `GET /stocks/:ticker/history?days=` warm start | `Engine.warm_start` |
| `GET /stage` poll 2-3 s; trade only if `simulation` + `RUNNING`; stop on PAUSED/COMPLETED | `Engine.stage_loop`, `trading_allowed` |
| `POST /orders` BUY/SELL/SHORT/COVER, whole-integer qty <= 1,000,000, `date` field | `bot/api.py` |
| `GET /portfolio` reconcile | `Portfolio.sync_from_server` |
| 120 req/min, 429 -> 30 s penalty box | `bot/ratelimit.py` (we cap at 100/min) |
| Rejection codes (all 9) | `Engine._handle_rejection` + local pre-validation |
| 3-4 s decision window | day batching + parallel order dispatch, ~0.35 s typical |
| Brokerage Rs20, DP Rs15.34 on SELL, STCG 15 %, loss offsets (Sec. 70/71) | `bot/portfolio.py`, `bot/strategy.py` |
| 100 % margin lock for SHORT, 10-day Friday square-off, negative-balance block | `Strategy`, `ShortCalendar`, `trading_blocked` |
| Sandbox loops back to Day 1 | date-went-backwards detection resets history and re-warms |

## Strategy in one page

**Signal** (per stock, score in [-1, 1], fully explainable):
EMA 8/21 trend, MACD histogram, 5d+20d momentum (all volatility-normalised), RSI/Bollinger
mean-reversion (damped in clean trends), cross-sectional 5d momentum z-score, volume confirmation,
decayed **news sentiment** (rule + lexicon scorer in `bot/sentiment.py`, microsecond latency) and a
market-regime tilt from an equal-weight index built from the universe.

**Discipline**: entry needs |score| >= 0.35, so doing nothing is the default. Panic filter blocks new
entries when index volatility spikes or the 5-day index drawdown is worse than -6 %.

**Sizing**: volatility-targeted, max 12 % of wealth per long (8 % per short), max 8 longs / 3 shorts,
gross exposure <= 85 %, 10 % cash reserve, minimum trade Rs25,000 so Rs20-35 friction stays tiny.

**Exits**: hard stop (5.5 % long / 4 % short), trailing stop, negative-news shock, take-profit when the
signal fades, signal reversal, over-concentration trim, and an end-of-simulation cover of shorts.

**Tax & friction**:
- Whole run is far below 365 days, so everything is STCG at 15 %; LTCG never applies.
- Reluctance to realise taxable gains (`tax_drag`) lowers the exit threshold for winners.
- **Loss harvesting**: a position down >= 3 % with weak signal is sold when realised gains exist, so the
  loss offsets gains (Sections 70/71).
- Longs are not liquidated at the end (avoids DP charge and realised tax; wealth is marked to market).
- Hysteresis bands, minimum notional and a daily order cap avoid churn.

**Reliability**: the rate limiter gives orders and heartbeats priority over history calls; orders are
validated locally (cash, margin, existing position) before sending; an order that cannot be sent while
its day is current is dropped, never sent late at the wrong close price; Kafka consumers reconnect with
back-off; a crash inside one decision cycle is logged and the bot continues.

## Testing without the venue

```bash
python -m unittest discover -s tests -v   # unit tests
python run_backtest.py --seeds 1 2 3 4 5  # synthetic markets via an in-process exchange simulator
python run_mock_e2e.py --interval 1.0     # real HTTP client + threads against a mock gateway
```

The mock gateway implements the PDF endpoints, fees, margin, rejection codes, anti-lookahead history and
Friday square-off. **The synthetic data is generated by us, including news-driven drift, so these runs
prove that the plumbing is correct and risk is bounded; they say nothing about real alpha.** Use the
venue sandbox loop (`--sandbox`) to calibrate thresholds.

## Things to verify at the venue (the PDF does not specify them)

1. **`GET /portfolio` and `POST /orders` response shapes.** The parser accepts several common shapes
   (`holdings`/`positions`/`short_positions`, `cash`/`available_cash`, `quantity`/`qty`, ...). Hit
   `/portfolio` once from the portal's API tab; if the keys differ, adjust `parse_portfolio()` in
   `bot/portfolio.py` (about 10 lines). If parsing fails the bot keeps its local mirror and keeps trading.
2. **Do orders work in the `development` stage?** The PDF says to test orders there but also lists
   `SIMULATION_NOT_RUNNING` for development. Default is live-only; `--sandbox` enables dev trading.
3. **Which Fridays are square-off days.** Learned automatically the first time a short disappears.
4. **Short proceeds accounting.** Local model assumes cash drops by trade value + fee (margin lock);
   periodic `/portfolio` sync corrects any difference.
5. Tune `bot/config.py` (thresholds, `max_weight`, `stop_loss`) from sandbox runs.

## Layout

```
run_bot.py            live entry point          run_backtest.py   offline synthetic backtest
run_mock_e2e.py       HTTP + threads rehearsal  tests/            unit tests
bot/  config api ratelimit feed engine state indicators sentiment strategy portfolio
sim/  data exchange mock_http   (test harness only; not used in live runs)
```
