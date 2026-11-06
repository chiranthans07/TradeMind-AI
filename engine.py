"""Trading engine: day batching, decisions, order execution, pre-flight, stage gating.

Threading model
---------------
* N Kafka consumer threads (same group id => partitions split across them) call
  Engine.on_tick / Engine.on_news. These only append to the current DayBatch.
* One decision thread waits for a batch to be complete (all tickers received, or
  batch_wait_s after the first tick), waits a short news grace period (news lands
  ~150 ms after ticks), then scores the universe and dispatches orders in parallel.
* One stage thread polls GET /stage every ~2.5 s.
* One heartbeat thread ACKs preflight_tick sequence numbers.

Order safety:
* Orders execute at the *current day's close*, so any order not sent before the next
  day's first tick arrives is dropped (never sent late at the wrong price).
* Every order is validated locally (cash, margin, existing position) before sending.
"""
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .portfolio import Portfolio
from .state import MarketState
from .strategy import Strategy

log = logging.getLogger("engine")


class DayBatch:
    def __init__(self, date):
        self.date = date
        self.ticks = {}
        self.news = []
        self.t_first = time.time()
        self.t_complete = None
        self.decided = False


class Engine:
    def __init__(self, cfg, api, trade_log_path=None, clock=time.time):
        self.cfg, self.api = cfg, api
        self.state = MarketState()
        self.book = Portfolio(cfg)
        self.strategy = Strategy(cfg, self.state, self.book)
        self.clock = clock

        self.stage = {"stage": "development", "status": "IDLE", "total_days": None, "days_completed": 0}
        self.stage_lock = threading.Lock()
        self.trading_blocked = False       # ACCOUNT_BLOCKED => SELL/COVER only
        self.paused_until = 0.0

        self.batch = None
        self.batch_lock = threading.Condition()
        self.latest_date = None
        self.day_idx = 0
        self.expected_n = 0
        self.stop_evt = threading.Event()
        self._pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="order")
        self._decision_thread = None
        self.last_hb = 0.0
        self.latencies = []
        self.order_log = []
        self.trade_log_path = trade_log_path
        self.metrics = {"batches": 0, "skipped_stale": 0, "orders_sent": 0, "orders_ok": 0,
                        "orders_rejected": 0, "days_no_trade": 0}
        self.sim_mode = False              # in-process backtests call decide_now() directly

    # ================================================================== lifecycle
    def start(self):
        self._decision_thread = threading.Thread(target=self._decision_loop, name="decision", daemon=True)
        self._decision_thread.start()

    def stop(self):
        self.stop_evt.set()
        with self.batch_lock:
            self.batch_lock.notify_all()
        self._pool.shutdown(wait=False)

    # ================================================================== pre-flight
    def bootstrap(self, warm=True):
        """GET /stocks, warm-start indicators from /history, sync portfolio."""
        r = self.api.stocks()
        if r.ok and isinstance(r.data, dict) and r.data.get("stocks"):
            self.state.set_universe(r.data["stocks"])
            self.expected_n = len(self.state.universe)
            log.info("Universe: %d stocks %s", self.expected_n, self.state.universe[:8])
        else:
            log.warning("GET /stocks failed (%s) - will discover tickers from ticks", r)
        if warm:
            self.warm_start()
        self.sync_portfolio()
        return self.expected_n

    def warm_start(self):
        n_ok = 0
        for t in list(self.state.universe):
            r = self.api.history(t, self.cfg.history_days)
            if r.ok and isinstance(r.data, dict):
                n_ok += 1 if self.state.load_history(t, r.data.get("prices", [])) else 0
            else:
                log.warning("history %s failed: %s", t, r)
            if self.stop_evt.is_set():
                return
        log.info("Warm-started %d/%d tickers", n_ok, len(self.state.universe))

    def sync_portfolio(self):
        r = self.api.portfolio()
        if r.ok:
            if self.book.sync_from_server(r.data, self.day_idx):
                return True
        else:
            log.debug("portfolio sync failed: %s", r)
        return False

    def handle_preflight(self, seq):
        """ACK a preflight_tick (>=1 per 15 s required; we ACK at most every 4 s)."""
        now = time.time()
        if seq is None or now - self.last_hb < 4.0:
            return
        self.last_hb = now
        r = self.api.heartbeat(seq)
        if not r.ok:
            log.warning("heartbeat seq=%s failed: %s", seq, r)
            self.last_hb = now - 3.0          # retry soon
        else:
            log.info("heartbeat ACK seq=%s", seq)

    # ================================================================== stage
    def poll_stage_once(self):
        r = self.api.stage()
        if r.ok and isinstance(r.data, dict):
            with self.stage_lock:
                self.stage.update({k: r.data.get(k) for k in
                                   ("stage", "status", "total_days", "days_completed", "current_date",
                                    "interval_seconds") if k in r.data})
        return r

    def stage_loop(self):
        prev = None
        while not self.stop_evt.is_set():
            self.poll_stage_once()
            cur = (self.stage.get("stage"), self.stage.get("status"))
            if cur != prev:
                log.info("STAGE -> %s / %s (day %s/%s)", cur[0], cur[1],
                         self.stage.get("days_completed"), self.stage.get("total_days"))
                if prev and prev[0] != "simulation" and cur[0] == "simulation":
                    threading.Thread(target=self.on_simulation_start, daemon=True).start()
                prev = cur
            self.stop_evt.wait(self.cfg.stage_poll_s)

    def on_simulation_start(self):
        """Fresh universe + portfolio the moment live trading begins."""
        try:
            self.bootstrap(warm=False)
        except Exception:
            log.exception("simulation-start bootstrap failed")

    def trading_allowed(self):
        if self.sim_mode:
            return True
        with self.stage_lock:
            st, status = self.stage.get("stage"), self.stage.get("status")
        if status != "RUNNING":
            return False
        if st == "simulation":
            return True
        return st == "development" and self.cfg.allow_dev_trading

    def remaining_days(self):
        with self.stage_lock:
            tot, done = self.stage.get("total_days"), self.stage.get("days_completed")
        if tot is None or done is None:
            return None
        return int(tot) - int(done)

    # ================================================================== market data in
    def on_tick(self, msg):
        """Called from consumer threads with a decoded market.ticks JSON message."""
        et = msg.get("event_type") or msg.get("event")
        if et == "preflight_tick":
            seq = msg.get("seq", (msg.get("data") or {}).get("seq"))
            self.handle_preflight(seq)
            return
        if et != "price_tick":
            return
        d, date = msg.get("data") or {}, msg.get("simulated_date")
        t = d.get("ticker")
        if not t or not date:
            return
        try:
            o, h, l, c, v = (float(d["open"]), float(d["high"]), float(d["low"]),
                             float(d["close"]), float(d.get("volume", 0)))
        except (KeyError, TypeError, ValueError):
            return
        with self.batch_lock:
            self._rollover_if_needed(date)
            b = self.batch
            if b is None or b.date != date:
                return
            b.ticks[t] = (o, h, l, c, v)
            if self.expected_n and len(b.ticks) >= self.expected_n and b.t_complete is None:
                b.t_complete = self.clock()
            self.batch_lock.notify_all()

    def on_news(self, msg):
        if (msg.get("event_type") or msg.get("event")) != "news_flash":
            return
        d, date = msg.get("data") or {}, msg.get("simulated_date") or (msg.get("data") or {}).get("date")
        t = d.get("ticker")
        if not t or not date:
            return
        with self.batch_lock:
            self._rollover_if_needed(date)
            if self.batch and self.batch.date == date:
                self.batch.news.append((t, d.get("headline", ""), d.get("description", "")))
            else:
                self.state.add_news(t, date, d.get("headline", ""), d.get("description", ""))

    def _rollover_if_needed(self, date):
        """Called with batch_lock held. Opens a new batch when the date changes."""
        if self.batch and self.batch.date == date:
            return
        if self.latest_date and date < self.latest_date:
            # dev sandbox looped back to Day 1: prices revert, history is now invalid
            log.warning("Simulated date went backwards (%s -> %s): sandbox loop reset", self.latest_date, date)
            self.state.reset_history()
            self.book.pos.clear()
            self.day_idx = 0
            self.book.cooldown.clear()
            self.latest_date = None
            threading.Thread(target=self._after_loop_reset, daemon=True).start()
        if self.batch and not self.batch.decided:
            self.metrics["skipped_stale"] += 1       # superseded before we decided
            self.batch.decided = True
        self.latest_date = date
        self.batch = DayBatch(date)

    def _after_loop_reset(self):
        try:
            self.warm_start()
            self.sync_portfolio()
        except Exception:
            log.exception("post-reset re-sync failed")

    # ================================================================== decision loop
    def _decision_loop(self):
        cfg = self.cfg
        while not self.stop_evt.is_set():
            with self.batch_lock:
                b = self.batch
                if not b or b.decided:
                    self.batch_lock.wait(0.05)
                    continue
                now = self.clock()
                complete_ready = b.t_complete is not None and now - b.t_complete >= cfg.batch_news_grace_s
                timed_out = now - b.t_first >= cfg.batch_wait_s
                if not (complete_ready or timed_out):
                    self.batch_lock.wait(0.02)
                    continue
                b.decided = True
            try:
                self.process_batch(b)
            except Exception:
                log.exception("process_batch crashed (bot continues)")

    def process_batch(self, b):
        """Ingest the day, score, decide, execute. Safe to call synchronously in backtests."""
        t0 = time.time()
        date = b.date
        for t, (o, h, l, c, v) in b.ticks.items():
            self.state.add_bar(t, date, o, h, l, c, v)
        news_hits = {}
        for t, head, desc in b.news:
            s = self.state.add_news(t, date, head, desc)
            news_hits[t] = news_hits.get(t, 0.0) + s
        if not self.expected_n:
            self.expected_n = len(self.state.universe)
        self.state.last_date = date
        self.day_idx += 1
        self.metrics["batches"] += 1
        prices = {t: v[3] for t, v in b.ticks.items()}
        for t in self.state.universe:                     # carry last known price for missing tickers
            if t not in prices and self.state.last_close(t):
                prices[t] = self.state.last_close(t)
        self.book.update_marks(prices)

        if not self.trading_allowed() or self.api.limiter.in_penalty() or time.time() < self.paused_until:
            log.debug("%s: observing only (stage=%s)", date, self.stage.get("stage"))
            return []

        # Reconcile with the exchange (1 request): every 3rd day, and on every Friday while we hold a
        # short - Fridays are when the 10-day short square-off can silently close positions.
        shorts_before = set(self.book.shorts())
        is_friday = self.strategy.calendar.is_settlement(date)
        if self.day_idx % 3 == 0 or self.day_idx <= 1 or (shorts_before and is_friday):
            self.sync_portfolio()
            vanished = shorts_before - set(self.book.shorts())
            if vanished and is_friday:
                self.strategy.calendar.learn(date)            # learn which Fridays are settlement days
                log.info("%s: shorts %s were squared off by the platform (settlement day learned)", date, sorted(vanished))

        intents = self.strategy.decide(date, self.day_idx, self.remaining_days(), prices,
                                       blocked=self.trading_blocked)
        if self.trading_blocked:
            intents = [i for i in intents if i.action in ("SELL", "COVER")]
        intents = self._validate(intents, prices)
        for t, sc in news_hits.items():
            if abs(sc) >= 0.4:
                log.info("%s NEWS %s score %+.2f", date, t, sc)
        if not intents:
            self.metrics["days_no_trade"] += 1
            log.info("%s: no trade (wealth %.0f, cash %.0f, %d positions)", date,
                     self.book.wealth(prices), self.book.cash, len(self.book.pos))
            return []
        self._execute(intents, b, t0)
        return intents

    # ================================================================== validation & execution
    def _validate(self, intents, prices):
        """Local pre-checks => avoids paying for rejected orders (counted in the scorecard)."""
        cfg, out = self.cfg, []
        cash = self.book.cash
        for i in intents:
            qty = int(i.qty)
            if qty < 1 or qty > cfg.max_qty or i.ticker not in prices:
                continue
            if i.action == "BUY":
                p = self.book.pos.get(i.ticker)
                if p and p.side == "SHORT":
                    continue                                   # BUY would silently cover
                cost = qty * i.price + cfg.brokerage
                if cost > cash:
                    qty = int((cash - cfg.brokerage) // i.price)
                    cost = qty * i.price + cfg.brokerage
                if qty < 1:
                    continue
                cash -= cost
            elif i.action == "SHORT":
                cost = qty * i.price + cfg.brokerage           # 100% margin + fee
                if cost > cash:
                    qty = int((cash - cfg.brokerage) // i.price)
                    cost = qty * i.price + cfg.brokerage
                if qty < 1:
                    continue
                cash -= cost
            elif i.action == "SELL":
                p = self.book.pos.get(i.ticker)
                if not p or p.side != "LONG":
                    continue
                qty = min(qty, p.qty)
                cash += qty * i.price - cfg.brokerage - cfg.dp_charge
            elif i.action == "COVER":
                p = self.book.pos.get(i.ticker)
                if not p or p.side != "SHORT":
                    continue                                   # would give NO_SHORT_POSITION
                qty = min(qty, p.qty)
                cash += qty * p.avg + p.pnl(i.price) - cfg.brokerage
            i.qty = qty
            out.append(i)
        return out

    def _execute(self, intents, batch, t_start):
        cfg = self.cfg
        date = batch.date
        futures = []
        for i in intents:
            futures.append(self._pool.submit(self._send_one, i, batch))
        for f in futures:
            try:
                f.result(timeout=cfg.decision_deadline_s + 1.5)
            except Exception:
                log.exception("order future failed")
        lat = time.time() - batch.t_first
        self.latencies.append(lat)
        log.info("%s: %d orders dispatched, decision latency %.2fs (first tick -> last ack)",
                 date, len(intents), lat)

    def _send_one(self, i, batch):
        cfg = self.cfg
        # never send an order for yesterday's close
        if self.latest_date != batch.date and not self.sim_mode:
            self.metrics["skipped_stale"] += 1
            log.warning("DROP stale order %s (day moved on)", i)
            return
        if time.time() - batch.t_first > cfg.decision_deadline_s and not self.sim_mode and i.priority > 0:
            log.warning("DROP late non-critical order %s", i)
            return
        self.metrics["orders_sent"] += 1
        res = self.api.place_order(i.ticker, i.action, i.qty, batch.date)
        rec = {"t": time.time(), "date": batch.date, "ticker": i.ticker, "action": i.action, "qty": i.qty,
               "price": i.price, "ok": res.ok, "code": res.code, "reason": i.reason, "score": round(i.score, 3)}
        self.order_log.append(rec)
        if self.trade_log_path:
            try:
                with open(self.trade_log_path, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps(rec) + "\n")
            except OSError:
                pass
        if res.ok:
            self.metrics["orders_ok"] += 1
            fill_px = _fill_price(res.data, i.price)
            self.book.apply_fill(i.action, i.ticker, i.qty, fill_px, self.day_idx)
            if i.priority == 0 and i.action in ("SELL", "COVER"):
                self.book.cooldown[i.ticker] = self.day_idx + cfg.cooldown_days
            log.info("OK   %s %d %s @%.2f  | %s", i.action, i.qty, i.ticker, fill_px, i.reason)
        else:
            if res.code != "LOCAL_RATE_WAIT":
                self.metrics["orders_rejected"] += 1
            log.warning("FAIL %s %d %s -> %s %s | %s", i.action, i.qty, i.ticker, res.code, res.message, i.reason)
            self._handle_rejection(res.code, i)

    def _handle_rejection(self, code, i):
        if code in ("SIMULATION_NOT_RUNNING",):
            self.paused_until = time.time() + 3.0
            self.poll_stage_once()
        elif code == "ACCOUNT_BLOCKED":
            self.trading_blocked = True
            log.error("ACCOUNT_BLOCKED - switching to SELL/COVER only until cash is positive")
            self.sync_portfolio()
        elif code in ("INSUFFICIENT_CAPITAL", "INSUFFICIENT_MARGIN", "NO_SHORT_POSITION"):
            self.sync_portfolio()
        elif code == "INVALID_TICKER":
            self.state.universe = [t for t in self.state.universe if t != i.ticker]
        elif code == "RATE_LIMITED":
            log.error("RATE_LIMITED - backing off 31 s")
        elif code == "ACCOUNT_NOT_VERIFIED":
            log.error("Team not verified at the registration desk - cannot trade")
            self.paused_until = time.time() + 30
        if self.trading_blocked and self.book.cash > 0:
            self.trading_blocked = False

    # ================================================================== reporting
    def summary(self):
        lat = sorted(self.latencies)
        return {
            **self.metrics,
            "api": dict(self.api.stats),
            "cash": round(self.book.cash, 2),
            "positions": {t: (p.side, p.qty, round(p.avg, 2)) for t, p in self.book.pos.items()},
            "realized_pnl": round(self.book.realized, 2),
            "fees_paid": round(self.book.fees_paid, 2),
            "est_tax": round(self.book.tax_estimate(), 2),
            "latency_p50_s": round(lat[len(lat) // 2], 3) if lat else None,
            "latency_max_s": round(lat[-1], 3) if lat else None,
        }


def _fill_price(data, default):
    """Execution is always at the day's close; prefer the server-reported fill if present."""
    if isinstance(data, dict):
        for src in (data, data.get("order") if isinstance(data.get("order"), dict) else {}):
            for k in ("fill_price", "executed_price", "execution_price", "price"):
                try:
                    v = float(src.get(k))
                    if v > 0:
                        return v
                except (TypeError, ValueError):
                    pass
    return default
