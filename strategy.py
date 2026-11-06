"""Signal model + position/risk logic.

Pipeline per simulated day (all tickers at once, ~ms of CPU):

  1. score(t) in [-1, 1] from 7 explainable components
        trend (EMA8/21 spread, vol-normalised)     0.20
        MACD histogram                              0.12
        momentum 5d + 20d (vol-normalised)          0.18
        mean-reversion (RSI + Bollinger %b)         0.12  (damped when trend is clean)
        cross-sectional 5d momentum z-score         0.10
        volume confirmation                         0.08
        news sentiment (decayed headline score)     0.55
        + market regime tilt                        0.12
  2. Exits first (stop-loss, trailing stop, news shock, signal reversal, tax-loss harvest)
  3. Entries ranked by |score|, sized by volatility targeting, capped by per-stock,
     gross-exposure and cash-reserve limits
  4. Trades are only issued if they clear the friction filter (min notional, hysteresis band).

"No trade" is the default outcome: the entry threshold is deliberately high.
"""
import datetime as dt
import logging
import math

import numpy as np

from . import indicators as ind

log = logging.getLogger("strategy")

SHORT_MAX_WEIGHT = 0.08
SHORT_STOP = 0.04


class Intent:
    __slots__ = ("ticker", "action", "qty", "price", "priority", "score", "reason")

    def __init__(self, ticker, action, qty, price, priority, score, reason):
        self.ticker, self.action, self.qty, self.price = ticker, action, int(qty), price
        self.priority, self.score, self.reason = priority, score, reason

    def __repr__(self):
        return f"{self.action} {self.qty} {self.ticker}@{self.price:.2f} p{self.priority} s={self.score:+.2f} [{self.reason}]"


class ShortCalendar:
    """Knows (or learns) which Fridays are 10-day short square-off days."""

    def __init__(self, every=10):
        self.every = every
        self.parity = None          # ISO-week parity of settlement Fridays once learned

    @staticmethod
    def _d(date):
        try:
            return dt.date.fromisoformat(str(date)[:10])
        except ValueError:
            return None

    def learn(self, date):
        d = self._d(date)
        if d and d.weekday() == 4:
            self.parity = d.isocalendar()[1] % 2

    def is_settlement(self, date):
        d = self._d(date)
        if not d or d.weekday() != 4:
            return False
        return True if self.parity is None else (d.isocalendar()[1] % 2 == self.parity)

    def days_to_settlement(self, date):
        d = self._d(date)
        if not d:
            return 99
        for i in range(0, 15):
            if self.is_settlement((d + dt.timedelta(days=i)).isoformat()):
                return i
        return 99


class Strategy:
    def __init__(self, cfg, state, book):
        self.cfg, self.state, self.book = cfg, state, book
        self.calendar = ShortCalendar(cfg.short_settlement_days)
        self.last_scores = {}

    # ------------------------------------------------------------------ scoring
    def score_all(self, date):
        st, cfg = self.state, self.cfg
        cs5 = st.cross_section_z(5)
        regime = st.market_regime()
        out = {}
        for t in st.universe:
            if st.n_bars(t) < cfg.min_history:
                continue
            o, h, l, c, v = st.arrays(t)
            out[t] = self._score_one(t, date, o, h, l, c, v, cs5.get(t, 0.0), regime)
        self.last_scores = out
        self.regime = regime
        return out

    def _score_one(self, t, date, o, h, l, c, v, cs_z, regime):
        vol = ind.realized_vol(c, 20)
        price = float(c[-1])
        ema_f, ema_s = ind.ema(c, 8), ind.ema(c, 21)
        trend = math.tanh((ema_f / ema_s - 1.0) / (vol * 3.0))
        _, _, hist = ind.macd(c)
        macd_s = math.tanh(1.5 * hist / price / vol)
        m5, m20 = ind.momentum(c, 5), ind.momentum(c, 20)
        mom = 0.5 * math.tanh(0.5 * m5 / (vol * math.sqrt(5))) + 0.5 * math.tanh(0.5 * m20 / (vol * math.sqrt(20)))
        clean = abs(math.tanh(6.0 * ind.trend_strength(c, 20) / vol))        # 0..1
        r = ind.rsi(c, 14)
        mr = 0.0
        if r < 30:
            mr += (30 - r) / 30.0
        elif r > 70:
            mr -= (r - 70) / 30.0
        pb = ind.bollinger_pctb(c, 20)
        if pb < 0.05:
            mr += 0.5
        elif pb > 0.95:
            mr -= 0.5
        mr = max(-1.0, min(1.0, mr)) * (1.0 - 0.6 * clean)                    # trust MR less in clean trends
        cs = math.tanh(cs_z / 2.0)
        ret1 = float(c[-1] / c[-2] - 1.0) if len(c) > 1 else 0.0
        vr = ind.volume_ratio(v, 20)
        volc = (math.copysign(1.0, ret1) * min(1.0, (vr - 1.5) / 2.0)) if vr > 1.5 and abs(ret1) > 0.5 * vol else 0.0
        news = self.state.news_score(t, date)
        s = (0.20 * trend + 0.12 * macd_s + 0.18 * mom + 0.12 * mr + 0.10 * cs + 0.08 * volc
             + 0.55 * news + 0.12 * regime["reg"])
        # bad-bar guard: a >25% one-day gap (split / data glitch) is not a signal
        if abs(ret1) > 0.25 and abs(news) < 0.3:
            s = 0.0
        s = max(-1.0, min(1.0, s))
        return {"score": s, "vol": vol, "price": price, "news": news, "rsi": r, "trend": trend,
                "mom": mom, "mr": mr, "cs": cs, "ret1": ret1}

    # ------------------------------------------------------------------ decisions
    def decide(self, date, day_idx, remaining_days, prices, allow_new=True, blocked=False):
        """Return list[Intent] sorted by priority. prices: {ticker: close today}."""
        cfg, book = self.cfg, self.book
        sc = self.score_all(date)
        reg = self.regime
        wealth = book.wealth(prices)
        intents = []
        panic = reg["vol_ratio"] > 3.0 or reg["dd5"] < -0.06
        final_stretch = remaining_days is not None and remaining_days <= cfg.cover_shorts_before_end_days + 1
        no_new = (remaining_days is not None and remaining_days <= 2) or panic or not allow_new

        # ---------------- exits ----------------
        planned_exit = set()
        for t, p in list(book.pos.items()):
            px = prices.get(t)
            info = sc.get(t)
            if px is None:
                continue
            s = info["score"] if info else 0.0
            news = info["news"] if info else 0.0
            pnl = p.pnl_pct(px)
            held = day_idx - p.open_day
            exit_act = "SELL" if p.side == "LONG" else "COVER"
            reason, prio = None, None
            if p.side == "LONG":
                stop = cfg.stop_loss
                if pnl <= -stop:
                    reason, prio = f"stop-loss {pnl:+.1%}", 0
                elif p.peak / p.avg - 1.0 >= 0.03 and px <= p.peak * (1 - cfg.trail_stop):
                    reason, prio = f"trailing stop from {p.peak:.2f}", 0
                elif news <= -0.5:
                    reason, prio = f"negative news shock ({news:+.2f})", 0
                elif pnl >= cfg.take_profit and s < 0.25:
                    reason, prio = f"take-profit {pnl:+.1%}, signal faded", 1
            else:
                if pnl <= -SHORT_STOP:
                    reason, prio = f"short stop-loss {pnl:+.1%}", 0
                elif (p.avg / p.trough - 1.0) >= 0.03 and px >= p.trough * (1 + cfg.trail_stop * 0.7):
                    reason, prio = f"short trailing stop from {p.trough:.2f}", 0
                elif news >= 0.5:
                    reason, prio = f"positive news shock vs short ({news:+.2f})", 0
                elif pnl >= cfg.take_profit and s > -0.25:
                    reason, prio = f"short take-profit {pnl:+.1%}", 1
                elif final_stretch:
                    reason, prio = "end-of-simulation unwind of short", 1
            if reason is None and held >= cfg.min_hold_days:
                # signal reversal, with tax drag: reluctance to realise a taxable gain
                tax_drag = 0.0
                if pnl > 0 and book.realized + p.pnl(px) > 0:
                    tax_drag = min(0.25, cfg.tax_drag_k * cfg.stcg_rate * pnl * 3)
                adj = s if p.side == "LONG" else -s
                if adj < cfg.exit_thr - tax_drag:
                    reason, prio = f"signal faded ({s:+.2f}, tax drag {tax_drag:.2f})", 1
                # tax-loss harvesting (Sections 70/71): realised loss offsets realised gains
                if reason is None and pnl <= -cfg.harvest_loss_pct and book.realized > 0 and adj < 0.15:
                    reason, prio = f"tax-loss harvest {pnl:+.1%} vs realised gains {book.realized:,.0f}", 2
            if reason is None and p.side == "LONG" and wealth > 0 and p.qty * px > 1.35 * cfg.max_weight * wealth:
                excess = p.qty * px - cfg.max_weight * wealth
                q = int(excess // px)
                if q * px >= cfg.min_notional:
                    intents.append(Intent(t, "SELL", q, px, 2, s, "trim over-concentration"))
                continue
            if reason:
                planned_exit.add(t)
                intents.append(Intent(t, exit_act, p.qty, px, prio, s, reason))

        # ---------------- entries ----------------
        if not no_new and not blocked:
            deployable = min(book.cash - cfg.cash_reserve * wealth,
                             cfg.gross_cap * wealth - book.exposure(prices))
            n_long = len([1 for t, p in book.pos.items() if p.side == "LONG" and t not in planned_exit])
            n_short = len([1 for t, p in book.pos.items() if p.side == "SHORT" and t not in planned_exit])
            can_short = (cfg.allow_shorts and self.calendar.days_to_settlement(date) >= 3)
            cands = sorted(((t, i) for t, i in sc.items() if t not in book.pos
                            and book.cooldown.get(t, -1) < day_idx and t in prices),
                           key=lambda x: -abs(x[1]["score"]))
            for t, i in cands:
                s, px, vol = i["score"], prices[t], i["vol"]
                if abs(s) < cfg.entry_thr or deployable < cfg.min_notional:
                    continue
                side = "LONG" if s > 0 else "SHORT"
                if side == "LONG" and (n_long >= cfg.max_longs):
                    continue
                if side == "SHORT":
                    if not can_short or n_short >= cfg.max_shorts or i["news"] > 0.1 or s > -cfg.entry_thr - 0.05:
                        continue
                wmax = cfg.max_weight if side == "LONG" else SHORT_MAX_WEIGHT
                strength = max(0.4, min(1.0, abs(s) / 0.6))
                vol_scale = max(0.4, min(1.3, cfg.target_daily_vol / vol))
                target = wealth * wmax * strength * vol_scale
                target = min(target, wealth * wmax, deployable - cfg.brokerage)
                qty = int(target // px)
                if qty < 1 or qty * px < cfg.min_notional:
                    continue
                qty = min(qty, cfg.max_qty)
                act = "BUY" if side == "LONG" else "SHORT"
                intents.append(Intent(t, act, qty, px, 3, s, f"{side.lower()} entry score {s:+.2f} "
                                      f"(trend {i['trend']:+.2f}, mom {i['mom']:+.2f}, news {i['news']:+.2f})"))
                deployable -= qty * px + cfg.brokerage
                n_long += side == "LONG"
                n_short += side == "SHORT"

            # add-on to winners that still carry a strong signal and are under-sized
            for t, p in book.pos.items():
                if p.side != "LONG" or t in planned_exit or t not in sc or t not in prices:
                    continue
                i, px = sc[t], prices[t]
                if i["score"] < cfg.entry_thr + 0.1 or p.pnl_pct(px) < 0.01:
                    continue
                tgt = wealth * cfg.max_weight * max(0.4, min(1.0, i["score"] / 0.6))
                gap = tgt - p.qty * px
                if gap > cfg.resize_band * tgt and gap > cfg.min_notional and gap < deployable:
                    q = int(gap // px)
                    if q >= 1:
                        intents.append(Intent(t, "BUY", q, px, 4, i["score"], "add to strong winner"))
                        deployable -= q * px + cfg.brokerage

        intents.sort(key=lambda x: (x.priority, -abs(x.score)))
        return self._cap(intents)

    def _cap(self, intents):
        cap = self.cfg.max_orders_per_day
        forced = [i for i in intents if i.priority == 0]
        rest = [i for i in intents if i.priority != 0]
        return (forced + rest[:max(0, cap - len(forced))])[:max(cap, len(forced))]
