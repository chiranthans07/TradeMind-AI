"""In-memory exchange that mimics the contest gateway (PDF sections 5, 7, 8).

Execution at the day's close, flat INR 20 brokerage, INR 15.34 DP on SELL, 100% margin lock for
SHORT, 10-day Friday short square-off with auto long liquidation, negative-balance block,
rejection codes, anti-lookahead history, stage/clock endpoints, heartbeat.
"""
import datetime as dt
import threading

START_CAPITAL = 1_000_000.0


class Simulator:
    def __init__(self, dataset, brokerage=20.0, dp=15.34, stcg=0.15):
        self.ds = dataset
        self.dates = dataset["dates"]
        self.first_sim = dataset["n_hist"]
        self.names = dataset["names"]
        self.brokerage, self.dp, self.stcg = brokerage, dp, stcg
        self.lock = threading.RLock()
        self.cursor = self.first_sim - 1          # index of the day currently being played
        self.stage, self.status = "development", "IDLE"
        self.cash = START_CAPITAL
        self.longs, self.shorts = {}, {}          # ticker -> [qty, avg]
        self.realized = 0.0
        self.tax_paid = 0.0
        self.orders = []
        self.rejections = 0
        self.heartbeats = []
        self.verified = True
        self.blocked = False
        self.settle_parity = 0                     # ISO-week parity of square-off Fridays
        self.wealth_curve = []

    # ------------------------------------------------------------------ clock
    def set_stage(self, stage, status):
        self.stage, self.status = stage, status

    def today(self):
        return self.dates[self.cursor]

    def close(self, t, i=None):
        return self.ds["bars"][t][self.cursor if i is None else i]["close"]

    def advance(self):
        """Move to the next trading day; run settlement. Returns False when dataset exhausted."""
        with self.lock:
            if self.cursor + 1 >= len(self.dates):
                self.status = "COMPLETED"
                return False
            self.cursor += 1
            d = dt.date.fromisoformat(self.today())
            if d.weekday() == 4 and d.isocalendar()[1] % 2 == self.settle_parity:
                self._settle_shorts()
            self.wealth_curve.append(self.wealth())
            return True

    def _settle_shorts(self):
        for t, (q, avg) in list(self.shorts.items()):
            px = self.close(t)
            pnl = (avg - px) * q
            self.cash += q * avg + pnl
            self.realized += pnl
            del self.shorts[t]
        if self.cash < 0:                          # liquidate most profitable longs first
            ranked = sorted(self.longs.items(), key=lambda kv: -(self.close(kv[0]) / kv[1][1] - 1))
            for t, (q, avg) in ranked:
                if self.cash >= 0:
                    break
                px = self.close(t)
                self.cash += q * px - self.brokerage - self.dp
                self.realized += (px - avg) * q
                del self.longs[t]
        self.blocked = self.cash < 0

    # ------------------------------------------------------------------ valuation
    def wealth(self):
        lv = sum(q * self.close(t) for t, (q, a) in self.longs.items())
        se = sum(q * a + (a - self.close(t)) * q for t, (q, a) in self.shorts.items())
        return self.cash + lv + se

    # ------------------------------------------------------------------ endpoints
    def api_stage(self):
        return 200, {"stage": self.stage, "status": self.status, "has_run": True, "simulation_id": "sim_mock",
                     "interval_seconds": 4, "total_days": len(self.dates) - self.first_sim,
                     "days_completed": max(0, self.cursor - self.first_sim + 1), "current_date": self.today()}

    def api_clock(self):
        s = self.api_stage()[1]
        return 200, {"current_day": s["days_completed"], "total_days": s["total_days"],
                     "remaining_days": s["total_days"] - s["days_completed"], "stage": self.stage,
                     "status": self.status}

    def api_stocks(self):
        return 200, {"status": "ok", "stocks": [{"ticker": n, "symbol": n, "name": n.title(),
                                                  "description": "mock company"} for n in self.names]}

    def api_history(self, ticker, days=365):
        if ticker not in self.ds["bars"]:
            return 404, {"status": "error", "error": "INVALID_TICKER"}
        with self.lock:
            rows = self.ds["bars"][ticker][:self.cursor]          # strictly < current date
        cur = dt.date.fromisoformat(self.today())
        cutoff = (cur - dt.timedelta(days=int(days))).isoformat()
        rows = [r for r in rows if r["date"] >= cutoff]
        return 200, {"status": "ok", "ticker": ticker, "current_simulation_date": self.today(),
                     "requested_days": days, "total_records": len(rows), "prices": rows}

    def api_heartbeat(self, seq):
        self.heartbeats.append(seq)
        return 200, {"status": "ok", "seq": seq}

    def api_portfolio(self):
        with self.lock:
            return 200, {"status": "ok", "cash": round(self.cash, 2),
                         "holdings": [{"ticker": t, "quantity": q, "avg_price": a} for t, (q, a) in self.longs.items()],
                         "short_positions": [{"ticker": t, "quantity": q, "avg_price": a, "side": "SHORT"}
                                             for t, (q, a) in self.shorts.items()],
                         "total_wealth": round(self.wealth(), 2)}

    def api_cash(self):
        return 200, {"status": "ok", "cash": round(self.cash, 2)}

    def _reject(self, code):
        self.rejections += 1
        return 400, {"status": "error", "error": code, "message": code}

    def api_order(self, body):
        with self.lock:
            if not self.verified:
                return self._reject("ACCOUNT_NOT_VERIFIED")
            if self.stage != "simulation" and not getattr(self, "allow_dev_orders", False) or self.status != "RUNNING":
                return self._reject("SIMULATION_NOT_RUNNING")
            t, act, q = body.get("ticker"), str(body.get("action", "")).upper(), body.get("quantity")
            if t not in self.ds["bars"]:
                return self._reject("INVALID_TICKER")
            if not isinstance(q, int) or isinstance(q, bool) or q < 1 or q > 1_000_000:
                return self._reject("INVALID_QUANTITY")
            if act not in ("BUY", "SELL", "SHORT", "COVER"):
                return self._reject("INVALID_ACTION")
            px = self.close(t)
            fee = self.brokerage
            if act in ("BUY", "SHORT") and self.blocked:
                return self._reject("ACCOUNT_BLOCKED")
            if act == "BUY":
                if q * px + fee > self.cash:
                    return self._reject("INSUFFICIENT_CAPITAL")
                if t in self.shorts:                               # auto-cover first
                    sq, sa = self.shorts[t]
                    c = min(q, sq)
                    pnl = (sa - px) * c
                    self.cash += c * sa + pnl
                    self.realized += pnl
                    if c == sq:
                        del self.shorts[t]
                    else:
                        self.shorts[t][0] -= c
                    q -= c
                self.cash -= fee
                if q > 0:
                    self.cash -= q * px
                    lq, la = self.longs.get(t, [0, 0.0])
                    self.longs[t] = [lq + q, (la * lq + px * q) / (lq + q)]
            elif act == "SELL":
                lq, la = self.longs.get(t, [0, 0.0])
                if lq < q:
                    return self._reject("INSUFFICIENT_HOLDINGS")
                self.cash += q * px - fee - self.dp
                self.realized += (px - la) * q
                if lq == q:
                    del self.longs[t]
                else:
                    self.longs[t][0] -= q
            elif act == "SHORT":
                if q * px + fee > self.cash:
                    return self._reject("INSUFFICIENT_MARGIN")
                self.cash -= q * px + fee
                sq, sa = self.shorts.get(t, [0, 0.0])
                self.shorts[t] = [sq + q, (sa * sq + px * q) / (sq + q)]
            elif act == "COVER":
                if t not in self.shorts or self.shorts[t][0] < q:
                    return self._reject("NO_SHORT_POSITION")
                sq, sa = self.shorts[t]
                pnl = (sa - px) * q
                self.cash += q * sa + pnl - fee
                self.realized += pnl
                if sq == q:
                    del self.shorts[t]
                else:
                    self.shorts[t][0] -= q
            rec = {"ticker": t, "action": act, "quantity": body.get("quantity"), "price": px,
                   "date": self.today(), "client_date": body.get("date")}
            self.orders.append(rec)
            return 200, {"status": "ok", "order": dict(rec, status="FILLED"), "fill_price": px}
