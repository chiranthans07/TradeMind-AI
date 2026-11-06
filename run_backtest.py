#!/usr/bin/env python3
"""Offline back-test on synthetic data using the in-process exchange simulator.

    python run_backtest.py                   # default seed
    python run_backtest.py --seeds 1 2 3 4 5 # robustness across market paths
"""
import argparse
import logging
import statistics
import sys
import time

from bot.config import Config
from bot.engine import DayBatch, Engine
from bot.ratelimit import RateLimiter
from sim.data import generate
from sim.exchange import START_CAPITAL, Simulator


class _Res:
    def __init__(self, status, data):
        self.status, self.data = status, data
        self.ok = 200 <= status < 300 and data.get("status") != "error"
        self.code = data.get("error") if not self.ok else None
        self.message = data.get("message") if not self.ok else None


class InProcessApi:
    """Same surface as bot.api.ApiClient, but talks to the Simulator directly."""

    def __init__(self, sim):
        self.sim = sim
        self.limiter = RateLimiter(10**9)
        self.stats = {"requests": 0, "errors": 0, "orders_ok": 0, "orders_rejected": 0}

    def stocks(self): return _Res(*self.sim.api_stocks())
    def history(self, t, days=200): return _Res(*self.sim.api_history(t, days))
    def portfolio(self, priority=0): return _Res(*self.sim.api_portfolio())
    def cash(self): return _Res(*self.sim.api_cash())
    def stage(self): return _Res(*self.sim.api_stage())
    def heartbeat(self, seq): return _Res(*self.sim.api_heartbeat(seq))

    def place_order(self, ticker, action, quantity, date=None):
        r = _Res(*self.sim.api_order({"ticker": ticker, "action": action, "quantity": quantity, "date": date}))
        self.stats["orders_ok" if r.ok else "orders_rejected"] += 1
        return r


def run(seed, n_stocks=20, n_sim=30, verbose=False, shorts=True):
    cfg = Config()
    cfg.allow_shorts = shorts
    ds = generate(n_stocks=n_stocks, n_sim=n_sim, seed=seed)
    sim = Simulator(ds)
    sim.set_stage("simulation", "RUNNING")
    api = InProcessApi(sim)
    eng = Engine(cfg, api)
    eng.sim_mode = True
    eng.bootstrap(warm=True)
    total = n_sim
    t0 = time.time()
    first_dates = None
    for k in range(total):
        sim.advance()
        d = sim.today()
        b = DayBatch(d)
        for n in ds["names"]:
            r = ds["bars"][n][sim.cursor]
            b.ticks[n] = (r["open"], r["high"], r["low"], r["close"], r["volume"])
        b.news = [(x["ticker"], x["headline"], x["description"]) for x in ds["news"][d]]
        eng.stage.update({"stage": "simulation", "status": "RUNNING", "total_days": total, "days_completed": k})
        eng.latest_date = d
        eng.process_batch(b)
    # final mark
    wealth = sim.wealth()
    curve = sim.wealth_curve
    rets = [curve[i] / curve[i - 1] - 1 for i in range(1, len(curve))]
    sharpe = (statistics.mean(rets) / statistics.pstdev(rets) * (252 ** 0.5)) if len(rets) > 2 and statistics.pstdev(rets) > 0 else 0
    peak, mdd = curve[0], 0.0
    for w in curve:
        peak = max(peak, w)
        mdd = max(mdd, 1 - w / peak)
    # equal-weight buy&hold benchmark
    bench = sum(ds["bars"][n][sim.cursor]["close"] / ds["bars"][n][ds["n_hist"] - 1]["close"] for n in ds["names"]) / len(ds["names"]) - 1
    return {"seed": seed, "wealth": round(wealth), "ret_pct": round((wealth / START_CAPITAL - 1) * 100, 2),
            "bench_ret_pct": round(bench * 100, 2), "sharpe": round(sharpe, 2), "max_dd_pct": round(mdd * 100, 2),
            "orders": len(sim.orders), "rejections": sim.rejections,
            "fees": round(eng.book.fees_paid), "est_tax": round(eng.book.tax_estimate()),
            "no_trade_days": eng.metrics["days_no_trade"], "final_cash": round(sim.cash)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, nargs="+", default=[7])
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--no-shorts", action="store_true")
    ap.add_argument("-v", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO if a.v else logging.WARNING, format="%(levelname)s %(name)s %(message)s")
    rows = [run(s, n_sim=a.days, shorts=not a.no_shorts) for s in a.seeds]
    for r in rows:
        print(r)
    if len(rows) > 1:
        print("mean ret %.2f%% | mean bench %.2f%% | worst DD %.2f%% | total rejections %d" % (
            statistics.mean(r["ret_pct"] for r in rows), statistics.mean(r["bench_ret_pct"] for r in rows),
            max(r["max_dd_pct"] for r in rows), sum(r["rejections"] for r in rows)))
