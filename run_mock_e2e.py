#!/usr/bin/env python3
"""End-to-end rehearsal WITHOUT Kafka: real HTTP ApiClient + threaded engine against a mock gateway.

Replays messages in exactly the JSON shapes from the problem statement (preflight_tick, price_tick at T+0,
news_flash at T+150ms) so heartbeat, stage gating, batching, latency and order dispatch are all exercised.

    python run_mock_e2e.py --interval 1.2 --days 20
"""
import argparse
import json
import logging
import threading
import time

from bot.api import ApiClient
from bot.config import Config
from bot.engine import Engine
from sim.data import generate
from sim.exchange import START_CAPITAL, Simulator
from sim.mock_http import make_server


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=1.2, help="seconds per simulated day")
    ap.add_argument("--days", type=int, default=20)
    ap.add_argument("--seed", type=int, default=9)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)-8s %(message)s")

    ds = generate(n_stocks=20, n_sim=a.days, seed=a.seed)
    sim = Simulator(ds)
    srv = make_server(sim, token="TEST_TOKEN")
    port = srv.server_address[1]

    cfg = Config()
    cfg.api_base_override = f"http://127.0.0.1:{port}/api/v1"
    cfg.token = "TEST_TOKEN"
    cfg.stage_poll_s = 1.0
    api = ApiClient(cfg)
    eng = Engine(cfg, api)
    eng.start()
    threading.Thread(target=eng.stage_loop, daemon=True).start()

    # ---- pre-flight: heartbeats + stock discovery + warm start
    sim.set_stage("preflight", "RUNNING")
    t_end = time.time() + 6
    seq = 0
    boot = threading.Thread(target=eng.bootstrap, daemon=True)
    boot.start()
    while time.time() < t_end:
        seq += 1
        eng.on_tick({"event": "preflight_tick", "event_type": "preflight_tick", "seq": seq,
                     "data": {"status": "STANDBY", "seq": seq}})
        time.sleep(1.0)
    boot.join(timeout=30)
    print(f"preflight: heartbeats ACKed = {sim.heartbeats}, universe = {len(eng.state.universe)}, "
          f"warm bars/ticker = {eng.state.n_bars(ds['names'][0])}")

    # ---- live simulation replay
    sim.set_stage("simulation", "RUNNING")
    time.sleep(0.6)
    for k in range(a.days):
        sim.advance()
        d = sim.today()
        for n in ds["names"]:
            r = ds["bars"][n][sim.cursor]
            eng.on_tick({"event_type": "price_tick", "simulated_date": d, "timestamp": "x",
                         "data": {"ticker": n, **{kk: r[kk] for kk in ("open", "high", "low", "close", "volume")}}})
        time.sleep(0.15)
        for nw in ds["news"][d]:
            eng.on_news({"event_type": "news_flash", "simulated_date": d, "data": nw})
        time.sleep(a.interval - 0.15)
    time.sleep(1.5)
    w = sim.wealth()
    print("\n=== E2E RESULT ===")
    print(f"wealth {w:,.0f}  ({(w / START_CAPITAL - 1) * 100:+.2f}%)  orders={len(sim.orders)}  rejections={sim.rejections}")
    print(json.dumps(eng.summary(), indent=1, default=str))
    eng.stop()
    srv.shutdown()


if __name__ == "__main__":
    main()
