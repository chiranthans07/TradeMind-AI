#!/usr/bin/env python3
"""MANTHAN 2026 bot entry point.

    python run_bot.py                # live (venue Wi-Fi, .env filled in)
    python run_bot.py --sandbox      # also place orders during the development stage loop
"""
import argparse
import logging
import os
import signal
import sys
import threading
import time

from bot.api import ApiClient
from bot.config import Config, load_dotenv
from bot.engine import Engine
from bot.feed import KafkaFeed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sandbox", action="store_true", help="trade during the development stage too")
    ap.add_argument("--no-warm", action="store_true", help="skip /history warm-start")
    args = ap.parse_args()

    load_dotenv()
    cfg = Config()
    if args.sandbox:
        cfg.allow_dev_trading = True
    os.makedirs(cfg.log_dir, exist_ok=True)
    logging.basicConfig(level=getattr(logging, cfg.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)-7s %(name)-9s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout),
                                  logging.FileHandler(os.path.join(cfg.log_dir, "bot.log"))])
    log = logging.getLogger("main")
    if not cfg.token:
        log.error("BOT_ACCESS_TOKEN is empty - copy it from Step 1 of the Participant Guide into .env")
        sys.exit(1)
    log.info("API %s | Kafka %s | group %s | dev-trading=%s", cfg.api_base, cfg.kafka_bootstrap,
             cfg.group_id, cfg.allow_dev_trading)

    api = ApiClient(cfg)
    eng = Engine(cfg, api, trade_log_path=os.path.join(cfg.log_dir, "orders.jsonl"))
    eng.poll_stage_once()
    eng.start()
    feed = KafkaFeed(cfg, eng)
    feed.start()                         # start consuming first so the pre-flight tracker sees us
    threading.Thread(target=eng.stage_loop, name="stage", daemon=True).start()
    try:
        eng.bootstrap(warm=not args.no_warm)
    except Exception:
        log.exception("bootstrap failed - will retry on simulation start")

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    last_report = 0
    while not stop.is_set():
        time.sleep(0.5)
        if time.time() - last_report > 30:
            last_report = time.time()
            log.info("status: %s | kafka %s | api-window %d/min", eng.summary(), feed.counts, api.limiter.used())
            if not eng.state.universe:
                try:
                    eng.bootstrap(warm=False)
                except Exception:
                    pass
    feed.stop()
    eng.stop()
    log.info("final: %s", eng.summary())


if __name__ == "__main__":
    main()
