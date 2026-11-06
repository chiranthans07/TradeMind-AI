"""Kafka ingestion (PDF section 3).

* PLAINTEXT, broker <SERVER_IP>:9094, topics market.ticks + market.news
* group_id = group-<team>  (required so the Pre-Flight tracker sees the consumer)
* auto_offset_reset='latest', enable_auto_commit=False  (no stale backlog on reconnect)
* up to 10 consumer threads (topics have 10 partitions) -> partitions are balanced by Kafka
"""
import json
import logging
import threading
import time

log = logging.getLogger("feed")


class KafkaFeed:
    def __init__(self, cfg, engine):
        self.cfg, self.engine = cfg, engine
        self.threads = []
        self.stop_evt = threading.Event()
        self.counts = {"ticks": 0, "news": 0, "errors": 0}

    def start(self):
        for n in range(self.cfg.num_consumers):
            th = threading.Thread(target=self._run, args=(n,), name=f"kafka-{n}", daemon=True)
            th.start()
            self.threads.append(th)
        log.info("Started %d Kafka consumers in %s on %s", self.cfg.num_consumers,
                 self.cfg.group_id, self.cfg.kafka_bootstrap)

    def stop(self):
        self.stop_evt.set()

    def _run(self, n):
        from kafka import KafkaConsumer          # imported lazily so tests run without kafka-python
        cfg = self.cfg
        backoff = 1.0
        while not self.stop_evt.is_set():
            consumer = None
            try:
                consumer = KafkaConsumer(
                    cfg.topic_ticks, cfg.topic_news,
                    bootstrap_servers=cfg.kafka_bootstrap,
                    group_id=cfg.group_id,
                    auto_offset_reset="latest",
                    enable_auto_commit=False,
                    security_protocol="PLAINTEXT",
                    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                    max_poll_records=200,
                    fetch_max_wait_ms=50,
                    session_timeout_ms=15000,
                    heartbeat_interval_ms=3000,
                    consumer_timeout_ms=1000,
                )
                log.info("consumer-%d connected", n)
                backoff = 1.0
                while not self.stop_evt.is_set():
                    for msg in consumer:
                        self._dispatch(msg)
                        if self.stop_evt.is_set():
                            break
            except Exception as e:                       # network drop, rebalance, broker restart...
                self.counts["errors"] += 1
                log.warning("consumer-%d error: %s - reconnecting in %.0fs", n, e, backoff)
                time.sleep(backoff)
                backoff = min(10.0, backoff * 2)
            finally:
                try:
                    if consumer:
                        consumer.close()
                except Exception:
                    pass

    def _dispatch(self, msg):
        try:
            val = msg.value
            if msg.topic == self.cfg.topic_ticks:
                self.counts["ticks"] += 1
                self.engine.on_tick(val)
            elif msg.topic == self.cfg.topic_news:
                self.counts["news"] += 1
                self.engine.on_news(val)
        except Exception:
            self.counts["errors"] += 1
            log.exception("bad message skipped")
