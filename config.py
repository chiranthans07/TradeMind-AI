"""Central configuration. Everything is overridable by environment variable."""
import os
from dataclasses import dataclass


def _env(name, default=None, cast=str):
    v = os.environ.get(name)
    if v is None or v == "":
        return default
    try:
        if cast is bool:
            return v.strip().lower() in ("1", "true", "yes", "y", "on")
        return cast(v)
    except Exception:
        return default


def load_dotenv(path=".env"):
    """Tiny .env loader (no dependency)."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


class Config:
    """Read env at construction time (so load_dotenv() can run first)."""

    def __init__(self):
        # ---- connectivity (PDF sections 3 & 5) ----
        self.server_ip = _env("SERVER_IP", "127.0.0.1")
        self.api_port = _env("API_PORT", 3002, int)
        self.kafka_port = _env("KAFKA_PORT", 9094, int)
        self.token = _env("BOT_ACCESS_TOKEN", "")
        self.team = _env("TEAM_USERNAME", "team")
        self.api_base_override = _env("API_BASE", "")
        self.kafka_override = _env("KAFKA_BOOTSTRAP", "")
        self.topic_ticks = "market.ticks"
        self.topic_news = "market.news"
        self.num_consumers = min(10, max(1, _env("NUM_CONSUMERS", 4, int)))

        # ---- platform rules (PDF sections 5, 7, 8) ----
        self.start_capital = 1_000_000.0
        self.brokerage = 20.0
        self.dp_charge = 15.34
        self.stcg_rate = 0.15
        self.max_qty = 1_000_000
        self.rate_limit_per_min = _env("RATE_LIMIT_PER_MIN", 100, int)  # hard cap 120
        self.rate_penalty_s = 31.0
        self.short_settlement_days = 10

        # ---- stage gating ----
        self.allow_dev_trading = _env("ALLOW_DEV_TRADING", False, bool)
        self.stage_poll_s = 2.5

        # ---- day batching / latency (~3-4 s per day, news at +150 ms) ----
        self.batch_wait_s = _env("BATCH_WAIT_S", 0.55, float)
        self.batch_news_grace_s = _env("BATCH_NEWS_GRACE_S", 0.30, float)
        self.max_orders_per_day = _env("MAX_ORDERS_PER_DAY", 6, int)
        self.decision_deadline_s = 2.6

        # ---- indicators ----
        self.history_days = 200
        self.min_history = 25

        # ---- sizing / risk ----
        self.max_weight = 0.12
        self.max_longs = 8
        self.max_shorts = 3
        self.gross_cap = 0.85
        self.cash_reserve = 0.10
        self.min_notional = 25_000.0
        self.resize_band = 0.30
        self.entry_thr = 0.35
        self.exit_thr = 0.08
        self.flip_thr = 0.50
        self.stop_loss = 0.055
        self.trail_stop = 0.07
        self.take_profit = 0.14
        self.cooldown_days = 2
        self.min_hold_days = 2
        self.target_daily_vol = 0.015
        self.allow_shorts = _env("ALLOW_SHORTS", True, bool)
        self.cover_shorts_before_end_days = 1
        self.unwind_longs_at_end = False

        # ---- tax awareness ----
        self.tax_drag_k = 1.2
        self.harvest_loss_pct = 0.03

        self.log_level = _env("LOG_LEVEL", "INFO")
        self.log_dir = _env("LOG_DIR", "logs")

    @property
    def api_base(self):
        return self.api_base_override or f"http://{self.server_ip}:{self.api_port}/api/v1"

    @property
    def kafka_bootstrap(self):
        return self.kafka_override or f"{self.server_ip}:{self.kafka_port}"

    @property
    def group_id(self):
        return f"group-{self.team}"
