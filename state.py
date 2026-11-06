"""Per-ticker market state: OHLCV history, news memory, cross-sectional helpers."""
import datetime as dt
import math
import threading

import numpy as np

from . import indicators as ind
from . import sentiment


class MarketState:
    NEWS_DECAY = 0.55          # per-trading-day multiplicative decay of a headline's impact

    def __init__(self, max_len=400):
        self.max_len = max_len
        self.universe = []
        self.names = {}
        self.sectors = {}
        self.dates = {}        # ticker -> [date str]
        self.o, self.h, self.l, self.c, self.v = {}, {}, {}, {}, {}
        self.news = {}         # ticker -> list[(date, score, headline)]
        self.last_date = None
        self._lock = threading.RLock()

    # ------------------------------------------------------------ universe
    def set_universe(self, stocks):
        """stocks: list of dicts from GET /stocks (ticker/symbol/name/description)."""
        with self._lock:
            uni = []
            for s in stocks:
                t = s.get("ticker") or s.get("symbol")
                if not t:
                    continue
                uni.append(t)
                self.names[t] = s.get("name", t)
                self.sectors[t] = s.get("description", "")
                for d in (self.dates, self.o, self.h, self.l, self.c, self.v):
                    d.setdefault(t, [])
                self.news.setdefault(t, [])
            self.universe = uni

    def ensure_ticker(self, t):
        with self._lock:
            if t not in self.c:
                for d in (self.dates, self.o, self.h, self.l, self.c, self.v):
                    d[t] = []
                self.news[t] = []
                if t not in self.universe:
                    self.universe.append(t)

    def reset_history(self):
        """Used when the dev sandbox loops back to Day 1."""
        with self._lock:
            for t in list(self.c):
                for d in (self.dates, self.o, self.h, self.l, self.c, self.v):
                    d[t] = []
                self.news[t] = []
            self.last_date = None

    # ------------------------------------------------------------ data in
    def add_bar(self, t, date, o, h, l, c, v):
        with self._lock:
            self.ensure_ticker(t)
            ds = self.dates[t]
            if ds and date < ds[-1]:
                return False                      # stale / out-of-order
            if ds and date == ds[-1]:             # replace same-day bar
                for arr, val in ((self.o, o), (self.h, h), (self.l, l), (self.c, c), (self.v, v)):
                    arr[t][-1] = val
                return True
            ds.append(date)
            for arr, val in ((self.o, o), (self.h, h), (self.l, l), (self.c, c), (self.v, v)):
                arr[t].append(val)
            if len(ds) > self.max_len:
                for arr in (self.dates, self.o, self.h, self.l, self.c, self.v):
                    arr[t] = arr[t][-self.max_len:]
            return True

    def load_history(self, t, rows):
        """Warm-start from GET /stocks/:ticker/history rows (dicts, oldest->newest)."""
        n = 0
        for r in rows:
            try:
                if self.add_bar(t, r["date"], float(r["open"]), float(r["high"]),
                                float(r["low"]), float(r["close"]), float(r.get("volume", 0))):
                    n += 1
            except (KeyError, TypeError, ValueError):
                continue
        return n

    def add_news(self, t, date, headline, description=""):
        s = sentiment.score(headline, description)
        with self._lock:
            self.ensure_ticker(t)
            self.news[t].append((date, s, headline))
            self.news[t] = self.news[t][-30:]
        return s

    # ------------------------------------------------------------ queries
    def n_bars(self, t):
        return len(self.c.get(t, []))

    def last_close(self, t):
        c = self.c.get(t)
        return c[-1] if c else None

    def arrays(self, t):
        with self._lock:
            return (np.array(self.o[t]), np.array(self.h[t]), np.array(self.l[t]),
                    np.array(self.c[t]), np.array(self.v[t]))

    def news_score(self, t, today):
        """Decayed sum of headline scores; age measured in calendar days / ~1.4 (trading days)."""
        tot = 0.0
        for d, s, _ in self.news.get(t, []):
            age = _age_days(d, today)
            if age < 0 or age > 6:
                continue
            tot += s * (self.NEWS_DECAY ** age)
        return max(-1.0, min(1.0, tot))

    def todays_news(self, t, today):
        return [x for x in self.news.get(t, []) if x[0] == today]

    # ------------------------------------------------------------ market-wide
    def cross_section_z(self, n=5):
        with self._lock:
            moms = {}
            for t in self.universe:
                c = self.c.get(t, [])
                if len(c) > n + 1 and c[-n - 1] > 0:
                    moms[t] = c[-1] / c[-n - 1] - 1.0
        if len(moms) < 4:
            return {}
        arr = np.array(list(moms.values()))
        mu, sd = arr.mean(), arr.std() or 1e-9
        return {t: float((m - mu) / sd) for t, m in moms.items()}

    def market_regime(self):
        """Equal-weight index built from the universe. Returns dict(reg, vol_ratio, dd5)."""
        with self._lock:
            rets = []
            for t in self.universe:
                c = np.array(self.c.get(t, [])[-61:])
                if len(c) > 30:
                    rets.append(np.diff(np.log(np.clip(c, 1e-9, None)))[-60:])
        if len(rets) < 3:
            return {"reg": 0.0, "vol_ratio": 1.0, "dd5": 0.0}
        m = min(len(r) for r in rets)
        idx_ret = np.mean([r[-m:] for r in rets], axis=0)
        idx = np.exp(np.cumsum(idx_ret))
        fast, slow = ind.ema(idx, 8), ind.ema(idx, 21)
        vol = float(idx_ret.std()) or 1e-4
        reg = math.tanh((fast / slow - 1.0) / (vol * 3.0))
        recent_vol = float(idx_ret[-5:].std()) if len(idx_ret) >= 5 else vol
        dd5 = float(idx[-1] / idx[-6] - 1.0) if len(idx) > 6 else 0.0
        return {"reg": reg, "vol_ratio": recent_vol / vol, "dd5": dd5}


def _parse(d):
    try:
        return dt.date.fromisoformat(str(d)[:10])
    except ValueError:
        return None


def _age_days(d, today):
    a, b = _parse(d), _parse(today)
    if not a or not b:
        return 0
    return int(round((b - a).days / 1.4))
