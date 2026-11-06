"""REST client for the order gateway (PDF section 5).

Endpoints used (all under http://<SERVER_IP>:3002/api/v1):
  POST /orders              GET /orders
  GET  /portfolio           GET /portfolio/cash
  GET  /stocks              GET /stocks/:ticker/history
  GET  /stage (public)      GET /simulation/clock (public)
  POST /bot/heartbeat
"""
import logging
import threading
import time

import requests

from .ratelimit import HIGH, LOW, RateLimiter

log = logging.getLogger("api")


class ApiResult:
    __slots__ = ("ok", "status", "data", "code", "message")

    def __init__(self, ok, status, data, code=None, message=None):
        self.ok, self.status, self.data, self.code, self.message = ok, status, data, code, message

    def __repr__(self):
        return f"ApiResult(ok={self.ok}, status={self.status}, code={self.code}, msg={self.message})"


class ApiClient:
    def __init__(self, cfg, limiter=None):
        self.cfg = cfg
        self.base = cfg.api_base.rstrip("/")
        self.limiter = limiter or RateLimiter(cfg.rate_limit_per_min, cfg.rate_penalty_s)
        # public endpoints (/stage, /simulation/clock) carry no token, so they should not eat the
        # per-token 120 req/min budget; they get their own small limiter instead.
        self.pub_limiter = RateLimiter(60, cfg.rate_penalty_s)
        self._local = threading.local()          # one keep-alive Session per thread
        self.stats = {"requests": 0, "errors": 0, "orders_ok": 0, "orders_rejected": 0}
        self._stats_lock = threading.Lock()

    # ------------------------------------------------------------------ low level
    def _session(self):
        s = getattr(self._local, "s", None)
        if s is None:
            s = requests.Session()
            s.headers.update({"Content-Type": "application/json"})
            if self.cfg.token:
                s.headers["Authorization"] = f"Bearer {self.cfg.token}"
            self._local.s = s
        return s

    def _bump(self, key):
        with self._stats_lock:
            self.stats[key] = self.stats.get(key, 0) + 1

    def _request(self, method, path, priority=HIGH, json=None, params=None, timeout=3.0, auth=True,
                 max_wait=None):
        lim = self.limiter if auth else self.pub_limiter
        if not lim.acquire(priority, timeout=(timeout + 1.0) if max_wait is None else max_wait):
            return ApiResult(False, 0, None, "LOCAL_RATE_WAIT", "rate limiter wait exceeded")
        url = f"{self.base}{path}"
        self._bump("requests")
        try:
            sess = self._session()
            headers = None if auth else {"Authorization": ""}
            r = sess.request(method, url, json=json, params=params, timeout=timeout,
                             headers=headers)
        except requests.RequestException as e:
            self._bump("errors")
            return ApiResult(False, 0, None, "NETWORK", str(e))
        try:
            data = r.json()
        except ValueError:
            data = {"raw": r.text[:300]}
        if r.status_code == 429:
            lim.penalize()
            self._bump("errors")
            return ApiResult(False, 429, data, "RATE_LIMITED", "HTTP 429 - penalty box")
        code, msg = _extract_error(data)
        ok = 200 <= r.status_code < 300 and not (isinstance(data, dict) and (
            data.get("status") in ("error", "rejected", "failed") or data.get("success") is False))
        if not ok:
            self._bump("errors")
            code = code or f"HTTP_{r.status_code}"
        return ApiResult(ok, r.status_code, data, code if not ok else None, msg if not ok else None)

    # ------------------------------------------------------------------ public
    def stage(self):
        return self._request("GET", "/stage", auth=False, timeout=2.0)

    def clock(self):
        return self._request("GET", "/simulation/clock", auth=False, timeout=2.0)

    def heartbeat(self, seq):
        return self._request("POST", "/bot/heartbeat", json={"seq": seq}, timeout=2.5)

    def stocks(self):
        return self._request("GET", "/stocks", priority=LOW, timeout=5.0)

    def history(self, ticker, days=200):
        return self._request("GET", f"/stocks/{ticker}/history", priority=LOW,
                             params={"days": days}, timeout=6.0)

    def portfolio(self, priority=HIGH):
        return self._request("GET", "/portfolio", priority=priority, timeout=3.0)

    def cash(self):
        return self._request("GET", "/portfolio/cash", timeout=2.5)

    def orders(self):
        return self._request("GET", "/orders", priority=LOW, timeout=4.0)

    def place_order(self, ticker, action, quantity, date=None):
        body = {"ticker": ticker, "action": action, "quantity": int(quantity)}
        if date:
            body["date"] = date
        # an order that cannot get a rate-limit slot within ~1 s is stale (fills at today's close only)
        res = self._request("POST", "/orders", json=body, timeout=3.0, max_wait=1.0)
        if res.code == "LOCAL_RATE_WAIT":
            self._bump("orders_dropped_local")
        else:
            self._bump("orders_ok" if res.ok else "orders_rejected")
        return res


def _extract_error(data):
    """Pull (code, message) out of whatever error shape the gateway uses."""
    if not isinstance(data, dict):
        return None, None
    err = data.get("error")
    code = data.get("code") or data.get("error_code") or data.get("reason_code")
    msg = data.get("message") or data.get("detail") or data.get("reason")
    if isinstance(err, dict):
        code = code or err.get("code")
        msg = msg or err.get("message")
    elif isinstance(err, str):
        if err.isupper() and "_" in err or err.isupper():
            code = code or err
        else:
            msg = msg or err
    if code:
        code = str(code).upper()
    return code, msg
