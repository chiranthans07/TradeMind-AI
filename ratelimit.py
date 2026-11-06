"""Sliding-window rate limiter with priorities and a 429 penalty box.

PDF: 120 requests/min per team token, HTTP 429 => 30 s penalty box.
We stay at ~100/min. Orders & heartbeats are HIGH priority; bulk history
warm-up is LOW priority and yields when the window is busy.
"""
import threading
import time
from collections import deque

HIGH, LOW = 0, 1


class RateLimiter:
    def __init__(self, per_min=100, penalty_s=31.0, low_reserve=0.35):
        self.per_min = per_min
        self.penalty_s = penalty_s
        self.low_reserve = low_reserve
        self._stamps = deque()
        self._lock = threading.Lock()
        self._penalty_until = 0.0

    def penalize(self):
        with self._lock:
            self._penalty_until = time.time() + self.penalty_s

    def in_penalty(self):
        return time.time() < self._penalty_until

    def _prune(self, now):
        while self._stamps and now - self._stamps[0] > 60.0:
            self._stamps.popleft()

    def acquire(self, priority=HIGH, timeout=None):
        """Block until a slot is free. Returns False on timeout."""
        t0 = time.time()
        while True:
            with self._lock:
                now = time.time()
                self._prune(now)
                cap = self.per_min if priority == HIGH else int(self.per_min * (1 - self.low_reserve))
                if now >= self._penalty_until and len(self._stamps) < cap:
                    self._stamps.append(now)
                    return True
                wait = 0.05
                if now < self._penalty_until:
                    wait = min(0.5, self._penalty_until - now)
                elif self._stamps:
                    wait = max(0.02, min(0.5, 60.0 - (now - self._stamps[0]) + 0.01))
            if timeout is not None and time.time() - t0 + wait > timeout:
                return False
            time.sleep(wait)

    def used(self):
        with self._lock:
            self._prune(time.time())
            return len(self._stamps)
