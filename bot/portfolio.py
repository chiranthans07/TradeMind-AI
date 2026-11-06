"""Local portfolio book + Indian-tax-aware realized P&L ledger.

The exchange is the source of truth. We keep a local mirror so we can size orders
without spending API calls (120 req/min is tight at ~3.5 s/day), and we
reconcile against GET /portfolio periodically.
"""
import logging
import threading
from collections import deque

log = logging.getLogger("portfolio")


class Position:
    __slots__ = ("ticker", "side", "qty", "avg", "open_day", "peak", "trough", "lots")

    def __init__(self, ticker, side, qty, avg, open_day):
        self.ticker, self.side, self.qty, self.avg, self.open_day = ticker, side, qty, avg, open_day
        self.peak = avg
        self.trough = avg
        self.lots = deque([[qty, avg, open_day]])   # FIFO tax lots for longs

    def pnl_pct(self, price):
        if self.avg <= 0:
            return 0.0
        return (price / self.avg - 1.0) if self.side == "LONG" else (1.0 - price / self.avg)

    def pnl(self, price):
        return (price - self.avg) * self.qty if self.side == "LONG" else (self.avg - price) * self.qty


class Portfolio:
    def __init__(self, cfg, cash=None):
        self.cfg = cfg
        self.cash = cfg.start_capital if cash is None else cash
        self.pos = {}                     # ticker -> Position
        self.realized = 0.0               # net realized capital P&L this run (gross of friction)
        self.fees_paid = 0.0
        self.tax_paid_est = 0.0
        self.cooldown = {}                # ticker -> day_idx until which we do not re-enter
        self.lock = threading.RLock()
        self.last_sync = 0

    # --------------------------------------------------------------- accounting
    def longs(self):
        return {t: p for t, p in self.pos.items() if p.side == "LONG"}

    def shorts(self):
        return {t: p for t, p in self.pos.items() if p.side == "SHORT"}

    def long_value(self, prices):
        return sum(p.qty * prices.get(t, p.avg) for t, p in self.longs().items())

    def short_notional(self, prices):
        return sum(p.qty * prices.get(t, p.avg) for t, p in self.shorts().items())

    def short_equity(self, prices):
        """Margin locked + unrealised short P&L."""
        return sum(p.qty * p.avg + p.pnl(prices.get(t, p.avg)) for t, p in self.shorts().items())

    def wealth(self, prices):
        return self.cash + self.long_value(prices) + self.short_equity(prices)

    def exposure(self, prices):
        return self.long_value(prices) + self.short_notional(prices)

    def tax_estimate(self):
        return max(0.0, self.realized) * self.cfg.stcg_rate

    # --------------------------------------------------------------- fills
    def apply_fill(self, action, ticker, qty, price, day):
        c = self.cfg
        with self.lock:
            p = self.pos.get(ticker)
            if action == "BUY":
                # BUY on a short position first covers it (platform rule) - we avoid that path,
                # but mirror it anyway.
                if p and p.side == "SHORT":
                    cover = min(qty, p.qty)
                    self._close(p, cover, price, day, fee_dp=False)
                    qty -= cover
                    if qty <= 0:
                        return
                self.cash -= qty * price + c.brokerage
                self.fees_paid += c.brokerage
                if p and p.side == "LONG":
                    tot = p.qty + qty
                    p.avg = (p.avg * p.qty + price * qty) / tot
                    p.qty = tot
                    p.lots.append([qty, price, day])
                else:
                    self.pos[ticker] = Position(ticker, "LONG", qty, price, day)
            elif action == "SELL":
                if p and p.side == "LONG":
                    self._close(p, min(qty, p.qty), price, day, fee_dp=True)
            elif action == "SHORT":
                self.cash -= qty * price + c.brokerage           # 100% margin locked
                self.fees_paid += c.brokerage
                if p and p.side == "SHORT":
                    tot = p.qty + qty
                    p.avg = (p.avg * p.qty + price * qty) / tot
                    p.qty = tot
                else:
                    self.pos[ticker] = Position(ticker, "SHORT", qty, price, day)
            elif action == "COVER":
                if p and p.side == "SHORT":
                    self._close(p, min(qty, p.qty), price, day, fee_dp=False)

    def _close(self, p, qty, price, day, fee_dp):
        c = self.cfg
        gross = p.pnl(price) / p.qty * qty if p.qty else 0.0
        if p.side == "LONG":
            self.cash += qty * price - c.brokerage - (c.dp_charge if fee_dp else 0.0)
            left = qty
            while left > 0 and p.lots:
                lot = p.lots[0]
                take = min(left, lot[0])
                lot[0] -= take
                left -= take
                if lot[0] <= 0:
                    p.lots.popleft()
        else:
            self.cash += qty * p.avg + gross - c.brokerage       # release margin + settle P&L
        self.fees_paid += c.brokerage + (c.dp_charge if fee_dp else 0.0)
        self.realized += gross
        p.qty -= qty
        if p.qty <= 0:
            self.pos.pop(p.ticker, None)

    def update_marks(self, prices):
        for t, p in self.pos.items():
            px = prices.get(t)
            if px is None:
                continue
            p.peak = max(p.peak, px)
            p.trough = min(p.trough, px)

    # --------------------------------------------------------------- server sync
    def sync_from_server(self, data, day):
        """Reconcile with GET /portfolio. Tolerant of several response shapes."""
        parsed = parse_portfolio(data)
        if parsed is None:
            return False
        with self.lock:
            if parsed["cash"] is not None:
                self.cash = parsed["cash"]
            seen = set()
            for side, items in (("LONG", parsed["longs"]), ("SHORT", parsed["shorts"])):
                for t, (q, avg) in items.items():
                    if q <= 0:
                        continue
                    seen.add(t)
                    p = self.pos.get(t)
                    if p and p.side == side:
                        p.qty = q
                        if avg and avg > 0:
                            p.avg = avg
                    else:
                        self.pos[t] = Position(t, side, q, avg or 0.0, day)
            for t in list(self.pos):
                if t not in seen:
                    log.info("sync: %s no longer held on server (forced square-off/liquidation?)", t)
                    self.pos.pop(t, None)
            self.last_sync = day
        return True


def _num(d, *keys):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] is not None:
            try:
                return float(d[k])
            except (TypeError, ValueError):
                continue
    return None


def parse_portfolio(data):
    """Return {'cash', 'longs': {t:(qty,avg)}, 'shorts': {t:(qty,avg)}} or None."""
    if not isinstance(data, dict):
        return None
    root = data.get("portfolio") if isinstance(data.get("portfolio"), dict) else data
    cash = _num(root, "cash", "available_cash", "cash_balance", "availableCash")
    if cash is None and isinstance(root.get("cash"), dict):
        cash = _num(root["cash"], "available", "available_cash", "balance")
    longs, shorts = {}, {}

    def ingest(items, default_side):
        if isinstance(items, dict):
            items = [dict(v, ticker=k) if isinstance(v, dict) else {"ticker": k, "quantity": v}
                     for k, v in items.items()]
        for it in items or []:
            if not isinstance(it, dict):
                continue
            t = it.get("ticker") or it.get("symbol")
            q = _num(it, "quantity", "qty", "shares", "units")
            avg = _num(it, "avg_price", "average_price", "avg_cost", "avg_buy_price",
                       "entry_price", "avgPrice", "average_cost") or 0.0
            if not t or q is None:
                continue
            side = str(it.get("side") or it.get("type") or it.get("position_type") or default_side).upper()
            side = "SHORT" if "SHORT" in side else "LONG" if "LONG" in side or default_side == "LONG" else default_side
            (shorts if side == "SHORT" else longs)[t] = (int(q), avg)

    for key in ("holdings", "positions", "stocks", "long_positions", "longs", "long_holdings"):
        if key in root:
            ingest(root[key], "LONG")
    for key in ("short_positions", "shorts", "short_holdings"):
        if key in root:
            ingest(root[key], "SHORT")
    if cash is None and not longs and not shorts:
        return None
    return {"cash": cash, "longs": longs, "shorts": shorts}
