"""Pure-numpy technical indicators. All functions take arrays ordered oldest -> newest."""
import numpy as np


def sma(x, n):
    if len(x) < n:
        return float(np.mean(x)) if len(x) else float("nan")
    return float(np.mean(x[-n:]))


def ema_series(x, n):
    x = np.asarray(x, dtype=float)
    if len(x) == 0:
        return x
    a = 2.0 / (n + 1.0)
    out = np.empty_like(x)
    out[0] = x[0]
    for i in range(1, len(x)):
        out[i] = a * x[i] + (1 - a) * out[i - 1]
    return out


def ema(x, n):
    s = ema_series(x, n)
    return float(s[-1]) if len(s) else float("nan")


def rsi(close, n=14):
    c = np.asarray(close, dtype=float)
    if len(c) < n + 1:
        return 50.0
    d = np.diff(c)
    up, dn = np.clip(d, 0, None), np.clip(-d, 0, None)
    au, ad = up[:n].mean(), dn[:n].mean()
    for i in range(n, len(d)):               # Wilder smoothing
        au = (au * (n - 1) + up[i]) / n
        ad = (ad * (n - 1) + dn[i]) / n
    if ad == 0:
        return 100.0 if au > 0 else 50.0
    return float(100.0 - 100.0 / (1.0 + au / ad))


def macd(close, fast=12, slow=26, sig=9):
    c = np.asarray(close, dtype=float)
    if len(c) < slow + sig:
        return 0.0, 0.0, 0.0
    line = ema_series(c, fast) - ema_series(c, slow)
    signal = ema_series(line, sig)
    return float(line[-1]), float(signal[-1]), float(line[-1] - signal[-1])


def bollinger_pctb(close, n=20, k=2.0):
    c = np.asarray(close, dtype=float)
    if len(c) < n:
        return 0.5
    w = c[-n:]
    m, s = w.mean(), w.std()
    if s == 0:
        return 0.5
    lo, hi = m - k * s, m + k * s
    return float((c[-1] - lo) / (hi - lo))


def atr(high, low, close, n=14):
    h, l, c = (np.asarray(a, dtype=float) for a in (high, low, close))
    if len(c) < 2:
        return 0.0
    tr = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    return float(tr[-n:].mean())


def log_returns(close):
    c = np.asarray(close, dtype=float)
    if len(c) < 2:
        return np.array([])
    return np.diff(np.log(np.clip(c, 1e-9, None)))


def realized_vol(close, n=20):
    r = log_returns(close)
    if len(r) < 3:
        return 0.02
    return float(max(r[-n:].std(), 1e-4))


def momentum(close, n):
    c = np.asarray(close, dtype=float)
    if len(c) <= n or c[-n - 1] <= 0:
        return 0.0
    return float(c[-1] / c[-n - 1] - 1.0)


def volume_ratio(volume, n=20):
    v = np.asarray(volume, dtype=float)
    if len(v) < 5:
        return 1.0
    base = v[-n - 1:-1].mean() if len(v) > n else v[:-1].mean()
    return float(v[-1] / base) if base > 0 else 1.0


def trend_strength(close, n=20):
    """R^2-weighted slope of log price (signed). Roughly 'how clean is the trend'."""
    c = np.log(np.clip(np.asarray(close, dtype=float)[-n:], 1e-9, None))
    if len(c) < 8:
        return 0.0
    x = np.arange(len(c))
    slope, icpt = np.polyfit(x, c, 1)
    fit = slope * x + icpt
    ss_res = float(((c - fit) ** 2).sum())
    ss_tot = float(((c - c.mean()) ** 2).sum()) or 1e-12
    r2 = max(0.0, 1.0 - ss_res / ss_tot)
    return float(slope * r2)
