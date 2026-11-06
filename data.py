"""Synthetic market + news generator for offline testing (masked tickers, like the contest)."""
import datetime as dt
import math
import random

NEWS_TEMPLATES_POS = [
    ("{t} Q{q} NET PROFIT SURGES {p}% YOY", "Company announces record exports and declares interim dividend."),
    ("{t} BEATS ESTIMATES, RAISES FULL-YEAR GUIDANCE", "Management upbeat on demand outlook."),
    ("{t} WINS LARGE ORDER WORTH RS {p}0 CRORE", "New contract expands order book."),
    ("BROKERS UPGRADE {t} TO BUY", "Analysts cite strong earnings growth."),
]
NEWS_TEMPLATES_NEG = [
    ("{t} Q{q} NET PROFIT FALLS {p}% YOY", "Weak demand and margin pressure weigh on results."),
    ("SEBI IMPOSES PENALTY ON {t} FOR DISCLOSURE LAPSES", "Regulator notice issued."),
    ("{t} CEO RESIGNS WITH IMMEDIATE EFFECT", "Board begins search for successor."),
    ("{t} CUTS GUIDANCE AS DEMAND SLOWS", "Profit warning issued."),
]
NEWS_TEMPLATES_NEUTRAL = [
    ("{t} ANNOUNCES AGM DATE", "Annual general meeting scheduled."),
    ("{t} APPOINTS NEW COMPANY SECRETARY", "Routine filing."),
]


def business_days(start, n):
    d, out = start, []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += dt.timedelta(days=1)
    return out


def generate(n_stocks=20, n_hist=120, n_sim=30, seed=7, start="2021-01-04"):
    rng = random.Random(seed)
    names = ["ALPHACORP", "BETATECH", "GAMMAFIN", "DELTAPHARMA", "EPSILONOIL", "ZETAAUTO", "ETAPOWER",
             "THETAINFRA", "IOTAFMCG", "KAPPAMETALS", "LAMBDARETAIL", "MUTELECOM", "NUCEMENT", "XIREALTY",
             "OMICRONSTEEL", "PIBANK", "RHOPAINTS", "SIGMAMEDIA", "TAUCHEM", "UPSILONLOG",
             "PHIENERGY", "CHIFOODS", "PSIAIR", "OMEGATEXT"][:n_stocks]
    total = n_hist + n_sim
    dates = business_days(dt.date.fromisoformat(start), total)
    # market factor with regimes
    mkt, reg = [], 1
    for i in range(total):
        if rng.random() < 0.04:
            reg = -reg
        mkt.append(0.0006 * reg + rng.gauss(0, 0.007))
    data = {n: [] for n in names}
    news = {d: [] for d in dates}
    for n in names:
        px = rng.uniform(80, 2500)
        beta, vol = rng.uniform(0.6, 1.4), rng.uniform(0.008, 0.02)
        base_vol = rng.randint(200_000, 3_000_000)
        drift_boost = [0.0] * (total + 6)
        for i, d in enumerate(dates):
            # news event -> drift over next 3 days (the "signal" that the bot can exploit)
            if i > 30 and rng.random() < 0.045:
                good = rng.random() < 0.5
                tpl = rng.choice(NEWS_TEMPLATES_POS if good else NEWS_TEMPLATES_NEG)
                news[d].append({"ticker": n,
                                "headline": tpl[0].format(t=n, q=rng.randint(1, 4), p=rng.randint(5, 30)),
                                "description": tpl[1], "date": d})
                for k in range(1, 4):
                    drift_boost[i + k] += (0.011 if good else -0.011) / k
            elif i > 30 and rng.random() < 0.03:
                tpl = rng.choice(NEWS_TEMPLATES_NEUTRAL)
                news[d].append({"ticker": n, "headline": tpl[0].format(t=n), "description": tpl[1], "date": d})
            r = beta * mkt[i] + drift_boost[i] + rng.gauss(0, vol)
            o = px * (1 + rng.gauss(0, vol * 0.3))
            c = px * math.exp(r)
            hi = max(o, c) * (1 + abs(rng.gauss(0, vol * 0.4)))
            lo = min(o, c) * (1 - abs(rng.gauss(0, vol * 0.4)))
            v = int(base_vol * (1 + 2.0 * abs(r) / vol * 0.3) * rng.uniform(0.7, 1.3))
            data[n].append({"date": d, "open": round(o, 2), "high": round(hi, 2), "low": round(lo, 2),
                            "close": round(c, 2), "volume": v})
            px = c
    return {"names": names, "dates": dates, "bars": data, "news": news, "n_hist": n_hist, "n_sim": n_sim}
