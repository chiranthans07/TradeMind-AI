"""Lightweight, dependency-free news sentiment scorer.

Corporate headlines are short and formulaic, so a weighted lexicon + event-type
rules (earnings / regulatory / M&A / dividends / management / rating) is fast
(microseconds, far inside the 3-4 s window) and fully explainable to the jury.

score(headline, description) -> float in [-1, 1].
"""
import re

# event rules: (regex, weight). Evaluated on lower-cased text; strongest hits dominate.
EVENT_RULES = [
    (r"\b(fraud|scam|accounting irregularit|insolvency|bankrupt|default(s|ed)?|delist|ponzi)\b", -1.00),
    (r"\b(sebi|regulator|regulatory)\b.*\b(fine|penalty|ban|probe|notice|action)\b", -0.80),
    (r"\b(fine[sd]?|penalt(y|ies))\b", -0.60),
    (r"\b(raid|investigation|probe|lawsuit|litigation|sued)\b", -0.60),
    (r"\b(profit warning|guidance cut|cuts? guidance|lowers? guidance|misses?|missed)\b", -0.70),
    (r"\b(net )?(profit|earnings|revenue|sales)\b.*\b(fall|falls|fell|drop|drops|dropped|plunge|plunges|decline[sd]?|slump|slumps|tumble[sd]?|down)\b", -0.65),
    (r"\b(loss|losses|net loss)\b", -0.50),
    (r"\b(downgrade[sd]?|sell rating|underperform)\b", -0.55),
    (r"\b(resign(s|ed|ation)?|steps? down|ceo exit|cfo exit|quits?)\b", -0.45),
    (r"\b(layoffs?|job cuts?|plant (closure|shutdown)|recall|outage|breach|hack)\b", -0.50),
    (r"\b(delay(s|ed)?|halt(s|ed)?|suspend(s|ed)?|cancel(s|led)?)\b", -0.35),
    (r"\b(rumou?r)\b.*\b(acqui|merger|takeover|buyout|bid)\b", 0.25),   # M&A rumour: mild +, volatile
    (r"\b(acquir(e|es|ed|ition)|merger|takeover|buyout|stake (buy|purchase))\b", 0.45),
    (r"\b(net )?(profit|earnings|revenue|sales|ebitda)\b.*\b(surge[sd]?|jump[sd]?|soar[sd]?|rise[sn]?|rose|grow[sn]?|up|beat[s]?|record|climb[sd]?)\b", 0.70),
    (r"\b(beats?|beat estimates|tops? estimates|record (profit|revenue|sales|exports|high))\b", 0.65),
    (r"\b(upgrade[sd]?|buy rating|outperform|raises? guidance|guidance (raise|hike))\b", 0.55),
    (r"\b(dividend|bonus issue|buyback|share repurchase)\b", 0.35),
    (r"\b(order win|bags? order|wins? (contract|order)|new contract|partnership|approval|approved|launch(es|ed)?|expansion)\b", 0.40),
    (r"\b(new ceo|appoints?)\b", 0.05),
    (r"\b(inclusion|added to) .*index\b", 0.30),
    (r"\b(index (exclusion|removal)|removed from .*index)\b", -0.30),
]
_COMPILED = [(re.compile(p), w) for p, w in EVENT_RULES]

POS_WORDS = {"surge": .5, "soar": .5, "record": .4, "strong": .3, "growth": .3, "beat": .4, "profit": .2,
             "gain": .3, "rally": .35, "robust": .3, "upbeat": .35, "boost": .3, "win": .3, "wins": .3,
             "bullish": .4, "recovery": .3, "exceed": .3, "exceeds": .3, "optimistic": .3}
NEG_WORDS = {"plunge": -.6, "slump": -.5, "weak": -.35, "decline": -.4, "fall": -.35, "falls": -.35,
             "loss": -.45, "miss": -.5, "cut": -.3, "cuts": -.3, "concern": -.3, "risk": -.15,
             "bearish": -.4, "crisis": -.6, "scandal": -.8, "probe": -.5, "warning": -.45,
             "pressure": -.25, "disappoint": -.5, "disappoints": -.5, "slowdown": -.4}
NEGATORS = {"no", "not", "denies", "denied", "denial", "dismisses", "refutes", "false", "rejects"}
_TOK = re.compile(r"[a-z]+")


def score(headline, description=""):
    text = f"{headline or ''}. {description or ''}".lower()
    head = (headline or "").lower()
    hits = []
    for rx, w in _COMPILED:
        if rx.search(head):
            hits.append(w * 1.0)
        elif rx.search(text):
            hits.append(w * 0.6)             # description-only hit counts less
    rule = 0.0
    if hits:
        strongest = max(hits, key=abs)
        others = sum(h for h in hits if h is not strongest)
        rule = strongest + 0.25 * others
    toks = _TOK.findall(head) + [t for t in _TOK.findall((description or "").lower())][:40]
    lex = 0.0
    for i, t in enumerate(toks):
        v = POS_WORDS.get(t, 0.0) + NEG_WORDS.get(t, 0.0)
        if v and any(n in toks[max(0, i - 3):i] for n in NEGATORS):
            v = -v * 0.5
        lex += v
    lex = max(-0.6, min(0.6, lex * 0.35))
    total = 0.75 * rule + 0.25 * lex if hits else lex
    # a *denied/refuted* story (e.g. "takeover rumour denied") loses most of its impact
    if hits and any(w in NEGATORS for w in _TOK.findall(head)):
        total *= -0.3
    # magnitude pulse: ALL-CAPS wire headlines ("SURGES 24%") scale with the quoted percentage
    m = re.search(r"(\d+(?:\.\d+)?)\s?%", head)
    if m and total:
        pct = float(m.group(1))
        total *= 1.0 + min(0.5, pct / 60.0)
    return max(-1.0, min(1.0, total))
