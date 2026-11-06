"""Run with:  python -m unittest discover -s tests -v   (no Kafka / network needed)"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.api import _extract_error
from bot.config import Config
from bot.portfolio import Portfolio, parse_portfolio
from bot.ratelimit import RateLimiter
from bot.sentiment import score
from bot.strategy import ShortCalendar
from run_backtest import run


class TestSentiment(unittest.TestCase):
    def test_direction(self):
        self.assertGreater(score("ALPHACORP Q3 NET PROFIT SURGES 24% YOY"), 0.5)
        self.assertLess(score("SEBI IMPOSES PENALTY ON BETATECH"), -0.5)
        self.assertLess(score("CEO RESIGNS WITH IMMEDIATE EFFECT"), 0)
        self.assertLess(abs(score("COMPANY ANNOUNCES AGM DATE")), 0.1)

    def test_denied_rumour_is_not_bullish(self):
        self.assertLess(score("Rumour of takeover bid denied"), 0.1)


class TestPortfolio(unittest.TestCase):
    def test_round_trip_and_fees(self):
        c = Config()
        p = Portfolio(c)
        p.apply_fill("BUY", "X", 100, 100.0, 1)
        p.apply_fill("SELL", "X", 100, 110.0, 3)
        self.assertAlmostEqual(p.cash, c.start_capital + 1000 - 20 - 20 - 15.34, places=2)
        self.assertAlmostEqual(p.realized, 1000.0)

    def test_short_margin_and_cover(self):
        c = Config()
        p = Portfolio(c)
        p.apply_fill("SHORT", "Y", 50, 200.0, 1)
        self.assertAlmostEqual(p.cash, c.start_capital - 10000 - 20)       # 100% margin locked
        p.apply_fill("COVER", "Y", 50, 190.0, 2)
        self.assertAlmostEqual(p.realized, 500.0)
        self.assertAlmostEqual(p.cash, c.start_capital + 500 - 40)

    def test_parse_portfolio_shapes(self):
        a = parse_portfolio({"cash": 5.0, "holdings": [{"ticker": "A", "quantity": 3, "avg_price": 10}]})
        self.assertEqual(a["longs"]["A"], (3, 10.0))
        b = parse_portfolio({"portfolio": {"available_cash": 7, "positions": [
            {"symbol": "B", "qty": 2, "side": "SHORT", "average_price": 9}]}})
        self.assertEqual(b["shorts"]["B"], (2, 9.0))
        self.assertIsNone(parse_portfolio({"status": "ok"}))


class TestMisc(unittest.TestCase):
    def test_error_codes(self):
        self.assertEqual(_extract_error({"error": "INSUFFICIENT_CAPITAL"})[0], "INSUFFICIENT_CAPITAL")
        self.assertEqual(_extract_error({"error": {"code": "ACCOUNT_BLOCKED", "message": "x"}})[0], "ACCOUNT_BLOCKED")
        self.assertEqual(_extract_error({"code": "rate_limited"})[0], "RATE_LIMITED")

    def test_rate_limiter_caps(self):
        rl = RateLimiter(per_min=5)
        got = [rl.acquire(timeout=0.01) for _ in range(8)]
        self.assertEqual(sum(got), 5)

    def test_short_calendar(self):
        cal = ShortCalendar()
        self.assertTrue(cal.is_settlement("2021-04-16"))        # a Friday
        self.assertFalse(cal.is_settlement("2021-04-15"))       # Thursday
        cal.learn("2021-04-16")
        self.assertFalse(cal.is_settlement("2021-04-23"))       # alternate Friday
        self.assertTrue(cal.is_settlement("2021-04-30"))


class TestBacktest(unittest.TestCase):
    def test_no_rejections_and_bounded_drawdown(self):
        for seed in (1, 2, 3):
            r = run(seed)
            self.assertEqual(r["rejections"], 0, r)
            self.assertLess(r["max_dd_pct"], 8.0, r)


if __name__ == "__main__":
    unittest.main()
