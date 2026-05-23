import unittest

from paper_bot import (
    BookTop,
    DirectionalConfig,
    PaperPosition,
    parse_json_list,
    parse_slug,
    should_enter,
    should_exit,
    slug_with_offset,
    url_or_slug_with_offset,
)


def cfg(**overrides):
    base = dict(
        token_id="token",
        label="test",
        market_slug="test-market",
        outcome="Up",
        target_price=100.0,
        direction="UP",
        size_usd=1.0,
        entry_min=0.65,
        entry_max=0.72,
        take_profit=0.85,
        stop_loss=0.55,
        hard_exit=0.50,
        late_seconds=30,
        min_distance_usd=10.0,
        poll=1,
        seconds=60,
    )
    base.update(overrides)
    return DirectionalConfig(**base)


def book(bid=None, ask=None):
    return BookTop(bid=bid, bid_size=10, ask=ask, ask_size=10, last=None, raw={})


class StrategyTests(unittest.TestCase):
    def test_parse_slug_from_url(self):
        self.assertEqual(
            parse_slug("https://polymarket.com/event/btc-updown-5m-1779549600"),
            "btc-updown-5m-1779549600",
        )

    def test_parse_json_list_from_gamma_string(self):
        self.assertEqual(parse_json_list('["Up", "Down"]'), ["Up", "Down"])

    def test_slug_with_offset(self):
        self.assertEqual(slug_with_offset("btc-updown-5m-1779550500", 300, 2), "btc-updown-5m-1779551100")

    def test_url_or_slug_with_offset(self):
        self.assertEqual(
            url_or_slug_with_offset("https://polymarket.com/event/btc-updown-5m-1779550500", 300, 1),
            "https://polymarket.com/event/btc-updown-5m-1779550800",
        )

    def test_enter_when_ask_in_band_and_target_far(self):
        ok, reason = should_enter(cfg(), book(ask=0.70), btc_price=115.0)
        self.assertTrue(ok)
        self.assertIn("entry_band", reason)

    def test_no_enter_when_target_too_close(self):
        ok, reason = should_enter(cfg(), book(ask=0.70), btc_price=105.0)
        self.assertFalse(ok)
        self.assertIn("distance_too_close", reason)

    def test_exit_take_profit(self):
        pos = PaperPosition(entry_ts=1, entry_price=0.70, shares=1.42, size_usd=1.0, reason="test")
        ok, reason = should_exit(cfg(), pos, book(bid=0.86), btc_price=115.0, remaining_seconds=120)
        self.assertTrue(ok)
        self.assertEqual(reason, "take_profit")

    def test_late_exit_when_target_close(self):
        pos = PaperPosition(entry_ts=1, entry_price=0.70, shares=1.42, size_usd=1.0, reason="test")
        ok, reason = should_exit(cfg(), pos, book(bid=0.70), btc_price=103.0, remaining_seconds=20)
        self.assertTrue(ok)
        self.assertTrue(reason.startswith("late_exit"))


if __name__ == "__main__":
    unittest.main()
