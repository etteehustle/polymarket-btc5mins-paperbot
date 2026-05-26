from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import call, patch

import paper_bot
from paper_bot import (
    BookTop,
    complete_set_taker_fee,
    DASHBOARD_HTML,
    DEFAULT_CHAIN_COUNT,
    DEFAULT_POLL_SECONDS,
    DirectionalConfig,
    auto_entry_precheck,
    ClockSnapshot,
    current_dashboard_payload,
    dashboard_payload,
    empty_dashboard_db_path,
    extract_resolution_source,
    extract_target_price,
    fetch_reference_price,
    IntervalGate,
    initial_dashboard_db_path,
    init_db,
    latest_run_db_path,
    new_run_db_path,
    PaperPosition,
    past_results_target_url,
    pending_market_metadata,
    fee_rate_from_base_fee,
    PolymarketClock,
    normalize_bot_config,
    normalize_presets,
    OrderBookState,
    PolymarketMarketWsClient,
    parse_json_list,
    parse_slug,
    rtds_symbol_from_resolution_source,
    rtds_price_snapshot_from_payload,
    should_enter,
    should_exit,
    target_lookup_window_open,
    target_price_from_event_page_html,
    target_price_from_past_results_payload,
    taker_fee_usdc,
    strategy_label,
    slug_with_offset,
    write_market_context,
    upsert_active_position,
    url_or_slug_with_offset,
)


def cfg(**overrides):
    base = dict(
        token_id="token",
        label="test",
        preset_key="safe",
        preset_name="An toan",
        market_slug="test-market",
        outcome="Up",
        target_price=100.0,
        direction="UP",
        size_usd=1.0,
        entry_min=0.65,
        entry_max=0.72,
        take_profit=0.85,
        stop_loss=0.55,
        trail_start=0.0,
        trail_distance=0.0,
        late_seconds=30,
        entry_window_seconds=None,
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

    def test_extract_resolution_source_from_polymarket_metadata(self):
        event = {"eventMetadata": '{"resolutionSource":"https://data.chain.link/streams/btc-usd"}'}
        self.assertEqual(extract_resolution_source(event, {}), "https://data.chain.link/streams/btc-usd")

    def test_resolution_source_maps_to_polymarket_rtds_symbol(self):
        self.assertEqual(rtds_symbol_from_resolution_source("https://data.chain.link/streams/btc-usd"), "btc/usd")
        self.assertEqual(rtds_symbol_from_resolution_source("https://data.chain.link/streams/eth-usd"), "eth/usd")
        self.assertIsNone(rtds_symbol_from_resolution_source("https://example.com/streams/btc-usd"))

    def test_reference_price_uses_polymarket_rtds_chainlink_stream(self):
        with patch("paper_bot.fetch_polymarket_rtds_snapshot", return_value=paper_bot.PriceSnapshot(77123.45, 1, 1, "test")) as fetch_price:
            price = fetch_reference_price("https://data.chain.link/streams/btc-usd", timeout=3.0)
        self.assertAlmostEqual(price, 77123.45)
        fetch_price.assert_called_once_with("btc/usd", timeout=3.0, max_age_ms=paper_bot.DEFAULT_REFERENCE_PRICE_STALE_MS)

    def test_rtds_price_snapshot_uses_subscription_backfill(self):
        snapshot = rtds_price_snapshot_from_payload(
            {
                "symbol": "btc/usd",
                "data": [
                    {"timestamp": 1_779_804_100_000, "value": 77138.7},
                    {"timestamp": 1_779_804_101_000, "value": 77146.5},
                ],
            },
            "btc/usd",
            received_ms=1_779_804_102_000,
        )

        self.assertIsNotNone(snapshot)
        self.assertAlmostEqual(snapshot.price, 77146.5)
        self.assertEqual(snapshot.timestamp_ms, 1_779_804_101_000)
        self.assertEqual(snapshot.received_ms, 1_779_804_102_000)

    def test_rtds_client_snapshot_waits_until_timeout_deadline(self):
        client = paper_bot.PolymarketRtdsPriceClient("btc/usd")
        client.start = lambda: None

        def publish() -> None:
            time.sleep(0.1)
            with client.lock:
                client.price = 77123.0
                client.timestamp_ms = 1_779_804_101_000
                client.received_ms = paper_bot.now_ms()
                client.ready.set()

        thread = threading.Thread(target=publish)
        thread.start()
        try:
            snapshot = client.snapshot(timeout=1.0, max_age_ms=5_000)
        finally:
            thread.join(timeout=1.0)

        self.assertAlmostEqual(snapshot.price, 77123.0)

    def test_runtime_source_no_longer_uses_binance(self):
        source = Path(paper_bot.__file__).read_text(encoding="utf-8")
        self.assertNotIn("api.binance.com", source)
        self.assertNotIn("api.dataengine.chain.link", source)

    def test_slug_with_offset(self):
        self.assertEqual(slug_with_offset("btc-updown-5m-1779550500", 300, 2), "btc-updown-5m-1779551100")

    def test_url_or_slug_with_offset(self):
        self.assertEqual(
            url_or_slug_with_offset("https://polymarket.com/event/btc-updown-5m-1779550500", 300, 1),
            "https://polymarket.com/event/btc-updown-5m-1779550800",
        )

    def test_updown_5m_slug_defines_trading_end(self):
        self.assertEqual(paper_bot.updown_5m_start_ts("btc-updown-5m-1779550500"), 1779550500)
        self.assertEqual(paper_bot.updown_5m_end_ts("btc-updown-5m-1779550500"), 1779550800)

    def test_default_end_ts_prefers_updown_5m_slug_end(self):
        market = paper_bot.MarketInfo(
            slug="btc-updown-5m-1000",
            title="BTC Up or Down",
            end_ts=5_000,
            start_ts=1_000,
            target_price=77000.0,
            resolution_source="https://data.chain.link/streams/btc-usd",
            outcomes=[paper_bot.Outcome("Up", "up"), paper_bot.Outcome("Down", "down")],
        )

        self.assertEqual(paper_bot.default_end_ts(market, requested_seconds=None, end_buffer_seconds=1), 1_301)

    def test_clob_book_error_after_market_end_stops_watch_cleanly(self):
        exc = ValueError("Polymarket CLOB websocket book unavailable: stale=up,down; last_error=Polymarket websocket closed unexpectedly")

        self.assertTrue(paper_bot.should_end_market_after_book_error(exc, now_ts=1_300, market_end_ts=1_300))
        self.assertFalse(paper_bot.should_end_market_after_book_error(exc, now_ts=1_299, market_end_ts=1_300))

    def test_market_ws_client_stop_closes_active_socket(self):
        class FakeSock:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        client = PolymarketMarketWsClient(["token"])
        sock = FakeSock()
        with client.lock:
            client.sock = sock

        client.stop()

        self.assertTrue(client.stop_event.is_set())
        self.assertTrue(sock.closed)

    def test_enter_when_ask_in_band_and_target_far(self):
        ok, reason = should_enter(cfg(), book(ask=0.70), btc_price=115.0, remaining_seconds=120)
        self.assertTrue(ok)
        self.assertIn("entry_band", reason)

    def test_no_enter_when_target_too_close(self):
        ok, reason = should_enter(cfg(), book(ask=0.70), btc_price=105.0, remaining_seconds=120)
        self.assertFalse(ok)
        self.assertIn("distance_too_close", reason)

    def test_no_enter_before_entry_window(self):
        ok, reason = should_enter(cfg(entry_window_seconds=120), book(ask=0.70), btc_price=115.0, remaining_seconds=180)
        self.assertFalse(ok)
        self.assertIn("entry_too_early", reason)

    def test_enter_inside_entry_window(self):
        ok, reason = should_enter(cfg(entry_window_seconds=120), book(ask=0.70), btc_price=115.0, remaining_seconds=90)
        self.assertTrue(ok)
        self.assertIn("entry_window_ok", reason)

    def test_auto_entry_precheck_blocks_after_market_loss(self):
        ok, reason = auto_entry_precheck(
            book(ask=0.70),
            book(ask=0.31),
            now_ts=130,
            market_start_ts=0,
            trade_count=0,
            max_trades_per_label_per_market=1,
            market_locked=True,
            no_entry_after_seconds=180,
            max_sum_asks=1.03,
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "market_locked_after_loss")

    def test_auto_entry_precheck_blocks_label_reentry(self):
        ok, reason = auto_entry_precheck(
            book(ask=0.70),
            book(ask=0.31),
            now_ts=130,
            market_start_ts=0,
            trade_count=1,
            max_trades_per_label_per_market=1,
            market_locked=False,
            no_entry_after_seconds=180,
            max_sum_asks=1.03,
        )
        self.assertFalse(ok)
        self.assertIn("max_trades_reached", reason)

    def test_auto_entry_precheck_blocks_late_elapsed_entry(self):
        ok, reason = auto_entry_precheck(
            book(ask=0.70),
            book(ask=0.31),
            now_ts=181,
            market_start_ts=0,
            trade_count=0,
            max_trades_per_label_per_market=1,
            market_locked=False,
            no_entry_after_seconds=180,
            max_sum_asks=1.03,
        )
        self.assertFalse(ok)
        self.assertIn("entry_after_cutoff", reason)

    def test_auto_entry_precheck_blocks_expensive_sum_asks(self):
        ok, reason = auto_entry_precheck(
            book(ask=0.70),
            book(ask=0.34),
            now_ts=130,
            market_start_ts=0,
            trade_count=0,
            max_trades_per_label_per_market=1,
            market_locked=False,
            no_entry_after_seconds=180,
            max_sum_asks=1.03,
        )
        self.assertFalse(ok)
        self.assertIn("sum_asks_too_high", reason)

    def test_auto_entry_precheck_allows_valid_setup(self):
        ok, reason = auto_entry_precheck(
            book(ask=0.70),
            book(ask=0.31),
            now_ts=130,
            market_start_ts=0,
            trade_count=0,
            max_trades_per_label_per_market=1,
            market_locked=False,
            no_entry_after_seconds=180,
            max_sum_asks=1.03,
        )
        self.assertTrue(ok)
        self.assertIn("sum_asks_ok", reason)

    def test_exit_take_profit(self):
        pos = PaperPosition(entry_ts=1, entry_price=0.70, shares=1.42, size_usd=1.0, reason="test")
        ok, reason = should_exit(cfg(), pos, book(bid=0.86), btc_price=115.0, remaining_seconds=120)
        self.assertTrue(ok)
        self.assertEqual(reason, "take_profit")

    def test_take_profit_zero_disables_take_profit_exit(self):
        pos = PaperPosition(entry_ts=1, entry_price=0.70, shares=1.42, size_usd=1.0, reason="test")
        ok, reason = should_exit(cfg(take_profit=0.0), pos, book(bid=0.86), btc_price=115.0, remaining_seconds=120)
        self.assertFalse(ok)
        self.assertEqual(reason, "hold")

    def test_exit_stop_loss(self):
        pos = PaperPosition(entry_ts=1, entry_price=0.70, shares=1.42, size_usd=1.0, reason="test")
        ok, reason = should_exit(cfg(), pos, book(bid=0.54), btc_price=115.0, remaining_seconds=120)
        self.assertTrue(ok)
        self.assertEqual(reason, "stop_loss")

    def test_exit_trailing_stop_after_price_reverses_from_peak(self):
        pos = PaperPosition(entry_ts=1, entry_price=0.70, shares=1.42, size_usd=1.0, reason="test")
        trailing_cfg = cfg(take_profit=0.95, trail_start=0.05, trail_distance=0.04)

        ok, reason = should_exit(trailing_cfg, pos, book(bid=0.76), btc_price=115.0, remaining_seconds=120)
        self.assertFalse(ok)
        self.assertEqual(reason, "hold")
        self.assertEqual(pos.peak_bid, 0.76)

        ok, reason = should_exit(trailing_cfg, pos, book(bid=0.71), btc_price=115.0, remaining_seconds=118)
        self.assertTrue(ok)
        self.assertEqual(reason, "trailing_stop")

    def test_trailing_stop_disabled_when_one_side_is_zero(self):
        pos = PaperPosition(entry_ts=1, entry_price=0.70, shares=1.42, size_usd=1.0, reason="test")
        disabled_cfg = cfg(take_profit=0.95, trail_start=0.05, trail_distance=0.0)

        ok, reason = should_exit(disabled_cfg, pos, book(bid=0.76), btc_price=115.0, remaining_seconds=120)
        self.assertFalse(ok)
        self.assertEqual(reason, "hold")

    def test_late_exit_when_target_close(self):
        pos = PaperPosition(entry_ts=1, entry_price=0.70, shares=1.42, size_usd=1.0, reason="test")
        ok, reason = should_exit(cfg(), pos, book(bid=0.70), btc_price=103.0, remaining_seconds=20)
        self.assertTrue(ok)
        self.assertTrue(reason.startswith("late_exit"))

    def test_taker_fee_matches_polymarket_formula(self):
        self.assertEqual(taker_fee_usdc(shares=100, price=0.50, fee_rate=0.07), 1.75)
        self.assertEqual(taker_fee_usdc(shares=100, price=0.70, fee_rate=0.07), 1.47)

    def test_complete_set_fee_sums_both_legs(self):
        self.assertEqual(complete_set_taker_fee(0.40, 0.60, fee_rate=0.07), 0.0336)

    def test_fee_rate_from_base_fee_bps(self):
        self.assertEqual(fee_rate_from_base_fee(700), 0.07)

    def test_order_book_state_applies_book_and_price_changes(self):
        state = OrderBookState("token-up")
        state.apply_book(
            {
                "asset_id": "token-up",
                "bids": [{"price": "0.48", "size": "30"}, {"price": "0.49", "size": "20"}],
                "asks": [{"price": "0.52", "size": "25"}, {"price": "0.53", "size": "60"}],
                "timestamp": "1000",
                "hash": "0xbook",
            },
            received_ms=1001,
        )

        self.assertEqual(state.to_book_top().bid, 0.49)
        self.assertEqual(state.to_book_top().bid_size, 20)
        self.assertEqual(state.to_book_top().ask, 0.52)
        self.assertEqual(state.to_book_top().ask_size, 25)
        self.assertTrue(state.fresh(max_age_ms=500, now_ms_value=1200))

        state.apply_price_change({"price": "0.49", "size": "0", "side": "BUY", "hash": "0xremove"}, received_ms=1300)
        self.assertEqual(state.to_book_top().bid, 0.48)
        self.assertEqual(state.to_book_top().bid_size, 30)
        self.assertEqual(state.hash, "0xremove")

    def test_order_book_state_uses_best_bid_ask_when_available(self):
        state = OrderBookState("token-down")
        state.apply_best_bid_ask(
            {
                "asset_id": "token-down",
                "best_bid": "0.31",
                "best_ask": "0.34",
                "timestamp": "2000",
            }
        )

        book_top = state.to_book_top()
        self.assertEqual(book_top.bid, 0.31)
        self.assertEqual(book_top.ask, 0.34)
        self.assertEqual(book_top.source, "polymarket_ws_best_bid_ask")

    def test_market_ws_client_applies_documented_messages(self):
        client = PolymarketMarketWsClient(["up", "down"], stale_after_ms=1000)
        client.start = lambda: None

        client.apply_message(
            {
                "event_type": "book",
                "asset_id": "up",
                "bids": [{"price": "0.63", "size": "10"}],
                "asks": [{"price": "0.66", "size": "12"}],
                "timestamp": "1000",
            },
            received_ms=1000,
        )
        client.apply_message(
            {
                "event_type": "best_bid_ask",
                "asset_id": "down",
                "best_bid": "0.32",
                "best_ask": "0.35",
                "timestamp": "1000",
            },
            received_ms=1000,
        )
        client.apply_message(
            {
                "event_type": "price_change",
                "timestamp": "1200",
                "price_changes": [{"asset_id": "up", "price": "0.67", "size": "4", "side": "SELL"}],
            },
            received_ms=1200,
        )

        with patch("paper_bot.now_ms", return_value=1200):
            books = client.get_books(["up", "down"], timeout=0)
        self.assertEqual(books["up"].ask, 0.66)
        self.assertEqual(books["up"].ask_size, 12)
        self.assertEqual(books["down"].bid, 0.32)
        self.assertEqual(books["down"].ask, 0.35)

    def test_market_ws_client_fails_closed_on_stale_books(self):
        client = PolymarketMarketWsClient(["up"], stale_after_ms=100)
        client.start = lambda: None
        client.apply_message(
            {
                "event_type": "best_bid_ask",
                "asset_id": "up",
                "best_bid": "0.60",
                "best_ask": "0.63",
                "timestamp": "1",
            },
            received_ms=1,
        )

        with patch("paper_bot.now_ms", return_value=500):
            with self.assertRaises(ValueError):
                client.get_books(["up"], timeout=0)

    def test_interval_gate_throttles_repeated_keys(self):
        gate = IntervalGate(interval_seconds=10)

        self.assertTrue(gate.allow("btc-up", now_ts=100))
        self.assertFalse(gate.allow("btc-up", now_ts=109))
        self.assertTrue(gate.allow("btc-up", now_ts=110))
        self.assertTrue(gate.allow("btc-down", now_ts=101))

    def test_polymarket_clock_falls_back_to_local_time_without_sync(self):
        clock = PolymarketClock()
        with patch("paper_bot.utc_now", return_value=1234):
            snapshot = clock.snapshot(sync=False)

        self.assertEqual(snapshot, ClockSnapshot(unix_ts=1234, source="local_fallback", synced_age_seconds=None))
        self.assertFalse(snapshot.fresh)

    def test_save_snapshot_omits_raw_orderbook_json_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            con = init_db(db_path)
            try:
                raw = {
                    "bids": [{"price": "0.60", "size": "100"} for _ in range(200)],
                    "asks": [{"price": "0.62", "size": "100"} for _ in range(200)],
                    "hash": "0xabc",
                }
                with patch.dict(paper_bot.os.environ, {"POLYMARKET_STORE_RAW_SNAPSHOT_JSON": "0"}):
                    paper_bot.save_snapshot(
                        con,
                        "test-market Up preset:safe",
                        "token",
                        BookTop(bid=0.60, bid_size=100, ask=0.62, ask_size=100, last=0.61, raw=raw),
                        btc_price=77123.45,
                    )
                row = con.execute("SELECT bid, ask, bid_size, ask_size, btc_price, raw_json FROM snapshots").fetchone()
            finally:
                con.close()

        self.assertEqual(row[0], 0.60)
        self.assertEqual(row[1], 0.62)
        self.assertEqual(row[2], 100)
        self.assertEqual(row[3], 100)
        self.assertEqual(row[4], 77123.45)
        self.assertIsNone(row[5])

    def test_write_latest_state_upserts_by_token(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            con = init_db(db_path)
            try:
                first_book = BookTop(
                    bid=0.60,
                    bid_size=100,
                    ask=0.62,
                    ask_size=100,
                    last=0.61,
                    raw={},
                    updated_ms=1_000,
                    source="polymarket_ws_book",
                )
                second_book = BookTop(
                    bid=0.63,
                    bid_size=90,
                    ask=0.65,
                    ask_size=80,
                    last=0.64,
                    raw={},
                    updated_ms=2_000,
                    source="polymarket_ws_price_change",
                )
                first_price = paper_bot.PriceSnapshot(77100.0, 1, 1_500, "polymarket_rtds_chainlink")
                second_price = paper_bot.PriceSnapshot(77200.0, 2, 2_500, "polymarket_rtds_chainlink")
                clock = ClockSnapshot(unix_ts=1_300, source="polymarket", synced_age_seconds=1)

                paper_bot.write_latest_state(con, "btc-updown-5m-1000", "Up", "token", first_book, first_price, clock, 10)
                paper_bot.write_latest_state(con, "btc-updown-5m-1000", "Up", "token", second_book, second_price, clock, 9)

                rows = con.execute("SELECT token_id, bid, ask, btc_price, remaining_seconds FROM latest_state").fetchall()
            finally:
                con.close()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "token")
        self.assertEqual(rows[0][1], 0.63)
        self.assertEqual(rows[0][2], 0.65)
        self.assertEqual(rows[0][3], 77200.0)
        self.assertEqual(rows[0][4], 9)

    def test_record_arb_event_compacts_raw_orderbook_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            con = init_db(db_path)
            try:
                raw_book = {
                    "market": "0xmarket",
                    "asset_id": "token",
                    "timestamp": "123",
                    "hash": "0xhash",
                    "bids": [{"price": "0.60", "size": "100"} for _ in range(200)],
                    "asks": [{"price": "0.62", "size": "100"} for _ in range(200)],
                }
                paper_bot.record_arb_event(
                    con,
                    label="test-market complete_set",
                    market_slug="test-market",
                    kind="buy_complete_set",
                    yes_price=0.40,
                    no_price=0.55,
                    edge=0.01,
                    payload={"yes": raw_book, "no": raw_book, "fee": 0.02, "buffer": 0.01, "fee_rate": 0.07},
                )
                raw_json = con.execute("SELECT raw_json FROM arb_events").fetchone()[0]
            finally:
                con.close()

        self.assertIn('"fee":0.02', raw_json)
        self.assertIn('"hash":"0xhash"', raw_json)
        self.assertNotIn('"bids"', raw_json)
        self.assertNotIn('"asks"', raw_json)

    def test_compact_db_file_strips_existing_raw_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            con = init_db(db_path)
            try:
                con.execute(
                    """
                    INSERT INTO snapshots
                    (ts, label, token_id, bid, ask, last, bid_size, ask_size, btc_price, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (1, "label", "token", 0.50, 0.51, 0.50, 10, 11, 77000.0, '{"bids":[1],"asks":[2]}'),
                )
                con.execute(
                    "INSERT INTO arb_events (ts, label, market_slug, kind, yes_price, no_price, edge, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (1, "label", "market", "buy_complete_set", 0.40, 0.55, 0.01, '{"bids":[1],"asks":[2]}'),
                )
                con.commit()
            finally:
                con.close()

            result = paper_bot.compact_db_file(db_path)
            con = sqlite3.connect(db_path)
            try:
                snapshot_raw = con.execute("SELECT raw_json FROM snapshots").fetchone()[0]
                arb_raw = con.execute("SELECT raw_json FROM arb_events").fetchone()[0]
            finally:
                con.close()

        self.assertIsNone(snapshot_raw)
        self.assertIsNone(arb_raw)
        self.assertEqual(result["cleared"]["snapshots"], 1)
        self.assertEqual(result["cleared"]["arb_events"], 1)

    def test_extract_target_price_from_event_metadata_price_to_beat(self):
        target = extract_target_price({"eventMetadata": {"priceToBeat": 76868.67001633714}}, {})
        self.assertEqual(target, 76868.67001633714)

    def test_past_results_target_url_for_btc_5m_slug(self):
        url = past_results_target_url("btc-updown-5m-1779623400", 1779623400)
        self.assertIn("symbol=BTC", url)
        self.assertIn("variant=fiveminute", url)
        self.assertIn("assetType=crypto", url)
        self.assertIn("currentEventStartTime=2026-05-24T11%3A50%3A00Z", url)
        self.assertIn("count=1", url)

    def test_target_price_from_past_results_payload_uses_previous_close(self):
        target = target_price_from_past_results_payload(
            {
                "status": "success",
                "data": {
                    "results": [
                        {
                            "startTime": "2026-05-24T11:45:00.000Z",
                            "endTime": "2026-05-24T11:50:00.000Z",
                            "openPrice": 77153.19023930343,
                            "closePrice": 77146.43456804988,
                        }
                    ]
                },
            }
        )
        self.assertEqual(target, 77146.43456804988)

    def test_target_price_from_event_page_html_uses_crypto_open_price(self):
        target = target_price_from_event_page_html(
            (
                '{"state":{"data":{"openPrice":77001.84379230977,"closePrice":null}},'
                '"queryKey":["crypto-prices","price","BTC","2026-05-24T12:20:00Z",'
                '"fiveminute","2026-05-24T12:25:00Z"]}'
            ),
            "BTC",
            "2026-05-24T12:20:00Z",
        )
        self.assertEqual(target, 77001.84379230977)

    def test_target_price_from_event_page_html_falls_back_to_past_results_close(self):
        target = target_price_from_event_page_html(
            (
                '{"state":{"data":{"results":[{"closePrice":77113.2639625187},'
                '{"closePrice":77001.84379230977}]}},'
                '"queryKey":["past-results","BTC","fiveminute","2026-05-24T12:20:00Z"]}'
            ),
            "BTC",
            "2026-05-24T12:20:00Z",
        )
        self.assertEqual(target, 77001.84379230977)

    def test_target_lookup_window_waits_for_future_market(self):
        self.assertFalse(target_lookup_window_open(start_ts=1_000, now_ts=990))
        self.assertFalse(target_lookup_window_open(start_ts=1_000, now_ts=1_001))
        self.assertTrue(target_lookup_window_open(start_ts=1_000, now_ts=1_005))

    def test_target_retry_wait_seconds_waits_until_start_plus_delay(self):
        self.assertEqual(paper_bot.target_retry_wait_seconds(start_ts=1_000, now_ts=999), 6)
        self.assertEqual(paper_bot.target_retry_wait_seconds(start_ts=1_000, now_ts=1_005), 0)

    def test_default_target_retry_policy(self):
        self.assertEqual(paper_bot.DEFAULT_TARGET_PRICE_START_DELAY_SECONDS, 5)
        self.assertEqual(paper_bot.DEFAULT_TARGET_PRICE_RETRY_SECONDS, 3)
        self.assertEqual(paper_bot.DEFAULT_TARGET_PRICE_MAX_RETRIES, 10)

    def test_fetch_market_info_with_retry_waits_then_retries_required_target(self):
        no_target = paper_bot.MarketInfo(
            slug="btc-updown-5m-1000",
            title="BTC Up or Down",
            end_ts=1_300,
            start_ts=1_000,
            target_price=None,
            resolution_source="https://data.chain.link/streams/btc-usd",
            outcomes=[paper_bot.Outcome("Up", "up"), paper_bot.Outcome("Down", "down")],
        )
        with_target = paper_bot.MarketInfo(
            slug="btc-updown-5m-1000",
            title="BTC Up or Down",
            end_ts=1_300,
            start_ts=1_000,
            target_price=77000.0,
            resolution_source="https://data.chain.link/streams/btc-usd",
            outcomes=[paper_bot.Outcome("Up", "up"), paper_bot.Outcome("Down", "down")],
        )
        with patch("paper_bot.fetch_market_info", side_effect=[no_target, no_target, with_target]) as fetch_info:
            with patch("paper_bot.target_retry_wait_seconds", return_value=5):
                with patch("paper_bot.time.sleep") as sleep:
                    market = paper_bot.fetch_market_info_with_retry(
                        "btc-updown-5m-1000",
                        timeout_seconds=60,
                        require_target=True,
                        target_retry_seconds=3,
                        target_max_retries=5,
                    )

        self.assertEqual(market.target_price, 77000.0)
        self.assertEqual(fetch_info.call_count, 3)
        self.assertEqual(sleep.call_args_list, [call(5), call(3)])

    def test_fetch_market_info_with_retry_waits_until_start_delay_even_if_target_exists(self):
        early_target = paper_bot.MarketInfo(
            slug="btc-updown-5m-1000",
            title="BTC Up or Down",
            end_ts=1_300,
            start_ts=1_000,
            target_price=76000.0,
            resolution_source="https://data.chain.link/streams/btc-usd",
            outcomes=[paper_bot.Outcome("Up", "up"), paper_bot.Outcome("Down", "down")],
        )
        delayed_target = paper_bot.MarketInfo(
            slug="btc-updown-5m-1000",
            title="BTC Up or Down",
            end_ts=1_300,
            start_ts=1_000,
            target_price=77000.0,
            resolution_source="https://data.chain.link/streams/btc-usd",
            outcomes=[paper_bot.Outcome("Up", "up"), paper_bot.Outcome("Down", "down")],
        )
        with patch("paper_bot.fetch_market_info", side_effect=[early_target, delayed_target]) as fetch_info:
            with patch("paper_bot.target_retry_wait_seconds", return_value=5):
                with patch("paper_bot.time.sleep") as sleep:
                    market = paper_bot.fetch_market_info_with_retry(
                        "btc-updown-5m-1000",
                        timeout_seconds=60,
                        require_target=True,
                    )

        self.assertEqual(market.target_price, 77000.0)
        self.assertEqual(fetch_info.call_count, 2)
        self.assertEqual(sleep.call_args_list, [call(5)])

    def test_fetch_market_info_with_retry_rejects_previous_market_target(self):
        stale_target = paper_bot.MarketInfo(
            slug="btc-updown-5m-1000",
            title="BTC Up or Down",
            end_ts=1_300,
            start_ts=1_000,
            target_price=76000.0,
            resolution_source="https://data.chain.link/streams/btc-usd",
            outcomes=[paper_bot.Outcome("Up", "up"), paper_bot.Outcome("Down", "down")],
        )
        current_target = paper_bot.MarketInfo(
            slug="btc-updown-5m-1000",
            title="BTC Up or Down",
            end_ts=1_300,
            start_ts=1_000,
            target_price=77000.0,
            resolution_source="https://data.chain.link/streams/btc-usd",
            outcomes=[paper_bot.Outcome("Up", "up"), paper_bot.Outcome("Down", "down")],
        )
        with patch("paper_bot.fetch_market_info", side_effect=[stale_target, stale_target, current_target]):
            with patch("paper_bot.target_retry_wait_seconds", return_value=0):
                with patch("paper_bot.time.sleep") as sleep:
                    market = paper_bot.fetch_market_info_with_retry(
                        "btc-updown-5m-1000",
                        timeout_seconds=60,
                        require_target=True,
                        previous_target_price=76000.0,
                        target_retry_seconds=3,
                        target_max_retries=5,
                    )

        self.assertEqual(market.target_price, 77000.0)
        self.assertEqual(sleep.call_args_list, [call(3)])

    def test_fetch_market_info_with_retry_stops_after_target_attempts(self):
        no_target = paper_bot.MarketInfo(
            slug="btc-updown-5m-1000",
            title="BTC Up or Down",
            end_ts=1_300,
            start_ts=1_000,
            target_price=None,
            resolution_source="https://data.chain.link/streams/btc-usd",
            outcomes=[paper_bot.Outcome("Up", "up"), paper_bot.Outcome("Down", "down")],
        )
        with patch("paper_bot.fetch_market_info", side_effect=[no_target, no_target, no_target, no_target]):
            with patch("paper_bot.target_retry_wait_seconds", return_value=0):
                with patch("paper_bot.time.sleep") as sleep:
                    with self.assertRaisesRegex(ValueError, "No current market target price"):
                        paper_bot.fetch_market_info_with_retry(
                            "btc-updown-5m-1000",
                            timeout_seconds=60,
                            require_target=True,
                            target_retry_seconds=3,
                            target_max_retries=2,
                        )

        self.assertEqual(sleep.call_args_list, [call(3), call(3)])

    def test_pending_market_metadata_clears_previous_target_during_chain_transition(self):
        metadata = pending_market_metadata("https://polymarket.com/event/btc-updown-5m-1779550800")

        self.assertEqual(metadata["slug"], "btc-updown-5m-1779550800")
        self.assertIsNone(metadata["target_price"])
        self.assertIsNone(metadata["resolution_source"])
        self.assertEqual(metadata["status"], "lookup")

    def test_latest_market_target_price_excludes_current_pending_slug(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            write_market_context(
                db_path,
                {
                    "slug": "btc-updown-5m-old",
                    "title": "old",
                    "target_price": 76000.0,
                    "resolution_source": "https://data.chain.link/streams/btc-usd",
                    "start_ts": 1_000,
                    "end_ts": 1_300,
                    "status": "active",
                },
            )
            write_market_context(
                db_path,
                {
                    "slug": "btc-updown-5m-new",
                    "title": "new",
                    "target_price": None,
                    "resolution_source": None,
                    "start_ts": 1_300,
                    "end_ts": 1_600,
                    "status": "lookup",
                },
            )

            target = paper_bot.latest_market_target_price(db_path, exclude_slug="btc-updown-5m-new")

        self.assertEqual(target, 76000.0)

    def test_dashboard_chain_count_uses_submitted_value(self):
        config = normalize_bot_config({"url": "btc-updown-5m-1", "chain_count": "3"})
        self.assertEqual(config["chain_count"], 3)

    def test_dashboard_chain_count_default_matches_ui(self):
        config = normalize_bot_config({"url": "btc-updown-5m-1"})
        self.assertEqual(config["chain_count"], DEFAULT_CHAIN_COUNT)

    def test_dashboard_poll_default_is_two_seconds(self):
        config = normalize_bot_config({"url": "btc-updown-5m-1"})
        self.assertEqual(config["poll"], DEFAULT_POLL_SECONDS)
        self.assertEqual(config["poll"], 2)

    def test_strategy_label_includes_core_settings(self):
        config = normalize_bot_config({"url": "btc-updown-5m-1", "min_distance_usd": "10", "entry_max": "0.67"})
        label = strategy_label(config)
        self.assertIn("custom_e0.65-0.67", label)
        self.assertIn("tp0.88", label)
        self.assertIn("dist10", label)
        self.assertIn("lock", label)

    def test_dashboard_config_defaults_to_three_enabled_presets(self):
        config = normalize_bot_config({"url": "btc-updown-5m-1"})
        self.assertEqual([preset["key"] for preset in config["presets"]], ["safe", "balanced", "aggressive"])
        self.assertTrue(all(preset["enabled"] for preset in config["presets"]))
        self.assertEqual(config["presets"][0]["take_profit"], 0.84)
        self.assertEqual(config["presets"][0]["min_distance_usd"], 100.0)
        self.assertEqual(config["presets"][0]["trail_start"], 0.05)
        self.assertEqual(config["presets"][0]["trail_distance"], 0.04)
        self.assertNotIn("hard_exit", config["presets"][0])
        self.assertEqual(config["presets"][1]["entry_min"], 0.65)
        self.assertEqual(config["presets"][1]["min_distance_usd"], 75.0)
        self.assertEqual(config["presets"][1]["trail_start"], 0.05)
        self.assertEqual(config["presets"][1]["trail_distance"], 0.05)
        self.assertEqual(config["presets"][2]["entry_max"], 0.72)
        self.assertEqual(config["presets"][2]["min_distance_usd"], 75.0)
        self.assertEqual(config["presets"][2]["trail_start"], 0.08)
        self.assertEqual(config["presets"][2]["trail_distance"], 0.06)

    def test_dashboard_config_accepts_custom_preset_values(self):
        config = normalize_bot_config(
            {
                "url": "btc-updown-5m-1",
                "presets": [
                    {
                        "key": "safe",
                        "enabled": "true",
                        "entry_min": "0.64",
                        "entry_max": "0.69",
                        "take_profit": "0.85",
                        "stop_loss": "0.59",
                        "trail_start": "0.04",
                        "trail_distance": "0.03",
                        "late_seconds": "25",
                        "min_distance_usd": "18",
                        "max_trades_per_label_per_market": "2",
                    }
                ],
            }
        )
        self.assertEqual(len(config["presets"]), 1)
        self.assertEqual(config["presets"][0]["entry_min"], 0.64)
        self.assertEqual(config["presets"][0]["trail_start"], 0.04)
        self.assertEqual(config["presets"][0]["trail_distance"], 0.03)
        self.assertEqual(config["presets"][0]["max_trades_per_label_per_market"], 2)

    def test_dashboard_config_allows_take_profit_zero(self):
        config = normalize_bot_config(
            {
                "url": "btc-updown-5m-1",
                "presets": [
                    {
                        "key": "safe",
                        "take_profit": "0",
                    }
                ],
            }
        )

        self.assertEqual(config["presets"][0]["take_profit"], 0.0)
        self.assertEqual(config["take_profit"], 0.0)

    def test_normalize_presets_zeroes_trailing_when_one_value_is_zero(self):
        presets = normalize_presets(
            {
                "presets": [
                    {
                        "key": "safe",
                        "trail_start": "0.05",
                        "trail_distance": "0",
                    }
                ]
            }
        )

        self.assertEqual(presets[0]["trail_start"], 0.0)
        self.assertEqual(presets[0]["trail_distance"], 0.0)

    def test_new_run_db_path_uses_timestamp_source_and_strategy(self):
        config = normalize_bot_config({"url": "https://polymarket.com/event/btc-updown-5m-1779623400"})
        with tempfile.TemporaryDirectory() as tmp:
            old_runs_dir = paper_bot.DEFAULT_RUNS_DIR
            paper_bot.DEFAULT_RUNS_DIR = Path(tmp)
            try:
                path = new_run_db_path(config, started_ts=1_779_623_400)
            finally:
                paper_bot.DEFAULT_RUNS_DIR = old_runs_dir
        self.assertEqual(path.parent, Path(tmp))
        self.assertIn("20260524_115000Z", path.name)
        self.assertIn("btc-updown-5m-1779623400", path.name)
        self.assertIn("multi_safe-balanced-aggressive_e0.65-0.68", path.name)
        self.assertEqual(path.suffix, ".sqlite")

    def test_initial_dashboard_db_path_starts_empty_even_when_runs_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            old_runs_dir = paper_bot.DEFAULT_RUNS_DIR
            paper_bot.DEFAULT_RUNS_DIR = Path(tmp)
            try:
                older = Path(tmp) / "20260524_110000Z_old.sqlite"
                newer = Path(tmp) / "20260524_120000Z_new.sqlite"
                ignored = Path(tmp) / "_dashboard_empty.sqlite"
                older.write_text("old", encoding="utf-8")
                ignored.write_text("ignored", encoding="utf-8")
                newer.write_text("new", encoding="utf-8")
                self.assertEqual(latest_run_db_path(), newer)
                self.assertEqual(initial_dashboard_db_path(paper_bot.DEFAULT_DB), empty_dashboard_db_path())
            finally:
                paper_bot.DEFAULT_RUNS_DIR = old_runs_dir

    def test_initial_dashboard_db_path_uses_empty_placeholder_without_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            old_runs_dir = paper_bot.DEFAULT_RUNS_DIR
            paper_bot.DEFAULT_RUNS_DIR = Path(tmp)
            try:
                self.assertIsNone(latest_run_db_path())
                self.assertEqual(initial_dashboard_db_path(paper_bot.DEFAULT_DB), empty_dashboard_db_path())
            finally:
                paper_bot.DEFAULT_RUNS_DIR = old_runs_dir

    def test_empty_dashboard_payload_does_not_create_placeholder_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            old_runs_dir = paper_bot.DEFAULT_RUNS_DIR
            paper_bot.DEFAULT_RUNS_DIR = Path(tmp)
            try:
                db_path = empty_dashboard_db_path()
                payload = dashboard_payload(db_path)
                self.assertFalse(db_path.exists())
            finally:
                paper_bot.DEFAULT_RUNS_DIR = old_runs_dir
        self.assertEqual(payload["overall"]["trades"], 0)
        self.assertEqual(payload["overall"]["pnl"], 0.0)
        self.assertEqual(payload["trades"], [])

    def test_current_dashboard_payload_is_empty_when_bot_not_running(self):
        with tempfile.TemporaryDirectory() as tmp:
            old_bot_process = paper_bot.BOT_PROCESS
            old_bot_db_path = paper_bot.BOT_DB_PATH
            old_dashboard_db_path = paper_bot.DASHBOARD_DB_PATH
            db_path = Path(tmp) / "old-run.sqlite"
            paper_bot.write_run_metadata(
                db_path,
                {
                    "market": {
                        "target_price": 77000.0,
                        "resolution_source": "https://data.chain.link/streams/btc-usd",
                        "end_ts": paper_bot.utc_now() + 120,
                    }
                },
            )
            try:
                paper_bot.BOT_PROCESS = None
                paper_bot.BOT_DB_PATH = db_path
                paper_bot.DASHBOARD_DB_PATH = db_path
                payload = current_dashboard_payload()
            finally:
                paper_bot.BOT_PROCESS = old_bot_process
                paper_bot.BOT_DB_PATH = old_bot_db_path
                paper_bot.DASHBOARD_DB_PATH = old_dashboard_db_path

        self.assertEqual(payload["db_path"], str(empty_dashboard_db_path()))
        self.assertIsNone(payload["market_reference"]["target_price"])
        self.assertIsNone(payload["market_reference"]["current_price"])
        self.assertEqual(payload["snapshots"], [])

    def test_dashboard_html_shows_current_data_file(self):
        self.assertIn("dataFile", DASHBOARD_HTML)
        self.assertIn("Data:", DASHBOARD_HTML)

    def test_dashboard_preserves_dirty_form_values_during_status_refresh(self):
        self.assertIn("formDirty", DASHBOARD_HTML)
        self.assertIn("!formDirty", DASHBOARD_HTML)
        self.assertIn("forceFormSync", DASHBOARD_HTML)

    def test_dashboard_highlights_market_target_price_log(self):
        self.assertIn(".terminal .log-target", DASHBOARD_HTML)
        self.assertIn("Market target price:", DASHBOARD_HTML)
        self.assertIn("return'log-target'", DASHBOARD_HTML)

    def test_dashboard_terminal_hides_scrollbars(self):
        self.assertIn("scrollbar-width:none", DASHBOARD_HTML)
        self.assertIn("-ms-overflow-style:none", DASHBOARD_HTML)
        self.assertIn(".terminal::-webkit-scrollbar{display:none", DASHBOARD_HTML)
        self.assertIn("overflow-wrap:anywhere", DASHBOARD_HTML)

    def test_dashboard_run_tab_uses_available_viewport_space(self):
        self.assertIn("width:calc(100% - 32px)", DASHBOARD_HTML)
        self.assertIn("min-height:calc(100dvh - 116px)", DASHBOARD_HTML)
        self.assertIn("#run:not(.hidden){display:flex;flex:1;min-height:0}", DASHBOARD_HTML)
        self.assertIn(".run-grid{width:100%;min-height:0", DASHBOARD_HTML)
        self.assertIn(".terminal-panel{display:flex;flex-direction:column;min-height:0;height:auto}", DASHBOARD_HTML)
        self.assertIn("@media(min-width:1321px){body{height:100dvh;overflow:hidden}", DASHBOARD_HTML)

    def test_dashboard_html_includes_active_position_view(self):
        self.assertIn("activeOverview", DASHBOARD_HTML)
        self.assertIn("activeOverviewCount", DASHBOARD_HTML)
        self.assertIn("activeMarketReference", DASHBOARD_HTML)
        self.assertIn("reference-current", DASHBOARD_HTML)
        self.assertIn("reference-distance-inline", DASHBOARD_HTML)
        self.assertIn("Price To Beat", DASHBOARD_HTML)
        self.assertIn("Current Price", DASHBOARD_HTML)
        self.assertIn("price_age_ms", DASHBOARD_HTML)
        self.assertIn("time_source", DASHBOARD_HTML)
        self.assertIn("countdown-pair", DASHBOARD_HTML)
        self.assertIn("#f7931a", DASHBOARD_HTML)
        self.assertIn("activePositions", DASHBOARD_HTML)
        self.assertIn("activePositionCell", DASHBOARD_HTML)
        self.assertIn("activePnlCell", DASHBOARD_HTML)
        self.assertIn("['PnL',activePnlCell]", DASHBOARD_HTML)

    def test_dashboard_html_polls_realtime_endpoint_between_full_refreshes(self):
        self.assertIn("/api/realtime", DASHBOARD_HTML)
        self.assertIn("refreshRealtime", DASHBOARD_HTML)
        self.assertIn("setInterval(refreshRealtime,500)", DASHBOARD_HTML)

    def test_dashboard_html_includes_preset_matrix_and_rr(self):
        self.assertIn("presetBoard", DASHBOARD_HTML)
        self.assertIn("data-preset=\"safe\"", DASHBOARD_HTML)
        self.assertIn("data-preset=\"balanced\"", DASHBOARD_HTML)
        self.assertIn("data-preset=\"aggressive\"", DASHBOARD_HTML)
        self.assertIn("data-preset-field=\"take_profit\" type=\"number\" min=\"0\"", DASHBOARD_HTML)
        self.assertIn("data-preset-field=\"trail_start\"", DASHBOARD_HTML)
        self.assertIn("data-preset-field=\"trail_distance\"", DASHBOARD_HTML)
        self.assertIn("rrMetrics", DASHBOARD_HTML)

    def test_dashboard_html_hides_binance_btc_reference_price(self):
        self.assertNotIn("BTC ${valueMaybe(latest?.btc_price", DASHBOARD_HTML)
        self.assertNotIn("['BTC',r=>valueMaybe(r.btc_price,2)]", DASHBOARD_HTML)

    def test_dashboard_equity_chart_marks_each_trade(self):
        self.assertIn("markers=rows.map", DASHBOARD_HTML)
        self.assertIn("<circle cx=", DASHBOARD_HTML)
        self.assertIn("PnL ${Number(r.pnl)", DASHBOARD_HTML)

    def test_dashboard_payload_includes_active_position_unrealized_pnl(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            con = init_db(db_path)
            try:
                pos = PaperPosition(entry_ts=1_000, entry_price=0.50, shares=2.0, size_usd=1.0, reason="entry")
                upsert_active_position(con, cfg(), pos, book(bid=0.60, ask=0.62), btc_price=101.0)
            finally:
                con.close()
            payload = dashboard_payload(db_path)
        self.assertEqual(payload["overall"]["active_positions"], 1)
        self.assertAlmostEqual(payload["overall"]["active_unrealized_pnl"], 0.2)
        self.assertAlmostEqual(payload["overall"]["total_with_active_pnl"], 0.2)
        self.assertEqual(payload["active_positions"][0]["market_slug"], "test-market")
        self.assertAlmostEqual(payload["active_positions"][0]["unrealized_roi"], 20.0)

    def test_dashboard_payload_includes_market_reference_prices(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            con = init_db(db_path)
            try:
                paper_bot.write_run_metadata(
                    db_path,
                    {
                        "market": {
                            "target_price": 77000.0,
                            "resolution_source": "https://data.chain.link/streams/btc-usd",
                            "end_ts": paper_bot.utc_now() + 120,
                        }
                    },
                )
                paper_bot.save_snapshot(con, "test-market Up preset:safe", "token", book(bid=0.60, ask=0.62), btc_price=77123.45)
            finally:
                con.close()
            payload = dashboard_payload(db_path)
        self.assertAlmostEqual(payload["market_reference"]["target_price"], 77000.0)
        self.assertAlmostEqual(payload["market_reference"]["current_price"], 77123.45)
        self.assertAlmostEqual(payload["market_reference"]["distance"], 123.45)
        self.assertGreaterEqual(payload["market_reference"]["remaining_seconds"], 0)
        self.assertLessEqual(payload["market_reference"]["remaining_seconds"], 120)
        self.assertEqual(payload["market_reference"]["resolution_source"], "https://data.chain.link/streams/btc-usd")
        self.assertEqual(payload["market_reference"]["current_price_source"], "snapshot")
        self.assertIn("price_age_ms", payload["market_reference"])
        self.assertEqual(payload["market_reference"]["time_source"], "local_fallback")

    def test_dashboard_payload_prefers_latest_state_over_history_snapshots(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            write_market_context(
                db_path,
                {
                    "slug": "btc-updown-5m-new",
                    "title": "new",
                    "target_price": 77000.0,
                    "resolution_source": "https://data.chain.link/streams/btc-usd",
                    "start_ts": 1_000,
                    "end_ts": paper_bot.utc_now() + 120,
                    "status": "active",
                },
            )
            con = init_db(db_path)
            try:
                paper_bot.save_snapshot(con, "btc-updown-5m-new Up preset:safe", "old-token", book(bid=0.60, ask=0.62), btc_price=77111.0)
                latest_book = BookTop(
                    bid=0.70,
                    bid_size=90,
                    ask=0.72,
                    ask_size=80,
                    last=0.71,
                    raw={},
                    updated_ms=paper_bot.now_ms(),
                    source="polymarket_ws_price_change",
                )
                latest_price = paper_bot.PriceSnapshot(
                    price=77222.0,
                    timestamp_ms=1,
                    received_ms=paper_bot.now_ms(),
                    source="polymarket_rtds_chainlink",
                )
                clock = ClockSnapshot(unix_ts=1_050, source="polymarket", synced_age_seconds=1)
                paper_bot.write_latest_state(con, "btc-updown-5m-new", "Up", "new-token", latest_book, latest_price, clock, 42)
            finally:
                con.close()

            payload = dashboard_payload(db_path)

        self.assertEqual(payload["market_reference"]["current_price"], 77222.0)
        self.assertEqual(payload["market_reference"]["current_price_source"], "polymarket_rtds_chainlink")
        self.assertEqual(payload["market_reference"]["remaining_seconds"], 42)
        self.assertEqual(payload["snapshots"][0]["bid"], 0.70)
        self.assertEqual(payload["snapshots"][0]["source"], "polymarket_ws_price_change")

    def test_dashboard_realtime_payload_uses_latest_state_without_reference_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            write_market_context(
                db_path,
                {
                    "slug": "btc-updown-5m-new",
                    "title": "new",
                    "target_price": 77000.0,
                    "resolution_source": "https://data.chain.link/streams/btc-usd",
                    "start_ts": 1_000,
                    "end_ts": paper_bot.utc_now() + 120,
                    "status": "active",
                },
            )
            con = init_db(db_path)
            try:
                latest_book = BookTop(
                    bid=0.70,
                    bid_size=90,
                    ask=0.72,
                    ask_size=80,
                    last=0.71,
                    raw={},
                    updated_ms=paper_bot.now_ms(),
                    source="polymarket_ws_price_change",
                )
                latest_price = paper_bot.PriceSnapshot(
                    price=77222.0,
                    timestamp_ms=1,
                    received_ms=paper_bot.now_ms(),
                    source="polymarket_rtds_chainlink",
                )
                clock = ClockSnapshot(unix_ts=1_050, source="polymarket", synced_age_seconds=1)
                paper_bot.write_latest_state(con, "btc-updown-5m-new", "Up", "new-token", latest_book, latest_price, clock, 42)
            finally:
                con.close()

            with patch("paper_bot.fetch_reference_price_snapshot") as fetch_price:
                payload = paper_bot.dashboard_realtime_payload(db_path)

        fetch_price.assert_not_called()
        self.assertEqual(payload["market_reference"]["current_price"], 77222.0)
        self.assertEqual(payload["market_reference"]["remaining_seconds"], 42)
        self.assertEqual(payload["snapshots"][0]["bid"], 0.70)

    def test_dashboard_payload_falls_back_to_polymarket_rtds_current_price(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            paper_bot.write_run_metadata(
                db_path,
                {
                    "market": {
                        "target_price": 77000.0,
                        "resolution_source": "https://data.chain.link/streams/btc-usd",
                        "end_ts": paper_bot.utc_now() + 120,
                    }
                },
            )
            snapshot = paper_bot.PriceSnapshot(price=77111.0, timestamp_ms=1, received_ms=paper_bot.now_ms(), source="polymarket_rtds_chainlink")
            with patch("paper_bot.fetch_reference_price_snapshot", return_value=snapshot) as fetch_price:
                payload = dashboard_payload(db_path)
        fetch_price.assert_called_once_with("https://data.chain.link/streams/btc-usd", timeout=3.0)
        self.assertAlmostEqual(payload["market_reference"]["current_price"], 77111.0)
        self.assertAlmostEqual(payload["market_reference"]["distance"], 111.0)
        self.assertEqual(payload["market_reference"]["current_price_source"], "polymarket_rtds_chainlink")

    def test_dashboard_payload_ignores_previous_market_snapshots_for_current_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            con = init_db(db_path)
            try:
                paper_bot.write_run_metadata(
                    db_path,
                    {
                        "source": "btc-updown-5m-new",
                        "market": {
                            "slug": "btc-updown-5m-new",
                            "title": "btc-updown-5m-new",
                            "target_price": None,
                            "resolution_source": None,
                            "start_ts": None,
                            "end_ts": paper_bot.utc_now() + 120,
                            "status": "lookup",
                        },
                    },
                )
                paper_bot.save_snapshot(con, "btc-updown-5m-old Up preset:safe", "old-token", book(bid=0.60, ask=0.62), btc_price=77123.45)
            finally:
                con.close()

            payload = dashboard_payload(db_path)

        self.assertIsNone(payload["market_reference"]["target_price"])
        self.assertIsNone(payload["market_reference"]["current_price"])
        self.assertEqual(payload["snapshots"], [])

    def test_dashboard_does_not_hydrate_failed_market_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            paper_bot.write_run_metadata(db_path, {"source": "btc-updown-5m-new"})
            write_market_context(
                db_path,
                {
                    "slug": "btc-updown-5m-new",
                    "title": "new",
                    "target_price": None,
                    "resolution_source": None,
                    "start_ts": 1_300,
                    "end_ts": paper_bot.utc_now() + 120,
                    "status": "failed",
                },
            )
            with patch("paper_bot.fetch_market_info") as fetch_info:
                payload = dashboard_payload(db_path)

        fetch_info.assert_not_called()
        self.assertIsNone(payload["market_reference"]["target_price"])
        self.assertIsNone(payload["market_reference"]["current_price"])

    def test_dashboard_payload_uses_only_current_market_snapshots(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            con = init_db(db_path)
            try:
                paper_bot.write_run_metadata(
                    db_path,
                    {
                        "source": "btc-updown-5m-new",
                        "market": {
                            "slug": "btc-updown-5m-new",
                            "title": "btc-updown-5m-new",
                            "target_price": 77000.0,
                            "resolution_source": "https://data.chain.link/streams/btc-usd",
                            "start_ts": None,
                            "end_ts": paper_bot.utc_now() + 120,
                            "status": "active",
                        },
                    },
                )
                paper_bot.save_snapshot(con, "btc-updown-5m-old Up preset:safe", "old-token", book(bid=0.60, ask=0.62), btc_price=77123.45)
                paper_bot.save_snapshot(con, "btc-updown-5m-new Up preset:safe", "new-token", book(bid=0.70, ask=0.72), btc_price=77222.0)
            finally:
                con.close()

            payload = dashboard_payload(db_path)

        self.assertAlmostEqual(payload["market_reference"]["target_price"], 77000.0)
        self.assertAlmostEqual(payload["market_reference"]["current_price"], 77222.0)
        self.assertEqual(len(payload["snapshots"]), 1)
        self.assertIn("btc-updown-5m-new", payload["snapshots"][0]["label"])

    def test_dashboard_uses_active_market_context_not_legacy_market_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            con = init_db(db_path)
            try:
                paper_bot.write_run_metadata(
                    db_path,
                    {
                        "market": {
                            "slug": "btc-updown-5m-old",
                            "title": "old",
                            "target_price": 76000.0,
                            "resolution_source": "https://data.chain.link/streams/btc-usd",
                            "start_ts": None,
                            "end_ts": paper_bot.utc_now() + 120,
                            "status": "active",
                        }
                    },
                )
                write_market_context(
                    db_path,
                    {
                        "slug": "btc-updown-5m-new",
                        "title": "new",
                        "target_price": 77000.0,
                        "resolution_source": "https://data.chain.link/streams/btc-usd",
                        "start_ts": None,
                        "end_ts": paper_bot.utc_now() + 120,
                        "status": "active",
                    },
                )
                paper_bot.save_snapshot(con, "btc-updown-5m-old Up preset:safe", "old-token", book(bid=0.60, ask=0.62), btc_price=76111.0)
                paper_bot.save_snapshot(con, "btc-updown-5m-new Up preset:safe", "new-token", book(bid=0.70, ask=0.72), btc_price=77222.0)
            finally:
                con.close()

            payload = dashboard_payload(db_path)

        self.assertEqual(payload["market_reference"]["target_price"], 77000.0)
        self.assertEqual(payload["market_reference"]["current_price"], 77222.0)
        self.assertEqual(len(payload["snapshots"]), 1)
        self.assertIn("btc-updown-5m-new", payload["snapshots"][0]["label"])

    def test_dashboard_payload_groups_trades_by_preset(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.sqlite"
            con = init_db(db_path)
            try:
                safe_cfg = cfg(label="test-market Up preset:safe", preset_key="safe", preset_name="An toan")
                aggressive_cfg = cfg(label="test-market Up preset:aggressive", preset_key="aggressive", preset_name="Aggressive")
                paper_bot.record_trade(
                    con,
                    safe_cfg,
                    PaperPosition(entry_ts=1_000, entry_price=0.50, shares=2.0, size_usd=1.0, reason="entry"),
                    0.60,
                    "take_profit",
                )
                paper_bot.record_trade(
                    con,
                    aggressive_cfg,
                    PaperPosition(entry_ts=1_010, entry_price=0.50, shares=2.0, size_usd=1.0, reason="entry"),
                    0.45,
                    "stop_loss",
                )
            finally:
                con.close()
            payload = dashboard_payload(db_path)
        by_preset = {row["preset_key"]: row for row in payload["preset_summary"]}
        self.assertAlmostEqual(by_preset["safe"]["pnl"], 0.2)
        self.assertAlmostEqual(by_preset["aggressive"]["pnl"], -0.1)
        self.assertEqual(payload["trades"][0]["preset_key"], "aggressive")
        self.assertIn("preset_name", payload["preset_equity"][0])

    def test_dashboard_payload_includes_run_metadata_and_db_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "run.sqlite"
            paper_bot.write_run_metadata(
                db_path,
                {
                    "run_id": "run-test",
                    "started_at": "2026-05-24T12:00:00+00:00",
                    "strategy": "band_test",
                    "config": {"entry_min": 0.65},
                },
            )
            payload = dashboard_payload(db_path)
        self.assertEqual(payload["db_path"], str(db_path))
        self.assertEqual(payload["run"]["run_id"], "run-test")
        self.assertEqual(payload["run"]["strategy"], "band_test")
        self.assertEqual(payload["run"]["config"]["entry_min"], 0.65)

    def test_bot_status_reports_orphan_bot_process(self):
        old_process = paper_bot.BOT_PROCESS
        old_started = paper_bot.BOT_STARTED_AT
        old_discover = paper_bot.discover_bot_process_ids
        try:
            paper_bot.BOT_PROCESS = None
            paper_bot.BOT_STARTED_AT = None
            paper_bot.discover_bot_process_ids = lambda: [12345]
            status = paper_bot.bot_status()
        finally:
            paper_bot.BOT_PROCESS = old_process
            paper_bot.BOT_STARTED_AT = old_started
            paper_bot.discover_bot_process_ids = old_discover
        self.assertTrue(status["running"])
        self.assertEqual(status["pid"], 12345)

    def test_dashboard_entry_window_uses_submitted_value(self):
        config = normalize_bot_config({"url": "btc-updown-5m-1", "entry_window_seconds": "120"})
        self.assertEqual(config["entry_window_seconds"], 120)

    def test_dashboard_risk_config_uses_submitted_values(self):
        config = normalize_bot_config(
            {
                "url": "btc-updown-5m-1",
                "max_trades_per_label_per_market": "2",
                "market_lock_after_loss": "false",
                "no_entry_after_seconds": "240",
                "max_sum_asks": "1.02",
            }
        )
        self.assertEqual(config["max_trades_per_label_per_market"], 2)
        self.assertFalse(config["market_lock_after_loss"])
        self.assertEqual(config["no_entry_after_seconds"], 240)
        self.assertEqual(config["max_sum_asks"], 1.02)


if __name__ == "__main__":
    unittest.main()
