from pathlib import Path
import tempfile
import unittest

import paper_bot
from paper_bot import (
    BookTop,
    complete_set_taker_fee,
    DASHBOARD_HTML,
    DEFAULT_CHAIN_COUNT,
    DEFAULT_POLL_SECONDS,
    DirectionalConfig,
    auto_entry_precheck,
    dashboard_payload,
    empty_dashboard_db_path,
    extract_target_price,
    initial_dashboard_db_path,
    init_db,
    latest_run_db_path,
    new_run_db_path,
    PaperPosition,
    past_results_target_url,
    fee_rate_from_base_fee,
    normalize_bot_config,
    normalize_presets,
    parse_json_list,
    parse_slug,
    should_enter,
    should_exit,
    target_lookup_window_open,
    target_price_from_event_page_html,
    target_price_from_past_results_payload,
    taker_fee_usdc,
    strategy_label,
    slug_with_offset,
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

    def test_slug_with_offset(self):
        self.assertEqual(slug_with_offset("btc-updown-5m-1779550500", 300, 2), "btc-updown-5m-1779551100")

    def test_url_or_slug_with_offset(self):
        self.assertEqual(
            url_or_slug_with_offset("https://polymarket.com/event/btc-updown-5m-1779550500", 300, 1),
            "https://polymarket.com/event/btc-updown-5m-1779550800",
        )

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
        self.assertTrue(target_lookup_window_open(start_ts=1_000, now_ts=995))
        self.assertTrue(target_lookup_window_open(start_ts=1_000, now_ts=1_001))

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

    def test_initial_dashboard_db_path_prefers_latest_run_over_legacy_default(self):
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
                self.assertEqual(initial_dashboard_db_path(paper_bot.DEFAULT_DB), newer)
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
        self.assertIn("activePositions", DASHBOARD_HTML)
        self.assertIn("activePositionCell", DASHBOARD_HTML)
        self.assertIn("activePnlCell", DASHBOARD_HTML)
        self.assertIn("['PnL',activePnlCell]", DASHBOARD_HTML)

    def test_dashboard_html_includes_preset_matrix_and_rr(self):
        self.assertIn("presetBoard", DASHBOARD_HTML)
        self.assertIn("data-preset=\"safe\"", DASHBOARD_HTML)
        self.assertIn("data-preset=\"balanced\"", DASHBOARD_HTML)
        self.assertIn("data-preset=\"aggressive\"", DASHBOARD_HTML)
        self.assertIn("data-preset-field=\"take_profit\" type=\"number\" min=\"0\"", DASHBOARD_HTML)
        self.assertIn("data-preset-field=\"trail_start\"", DASHBOARD_HTML)
        self.assertIn("data-preset-field=\"trail_distance\"", DASHBOARD_HTML)
        self.assertIn("rrMetrics", DASHBOARD_HTML)

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
