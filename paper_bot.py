#!/usr/bin/env python3
"""Read-only realtime paper tester for Polymarket strategies."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "data" / "paper.sqlite"
DEFAULT_RUNS_DIR = ROOT / "data" / "runs"
DEFAULT_BOT_STDOUT = ROOT / "reports" / "ui_bot.log"
DEFAULT_BOT_STDERR = ROOT / "reports" / "ui_bot.err.log"
USER_AGENT = "SecondBrainPolymarketPaperBot/0.1"
DEFAULT_TAKER_FEE_RATE = 0.07
DEFAULT_POLL_SECONDS = 2
DEFAULT_CHAIN_COUNT = 6
DEFAULT_ENTRY_MIN = 0.65
DEFAULT_ENTRY_MAX = 0.68
DEFAULT_TAKE_PROFIT = 0.88
DEFAULT_STOP_LOSS = 0.58
DEFAULT_HARD_EXIT = 0.58
DEFAULT_LATE_SECONDS = 30
DEFAULT_MIN_DISTANCE_USD = 100.0
DEFAULT_ENTRY_WINDOW_SECONDS = 120
DEFAULT_MAX_TRADES_PER_LABEL_PER_MARKET = 1
DEFAULT_MARKET_LOCK_AFTER_LOSS = True
DEFAULT_NO_ENTRY_AFTER_SECONDS = 0
DEFAULT_MAX_SUM_ASKS = 1.03
DEFAULT_PRESETS = [
    {
        "key": "safe",
        "name": "An toan",
        "enabled": True,
        "entry_min": 0.65,
        "entry_max": 0.68,
        "take_profit": 0.84,
        "stop_loss": 0.60,
        "hard_exit": 0.60,
        "late_seconds": DEFAULT_LATE_SECONDS,
        "min_distance_usd": 100.0,
        "max_trades_per_label_per_market": DEFAULT_MAX_TRADES_PER_LABEL_PER_MARKET,
    },
    {
        "key": "balanced",
        "name": "Can bang",
        "enabled": True,
        "entry_min": 0.65,
        "entry_max": 0.70,
        "take_profit": 0.86,
        "stop_loss": 0.58,
        "hard_exit": 0.58,
        "late_seconds": DEFAULT_LATE_SECONDS,
        "min_distance_usd": 75.0,
        "max_trades_per_label_per_market": DEFAULT_MAX_TRADES_PER_LABEL_PER_MARKET,
    },
    {
        "key": "aggressive",
        "name": "Aggressive",
        "enabled": True,
        "entry_min": 0.65,
        "entry_max": 0.72,
        "take_profit": 0.88,
        "stop_loss": 0.56,
        "hard_exit": 0.56,
        "late_seconds": DEFAULT_LATE_SECONDS,
        "min_distance_usd": 75.0,
        "max_trades_per_label_per_market": DEFAULT_MAX_TRADES_PER_LABEL_PER_MARKET,
    },
]


@dataclass
class BookTop:
    bid: float | None
    bid_size: float | None
    ask: float | None
    ask_size: float | None
    last: float | None
    raw: dict[str, Any]


@dataclass
class DirectionalConfig:
    token_id: str
    label: str
    preset_key: str
    preset_name: str
    market_slug: str | None
    outcome: str | None
    target_price: float | None
    direction: str
    size_usd: float
    entry_min: float
    entry_max: float
    take_profit: float
    stop_loss: float
    hard_exit: float
    late_seconds: int
    entry_window_seconds: int | None
    min_distance_usd: float
    poll: int
    seconds: int


@dataclass
class PaperPosition:
    entry_ts: int
    entry_price: float
    shares: float
    size_usd: float
    reason: str


@dataclass
class Outcome:
    name: str
    token_id: str


@dataclass
class MarketInfo:
    slug: str
    title: str
    end_ts: int | None
    start_ts: int | None
    target_price: float | None
    outcomes: list[Outcome]


def utc_now() -> int:
    return int(time.time())


def iso(ts: int | None = None) -> str:
    return datetime.fromtimestamp(ts or utc_now(), tz=timezone.utc).isoformat()


def http_json(url: str, timeout: int = 20) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def http_text(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": f"Mozilla/5.0 {USER_AGENT}"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def parse_slug(value: str) -> str:
    value = value.strip()
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme and parsed.netloc:
        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            raise ValueError("Polymarket URL has no slug path")
        return parts[-1]
    return value


def slug_with_offset(slug: str, interval_seconds: int, offset: int) -> str:
    match = re.match(r"^(.*-)(\d+)$", slug)
    if not match:
        raise ValueError(
            f"Cannot auto-chain slug '{slug}' because it does not end with a timestamp. "
            "Use a BTC 5m slug like btc-updown-5m-1779550500."
        )
    prefix, raw_ts = match.groups()
    return f"{prefix}{int(raw_ts) + interval_seconds * offset}"


def url_or_slug_with_offset(value: str, interval_seconds: int, offset: int) -> str:
    slug = parse_slug(value)
    next_slug = slug_with_offset(slug, interval_seconds, offset)
    parsed = urllib.parse.urlparse(value.strip())
    if not (parsed.scheme and parsed.netloc):
        return next_slug
    parts = [part for part in parsed.path.split("/") if part]
    parts[-1] = next_slug
    new_path = "/" + "/".join(parts)
    return urllib.parse.urlunparse(parsed._replace(path=new_path))


def parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        return json.loads(value)
    raise ValueError(f"Expected list-like value, got {type(value).__name__}")


def parse_ts(value: str | None) -> int | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    return int(datetime.fromisoformat(normalized).timestamp())


def parse_optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def extract_target_price(event_data: dict[str, Any], market_data: dict[str, Any]) -> float | None:
    target_keys = ("priceToBeat", "targetPrice", "strikePrice", "startPrice", "openPrice")
    containers = [
        parse_json_object(event_data.get("eventMetadata")),
        parse_json_object(market_data.get("eventMetadata")),
        event_data,
        market_data,
    ]
    for container in containers:
        for key in target_keys:
            target = parse_optional_float(container.get(key))
            if target is not None:
                return target
    return None


def iso_z(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def target_lookup_window_open(start_ts: int | None, now_ts: int | None = None) -> bool:
    if start_ts is None:
        return False
    current_ts = utc_now() if now_ts is None else now_ts
    return start_ts <= current_ts + 5


def updown_symbol(slug: str) -> str | None:
    match = re.match(r"^([a-z0-9]+)-updown-5m-\d+$", slug)
    if not match:
        return None
    return match.group(1).upper()


def past_results_target_url(slug: str, start_ts: int | None) -> str | None:
    if start_ts is None:
        return None
    symbol = updown_symbol(slug)
    if symbol is None:
        return None
    params = urllib.parse.urlencode(
        {
            "symbol": symbol,
            "variant": "fiveminute",
            "assetType": "crypto",
            "currentEventStartTime": iso_z(start_ts),
            "count": 1,
        }
    )
    return f"https://polymarket.com/api/past-results?{params}"


def target_price_from_past_results_payload(payload: Any) -> float | None:
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    results = data.get("results")
    if not isinstance(results, list) or not results:
        return None
    last_result = results[-1]
    if not isinstance(last_result, dict):
        return None
    return parse_optional_float(last_result.get("closePrice"))


def fetch_past_results_target_price(slug: str, start_ts: int | None) -> float | None:
    url = past_results_target_url(slug, start_ts)
    if url is None:
        return None
    return target_price_from_past_results_payload(http_json(url))


def last_json_number_before(text: str, key: str, end_pos: int, max_backtrack: int = 8000) -> float | None:
    window = text[max(0, end_pos - max_backtrack) : end_pos]
    matches = list(re.finditer(rf'"{re.escape(key)}":(-?\d+(?:\.\d+)?)', window))
    if not matches:
        return None
    return parse_optional_float(matches[-1].group(1))


def target_price_from_event_page_html(page_html: str, symbol: str, start_iso: str) -> float | None:
    crypto_price_query = f'"queryKey":["crypto-prices","price","{symbol}","{start_iso}","fiveminute"'
    crypto_pos = page_html.find(crypto_price_query)
    if crypto_pos >= 0:
        target = last_json_number_before(page_html, "openPrice", crypto_pos, max_backtrack=3000)
        if target is not None:
            return target

    past_results_query = f'"queryKey":["past-results","{symbol}","fiveminute","{start_iso}"]'
    past_results_pos = page_html.find(past_results_query)
    if past_results_pos >= 0:
        return last_json_number_before(page_html, "closePrice", past_results_pos)
    return None


def fetch_event_page_target_price(slug: str, start_ts: int | None) -> float | None:
    if start_ts is None:
        return None
    symbol = updown_symbol(slug)
    if symbol is None:
        return None
    page_html = http_text(f"https://polymarket.com/event/{urllib.parse.quote(slug)}")
    return target_price_from_event_page_html(page_html, symbol, iso_z(start_ts))


def fetch_market_info(url_or_slug: str) -> MarketInfo:
    slug = parse_slug(url_or_slug)
    quoted = urllib.parse.quote(slug)
    data: Any | None = None
    errors: list[str] = []
    urls = [
        f"https://gamma-api.polymarket.com/events/slug/{quoted}",
        f"https://gamma-api.polymarket.com/markets/slug/{quoted}",
        f"https://gamma-api.polymarket.com/events?slug={quoted}",
        f"https://gamma-api.polymarket.com/markets?slug={quoted}",
    ]
    for url in urls:
        try:
            data = http_json(url)
            break
        except urllib.error.HTTPError as exc:
            errors.append(f"{url} -> HTTP {exc.code}")
        except urllib.error.URLError as exc:
            errors.append(f"{url} -> {exc.reason}")
    if data is None:
        details = "; ".join(errors)
        raise ValueError(
            f"Cannot find market slug '{slug}'. It may not be indexed yet, expired, or copied incorrectly. "
            f"Try again in 10-30 seconds. Details: {details}"
        )
    if isinstance(data, list):
        if not data:
            raise ValueError(f"No market/event found for slug '{slug}'")
        data = data[0]
    event = data if isinstance(data, dict) else {}
    if "markets" in data:
        markets = data.get("markets") or []
        if not markets:
            raise ValueError(f"No markets found inside event slug: {slug}")
        market = markets[0]
        title = data.get("title") or market.get("question") or slug
    else:
        market = data
        title = market.get("question") or market.get("title") or slug
    names = [str(item) for item in parse_json_list(market.get("outcomes"))]
    token_ids = [str(item) for item in parse_json_list(market.get("clobTokenIds"))]
    if len(names) != len(token_ids):
        raise ValueError("Outcome count does not match token ID count")
    outcomes = [Outcome(name=name, token_id=token_id) for name, token_id in zip(names, token_ids)]
    start_ts = parse_ts(market.get("eventStartTime") or event.get("startTime") or market.get("startDate"))
    target_price = extract_target_price(event, market)
    if target_price is None and target_lookup_window_open(start_ts):
        try:
            target_price = fetch_past_results_target_price(slug, start_ts)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, KeyError, TypeError):
            target_price = None
    if target_price is None and target_lookup_window_open(start_ts):
        try:
            target_price = fetch_event_page_target_price(slug, start_ts)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, KeyError, TypeError):
            target_price = None
    return MarketInfo(
        slug=slug,
        title=title,
        end_ts=parse_ts(market.get("endDate")),
        start_ts=start_ts,
        target_price=target_price,
        outcomes=outcomes,
    )


def fetch_market_info_with_retry(url_or_slug: str, timeout_seconds: int, require_target: bool = False) -> MarketInfo:
    deadline = utc_now() + max(0, timeout_seconds)
    last_error: ValueError | None = None
    while True:
        try:
            market = fetch_market_info(url_or_slug)
            if require_target and market.target_price is None:
                raise ValueError("market target price is not available yet from eventMetadata.priceToBeat, /api/past-results, or event page data")
            return market
        except ValueError as exc:
            last_error = exc
            if utc_now() >= deadline:
                raise
            wait = min(5, max(1, deadline - utc_now()))
            print(f"[{iso()}] market not available yet, retrying in {wait}s: {exc}")
            time.sleep(wait)


def fetch_book(token_id: str) -> BookTop:
    qs = urllib.parse.urlencode({"token_id": token_id})
    data = http_json(f"https://clob.polymarket.com/book?{qs}")
    bids = data.get("bids") or []
    asks = data.get("asks") or []
    best_bid = max((float(level["price"]) for level in bids), default=None)
    best_ask = min((float(level["price"]) for level in asks), default=None)
    bid_size = next((float(level["size"]) for level in bids if float(level["price"]) == best_bid), None)
    ask_size = next((float(level["size"]) for level in asks if float(level["price"]) == best_ask), None)
    last_raw = data.get("last_trade_price")
    return BookTop(
        bid=best_bid,
        bid_size=bid_size,
        ask=best_ask,
        ask_size=ask_size,
        last=float(last_raw) if last_raw not in (None, "") else None,
        raw=data,
    )


def fetch_btc_price() -> float:
    data = http_json("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
    return float(data["price"])


def fee_rate_from_base_fee(base_fee: Any) -> float:
    return float(base_fee) / 10_000.0


def fetch_fee_rate(token_id: str) -> float:
    qs = urllib.parse.urlencode({"token_id": token_id})
    data = http_json(f"https://clob.polymarket.com/fee-rate?{qs}")
    if "base_fee" not in data:
        raise ValueError("fee-rate response did not include base_fee")
    return fee_rate_from_base_fee(data["base_fee"])


def resolve_fee_rate(token_ids: list[str], fallback_fee_rate: float) -> float:
    last_error: Exception | None = None
    for token_id in token_ids:
        try:
            return fetch_fee_rate(token_id)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, KeyError) as exc:
            last_error = exc
    if last_error is not None:
        print(f"[{iso()}] fee-rate fallback={fallback_fee_rate}: {last_error}", file=sys.stderr)
    return fallback_fee_rate


def init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            label TEXT NOT NULL,
            token_id TEXT NOT NULL,
            bid REAL,
            ask REAL,
            last REAL,
            bid_size REAL,
            ask_size REAL,
            btc_price REAL,
            raw_json TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL,
            label TEXT NOT NULL,
            market_slug TEXT,
            outcome TEXT,
            token_id TEXT NOT NULL,
            entry_ts INTEGER NOT NULL,
            exit_ts INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL NOT NULL,
            shares REAL NOT NULL,
            size_usd REAL NOT NULL,
            pnl REAL NOT NULL,
            entry_reason TEXT NOT NULL,
            exit_reason TEXT NOT NULL
        )
        """
    )
    ensure_column(con, "paper_trades", "market_slug", "TEXT")
    ensure_column(con, "paper_trades", "outcome", "TEXT")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS run_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS active_positions (
            label TEXT PRIMARY KEY,
            market_slug TEXT,
            outcome TEXT,
            token_id TEXT NOT NULL,
            entry_ts INTEGER NOT NULL,
            updated_ts INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            bid REAL,
            ask REAL,
            shares REAL NOT NULL,
            size_usd REAL NOT NULL,
            unrealized_pnl REAL,
            unrealized_roi REAL,
            btc_price REAL,
            entry_reason TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS arb_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            label TEXT NOT NULL,
            market_slug TEXT,
            kind TEXT NOT NULL,
            yes_price REAL NOT NULL,
            no_price REAL NOT NULL,
            edge REAL NOT NULL,
            raw_json TEXT
        )
        """
    )
    ensure_column(con, "arb_events", "market_slug", "TEXT")
    con.commit()
    return con


def ensure_column(con: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    columns = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
    if column not in columns:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def save_snapshot(
    con: sqlite3.Connection,
    label: str,
    token_id: str,
    book: BookTop,
    btc_price: float | None,
) -> None:
    con.execute(
        """
        INSERT INTO snapshots
        (ts, label, token_id, bid, ask, last, bid_size, ask_size, btc_price, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            utc_now(),
            label,
            token_id,
            book.bid,
            book.ask,
            book.last,
            book.bid_size,
            book.ask_size,
            btc_price,
            json.dumps(book.raw, separators=(",", ":")),
        ),
    )
    con.commit()


def active_position_values(
    cfg: DirectionalConfig,
    pos: PaperPosition,
    book: BookTop,
    btc_price: float | None,
) -> dict[str, Any]:
    unrealized_pnl = (pos.shares * book.bid - pos.size_usd) if book.bid is not None else None
    unrealized_roi = (unrealized_pnl / pos.size_usd * 100.0) if unrealized_pnl is not None and pos.size_usd else None
    updated_ts = utc_now()
    return {
        "label": cfg.label,
        "market_slug": cfg.market_slug,
        "outcome": cfg.outcome,
        "token_id": cfg.token_id,
        "entry_ts": pos.entry_ts,
        "updated_ts": updated_ts,
        "entry_price": pos.entry_price,
        "bid": book.bid,
        "ask": book.ask,
        "shares": pos.shares,
        "size_usd": pos.size_usd,
        "unrealized_pnl": unrealized_pnl,
        "unrealized_roi": unrealized_roi,
        "btc_price": btc_price,
        "entry_reason": pos.reason,
    }


def upsert_active_position(
    con: sqlite3.Connection,
    cfg: DirectionalConfig,
    pos: PaperPosition,
    book: BookTop,
    btc_price: float | None,
) -> None:
    values = active_position_values(cfg, pos, book, btc_price)
    con.execute(
        """
        INSERT INTO active_positions
        (label, market_slug, outcome, token_id, entry_ts, updated_ts, entry_price, bid, ask, shares,
         size_usd, unrealized_pnl, unrealized_roi, btc_price, entry_reason)
        VALUES
        (:label, :market_slug, :outcome, :token_id, :entry_ts, :updated_ts, :entry_price, :bid, :ask, :shares,
         :size_usd, :unrealized_pnl, :unrealized_roi, :btc_price, :entry_reason)
        ON CONFLICT(label) DO UPDATE SET
            market_slug=excluded.market_slug,
            outcome=excluded.outcome,
            token_id=excluded.token_id,
            entry_ts=excluded.entry_ts,
            updated_ts=excluded.updated_ts,
            entry_price=excluded.entry_price,
            bid=excluded.bid,
            ask=excluded.ask,
            shares=excluded.shares,
            size_usd=excluded.size_usd,
            unrealized_pnl=excluded.unrealized_pnl,
            unrealized_roi=excluded.unrealized_roi,
            btc_price=excluded.btc_price,
            entry_reason=excluded.entry_reason
        """,
        values,
    )
    con.commit()


def delete_active_position(con: sqlite3.Connection, cfg: DirectionalConfig) -> None:
    con.execute("DELETE FROM active_positions WHERE label = ?", (cfg.label,))
    con.commit()


def clear_active_positions(con: sqlite3.Connection, market_slug: str | None = None) -> None:
    if market_slug:
        con.execute("DELETE FROM active_positions WHERE market_slug = ?", (market_slug,))
    else:
        con.execute("DELETE FROM active_positions")
    con.commit()


def target_distance_ok(cfg: DirectionalConfig, btc_price: float | None) -> tuple[bool, str]:
    if cfg.target_price is None or btc_price is None:
        return True, "no_target_filter"
    if cfg.direction == "UP":
        distance = btc_price - cfg.target_price
    else:
        distance = cfg.target_price - btc_price
    if distance >= cfg.min_distance_usd:
        return True, f"distance_ok:{distance:.2f}"
    return False, f"distance_too_close:{distance:.2f}"


def should_enter(
    cfg: DirectionalConfig,
    book: BookTop,
    btc_price: float | None,
    remaining_seconds: int | None = None,
) -> tuple[bool, str]:
    if book.ask is None:
        return False, "no_ask"
    if cfg.entry_window_seconds and remaining_seconds is not None and remaining_seconds > cfg.entry_window_seconds:
        return False, f"entry_too_early:{remaining_seconds}s>{cfg.entry_window_seconds}s"
    if not (cfg.entry_min <= book.ask <= cfg.entry_max):
        return False, "ask_outside_entry_band"
    ok, reason = target_distance_ok(cfg, btc_price)
    if not ok:
        return False, reason
    window_reason = (
        f"entry_window_ok:{remaining_seconds}s<={cfg.entry_window_seconds}s"
        if cfg.entry_window_seconds and remaining_seconds is not None
        else "entry_window_unrestricted"
    )
    return True, f"entry_band:{book.ask:.3f};{window_reason};{reason}"


def auto_entry_precheck(
    book: BookTop,
    opposite_book: BookTop | None,
    now_ts: int,
    market_start_ts: int | None,
    trade_count: int,
    max_trades_per_label_per_market: int,
    market_locked: bool,
    no_entry_after_seconds: int,
    max_sum_asks: float,
) -> tuple[bool, str]:
    if market_locked:
        return False, "market_locked_after_loss"
    if max_trades_per_label_per_market > 0 and trade_count >= max_trades_per_label_per_market:
        return False, f"max_trades_reached:{trade_count}>={max_trades_per_label_per_market}"

    reasons: list[str] = []
    if no_entry_after_seconds > 0 and market_start_ts is not None:
        elapsed = max(0, now_ts - market_start_ts)
        if elapsed > no_entry_after_seconds:
            return False, f"entry_after_cutoff:{elapsed}s>{no_entry_after_seconds}s"
        reasons.append(f"entry_elapsed_ok:{elapsed}s<={no_entry_after_seconds}s")
    elif no_entry_after_seconds > 0:
        reasons.append("entry_elapsed_unknown")
    else:
        reasons.append("entry_elapsed_unrestricted")

    if max_sum_asks > 0:
        if book.ask is None:
            return False, "no_ask"
        if opposite_book is None or opposite_book.ask is None:
            return False, "no_opposite_ask"
        sum_asks = book.ask + opposite_book.ask
        if sum_asks > max_sum_asks:
            return False, f"sum_asks_too_high:{sum_asks:.3f}>{max_sum_asks:.3f}"
        reasons.append(f"sum_asks_ok:{sum_asks:.3f}<={max_sum_asks:.3f}")
    else:
        reasons.append("sum_asks_unrestricted")

    return True, ";".join(reasons)


def should_exit(
    cfg: DirectionalConfig,
    pos: PaperPosition,
    book: BookTop,
    btc_price: float | None,
    remaining_seconds: int,
) -> tuple[bool, str]:
    if book.bid is None:
        return False, "no_bid"
    if book.bid >= cfg.take_profit:
        return True, "take_profit"
    if book.bid <= cfg.hard_exit:
        return True, "hard_exit"
    if book.bid <= cfg.stop_loss:
        return True, "stop_loss"
    if remaining_seconds <= cfg.late_seconds:
        ok, reason = target_distance_ok(cfg, btc_price)
        if not ok:
            return True, f"late_exit:{reason}"
    return False, "hold"


def taker_fee_usdc(shares: float, price: float, fee_rate: float) -> float:
    if shares <= 0 or fee_rate <= 0:
        return 0.0
    return round(shares * fee_rate * price * (1.0 - price), 5)


def complete_set_taker_fee(first_price: float, second_price: float, fee_rate: float) -> float:
    return taker_fee_usdc(1.0, first_price, fee_rate) + taker_fee_usdc(1.0, second_price, fee_rate)


def record_trade(
    con: sqlite3.Connection,
    cfg: DirectionalConfig,
    pos: PaperPosition,
    exit_price: float,
    exit_reason: str,
    strategy: str = "directional_scalp",
) -> float:
    pnl = pos.shares * exit_price - pos.size_usd
    con.execute(
        """
        INSERT INTO paper_trades
        (strategy, label, market_slug, outcome, token_id, entry_ts, exit_ts, entry_price, exit_price, shares,
         size_usd, pnl, entry_reason, exit_reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            strategy,
            cfg.label,
            cfg.market_slug,
            cfg.outcome,
            cfg.token_id,
            pos.entry_ts,
            utc_now(),
            pos.entry_price,
            exit_price,
            pos.shares,
            pos.size_usd,
            pnl,
            pos.reason,
            exit_reason,
        ),
    )
    con.commit()
    return pnl


def record_arb_event(
    con: sqlite3.Connection,
    label: str,
    market_slug: str | None,
    kind: str,
    yes_price: float,
    no_price: float,
    edge: float,
    payload: dict[str, Any],
) -> None:
    con.execute(
        "INSERT INTO arb_events (ts, label, market_slug, kind, yes_price, no_price, edge, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (utc_now(), label, market_slug, kind, yes_price, no_price, edge, json.dumps(payload)),
    )
    con.commit()


def slug_expr() -> str:
    return "COALESCE(market_slug, substr(label, 1, instr(label || ' ', ' ') - 1))"


def outcome_expr() -> str:
    return """
    COALESCE(
        outcome,
        CASE
            WHEN label LIKE '% Up %' THEN 'Up'
            WHEN label LIKE '% Down %' THEN 'Down'
            WHEN label LIKE '% Yes %' THEN 'Yes'
            WHEN label LIKE '% No %' THEN 'No'
            ELSE 'Unknown'
        END
    )
    """


def preset_expr() -> str:
    return """
    CASE
        WHEN instr(label, 'preset:') > 0 THEN substr(label, instr(label, 'preset:') + 7)
        ELSE 'legacy'
    END
    """


def preset_name_map(run_metadata: dict[str, Any]) -> dict[str, str]:
    config = run_metadata.get("config") if isinstance(run_metadata.get("config"), dict) else {}
    presets = config.get("presets") if isinstance(config, dict) else None
    if not isinstance(presets, list):
        presets = DEFAULT_PRESETS
    names: dict[str, str] = {"legacy": "Legacy"}
    for preset in presets:
        if isinstance(preset, dict):
            key = str(preset.get("key") or "legacy")
            names[key] = str(preset.get("name") or key)
    return names


def add_preset_names(rows: list[dict[str, Any]], names: dict[str, str]) -> list[dict[str, Any]]:
    for row in rows:
        key = str(row.get("preset_key") or "legacy")
        row["preset_key"] = key
        row["preset_name"] = names.get(key, key)
    return rows


def watch_directional(cfg: DirectionalConfig, db_path: Path) -> None:
    con = init_db(db_path)
    delete_active_position(con, cfg)
    position: PaperPosition | None = None
    end_ts = utc_now() + cfg.seconds
    print(f"[{iso()}] paper watch started: {cfg.label}")
    print("No wallet. No private key. No real orders.")
    while utc_now() < end_ts:
        remaining = max(0, end_ts - utc_now())
        try:
            book = fetch_book(cfg.token_id)
            btc = fetch_btc_price() if cfg.target_price is not None else None
            save_snapshot(con, cfg.label, cfg.token_id, book, btc)
            if position is None:
                enter, reason = should_enter(cfg, book, btc, remaining)
                if enter and book.ask:
                    shares = cfg.size_usd / book.ask
                    position = PaperPosition(utc_now(), book.ask, shares, cfg.size_usd, reason)
                    upsert_active_position(con, cfg, position, book, btc)
                    print(
                        f"[{iso()}] PAPER ENTER ask={book.ask:.3f} shares={shares:.4f} "
                        f"btc={btc if btc is not None else 'n/a'} reason={reason}"
                    )
                else:
                    print(
                        f"[{iso()}] watch bid={book.bid} ask={book.ask} btc={btc if btc is not None else 'n/a'} "
                        f"remaining={remaining}s reason={reason}"
                    )
            else:
                exit_now, reason = should_exit(cfg, position, book, btc, remaining)
                if exit_now and book.bid is not None:
                    pnl = record_trade(con, cfg, position, book.bid, reason)
                    delete_active_position(con, cfg)
                    print(f"[{iso()}] PAPER EXIT bid={book.bid:.3f} pnl={pnl:.4f} reason={reason}")
                    position = None
                else:
                    upsert_active_position(con, cfg, position, book, btc)
                    unrealized = (position.shares * book.bid - position.size_usd) if book.bid else math.nan
                    print(
                        f"[{iso()}] hold bid={book.bid} ask={book.ask} unrealized={unrealized:.4f} "
                        f"remaining={remaining}s"
                    )
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, KeyError) as exc:
            print(f"[{iso()}] fetch/error: {exc}", file=sys.stderr)
        time.sleep(cfg.poll)
    delete_active_position(con, cfg)
    print(f"[{iso()}] paper watch ended: {cfg.label}")


def watch_arb(
    yes_token_id: str,
    no_token_id: str,
    label: str,
    buffer: float,
    fee_rate: float,
    poll: int,
    seconds: int,
    db_path: Path,
) -> None:
    con = init_db(db_path)
    active_fee_rate = resolve_fee_rate([yes_token_id, no_token_id], fee_rate)
    end_ts = utc_now() + seconds
    print(f"[{iso()}] arb watch started: {label}")
    print(f"Fee rate: {active_fee_rate:.6f}; extra buffer: {buffer:.6f}")
    while utc_now() < end_ts:
        try:
            yes = fetch_book(yes_token_id)
            no = fetch_book(no_token_id)
            payload = {"yes": yes.raw, "no": no.raw}
            if yes.ask is not None and no.ask is not None:
                fee = complete_set_taker_fee(yes.ask, no.ask, active_fee_rate)
                buy_total = yes.ask + no.ask + fee + buffer
                edge = 1.0 - buy_total
                if edge > 0:
                    record_arb_event(
                        con,
                        label,
                        None,
                        "buy_complete_set",
                        yes.ask,
                        no.ask,
                        edge,
                        payload | {"fee": fee, "buffer": buffer, "fee_rate": active_fee_rate},
                    )
                    print(f"[{iso()}] ARB buy both asks yes={yes.ask:.3f} no={no.ask:.3f} fee={fee:.5f} edge={edge:.4f}")
            if yes.bid is not None and no.bid is not None:
                fee = complete_set_taker_fee(yes.bid, no.bid, active_fee_rate)
                sell_total = yes.bid + no.bid - fee - buffer
                edge = sell_total - 1.0
                if edge > 0:
                    record_arb_event(
                        con,
                        label,
                        None,
                        "sell_complete_set",
                        yes.bid,
                        no.bid,
                        edge,
                        payload | {"fee": fee, "buffer": buffer, "fee_rate": active_fee_rate},
                    )
                    print(f"[{iso()}] ARB sell both bids yes={yes.bid:.3f} no={no.bid:.3f} fee={fee:.5f} edge={edge:.4f}")
            print(f"[{iso()}] arb watch yes_bid={yes.bid} yes_ask={yes.ask} no_bid={no.bid} no_ask={no.ask}")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, KeyError) as exc:
            print(f"[{iso()}] fetch/error: {exc}", file=sys.stderr)
        time.sleep(poll)
    print(f"[{iso()}] arb watch ended: {label}")


def outcome_direction(name: str, index: int) -> str:
    normalized = name.strip().lower()
    if normalized in {"up", "yes"}:
        return "UP"
    if normalized in {"down", "no"}:
        return "DOWN"
    return "UP" if index == 0 else "DOWN"


def default_end_ts(market: MarketInfo, requested_seconds: int | None, end_buffer_seconds: int) -> int:
    if requested_seconds is not None:
        return utc_now() + requested_seconds
    if market.end_ts is None:
        return utc_now() + 900
    return market.end_ts + end_buffer_seconds


def watch_url(
    url_or_slug: str,
    db_path: Path,
    seconds: int | None,
    poll: int,
    size_usd: float,
    entry_min: float,
    entry_max: float,
    take_profit: float,
    stop_loss: float,
    hard_exit: float,
    late_seconds: int,
    min_distance_usd: float,
    entry_window_seconds: int,
    max_trades_per_label_per_market: int,
    market_lock_after_loss: bool,
    no_entry_after_seconds: int,
    max_sum_asks: float,
    arb_buffer: float,
    fee_rate: float,
    end_buffer_seconds: int,
    lookup_timeout_seconds: int,
    presets: list[dict[str, Any]] | str | None = None,
) -> None:
    active_presets = (
        [preset for preset in normalize_presets({"presets": presets}) if preset["enabled"]]
        if presets is not None
        else [
            single_preset_from_args(
                entry_min=entry_min,
                entry_max=entry_max,
                take_profit=take_profit,
                stop_loss=stop_loss,
                hard_exit=hard_exit,
                late_seconds=late_seconds,
                min_distance_usd=min_distance_usd,
                max_trades_per_label_per_market=max_trades_per_label_per_market,
            )
        ]
    )
    needs_target = any(float(preset["min_distance_usd"]) > 0 for preset in active_presets)
    market = fetch_market_info_with_retry(url_or_slug, lookup_timeout_seconds, require_target=needs_target)
    if len(market.outcomes) < 2:
        raise ValueError("watch-url needs at least two outcomes")
    if market.target_price is None and needs_target:
        raise ValueError(
            "Market target price is unavailable from Polymarket metadata. "
            "Wait until eventMetadata.priceToBeat or /api/past-results is available, "
            "or set --min-distance-usd 0 to disable the distance filter."
        )
    active_fee_rate = resolve_fee_rate([outcome.token_id for outcome in market.outcomes], fee_rate)
    end_ts = default_end_ts(market, seconds, end_buffer_seconds)
    run_seconds = max(0, end_ts - utc_now())
    con = init_db(db_path)
    clear_active_positions(con, market.slug)
    positions: dict[str, PaperPosition | None] = {}
    trade_counts: dict[str, int] = {}
    market_locked = False
    configs: list[DirectionalConfig] = []
    preset_trade_limits = {
        str(preset["key"]): int(preset["max_trades_per_label_per_market"])
        for preset in active_presets
    }
    for preset in active_presets:
        for index, outcome in enumerate(market.outcomes):
            label = f"{market.slug} {outcome.name} preset:{preset['key']}"
            configs.append(
                DirectionalConfig(
                    token_id=outcome.token_id,
                    label=label,
                    preset_key=str(preset["key"]),
                    preset_name=str(preset["name"]),
                    market_slug=market.slug,
                    outcome=outcome.name,
                    target_price=market.target_price,
                    direction=outcome_direction(outcome.name, index),
                    size_usd=size_usd,
                    entry_min=float(preset["entry_min"]),
                    entry_max=float(preset["entry_max"]),
                    take_profit=float(preset["take_profit"]),
                    stop_loss=float(preset["stop_loss"]),
                    hard_exit=float(preset["hard_exit"]),
                    late_seconds=int(preset["late_seconds"]),
                    entry_window_seconds=max(0, entry_window_seconds) or None,
                    min_distance_usd=float(preset["min_distance_usd"]),
                    poll=poll,
                    seconds=run_seconds,
                )
            )
            positions[label] = None
            trade_counts[label] = 0
    print(f"[{iso()}] auto paper watch started: {market.title}")
    print(f"Slug: {market.slug}")
    if market.start_ts:
        print(f"Market start: {iso(market.start_ts)}")
    if market.end_ts:
        print(f"Market end: {iso(market.end_ts)}")
    print(f"Market target price: {market.target_price if market.target_price is not None else 'n/a'}")
    print(f"Watcher end: {iso(end_ts)}")
    print("Outcomes:")
    for outcome in market.outcomes:
        print(f"  {outcome.name}: {outcome.token_id}")
    print("Strategies: preset band_scalp for every outcome + complete_set_arb for first two outcomes")
    print("Preset settings:")
    for preset in active_presets:
        print(
            f"  {preset['key']}: entry={float(preset['entry_min']):.2f}-{float(preset['entry_max']):.2f} "
            f"tp={float(preset['take_profit']):.2f} sl={float(preset['stop_loss']):.2f} "
            f"hard={float(preset['hard_exit']):.2f} late={int(preset['late_seconds'])}s "
            f"dist={float(preset['min_distance_usd']):.1f} max_trades={int(preset['max_trades_per_label_per_market'])}"
        )
    print(
        f"Risk guards: max_trades_per_label={max_trades_per_label_per_market} "
        f"market_lock_after_loss={market_lock_after_loss} no_entry_after={no_entry_after_seconds}s "
        f"max_sum_asks={max_sum_asks:.3f}"
    )
    print(f"Fee rate: {active_fee_rate:.6f}; extra arb buffer: {arb_buffer:.6f}")
    print("No wallet. No private key. No real orders.")
    while utc_now() < end_ts:
        now_ts = utc_now()
        remaining = max(0, end_ts - now_ts)
        try:
            books = {token_id: fetch_book(token_id) for token_id in {cfg.token_id for cfg in configs}}
            btc = fetch_btc_price() if market.target_price is not None else None
            for cfg in configs:
                book = books[cfg.token_id]
                save_snapshot(con, cfg.label, cfg.token_id, book, btc)
                pos = positions[cfg.label]
                if pos is None:
                    opposite_book = None
                    if len(market.outcomes) == 2:
                        opposite_cfg = next(
                            item
                            for item in configs
                            if item.preset_key == cfg.preset_key and item.token_id != cfg.token_id
                        )
                        opposite_book = books.get(opposite_cfg.token_id)
                    precheck_ok, precheck_reason = auto_entry_precheck(
                        book=book,
                        opposite_book=opposite_book,
                        now_ts=now_ts,
                        market_start_ts=market.start_ts,
                        trade_count=trade_counts[cfg.label],
                        max_trades_per_label_per_market=max(0, preset_trade_limits[cfg.preset_key]),
                        market_locked=market_locked,
                        no_entry_after_seconds=max(0, no_entry_after_seconds),
                        max_sum_asks=max(0.0, max_sum_asks) if len(market.outcomes) == 2 else 0.0,
                    )
                    if not precheck_ok:
                        print(f"[{iso()}] WATCH {cfg.label} bid={book.bid} ask={book.ask} reason={precheck_reason}")
                        continue
                    enter, reason = should_enter(cfg, book, btc, remaining)
                    reason = f"{precheck_reason};{reason}"
                    if enter and book.ask:
                        shares = cfg.size_usd / book.ask
                        positions[cfg.label] = PaperPosition(utc_now(), book.ask, shares, cfg.size_usd, reason)
                        upsert_active_position(con, cfg, positions[cfg.label], book, btc)
                        print(f"[{iso()}] ENTER {cfg.label} ask={book.ask:.3f} shares={shares:.4f} reason={reason}")
                    else:
                        print(f"[{iso()}] WATCH {cfg.label} bid={book.bid} ask={book.ask} reason={reason}")
                else:
                    exit_now, reason = should_exit(cfg, pos, book, btc, remaining)
                    if exit_now and book.bid is not None:
                        pnl = record_trade(con, cfg, pos, book.bid, reason, strategy="auto_band_scalp")
                        delete_active_position(con, cfg)
                        positions[cfg.label] = None
                        trade_counts[cfg.label] += 1
                        if market_lock_after_loss and pnl < 0:
                            market_locked = True
                        print(f"[{iso()}] EXIT {cfg.label} bid={book.bid:.3f} pnl={pnl:.4f} reason={reason}")
                    else:
                        upsert_active_position(con, cfg, pos, book, btc)
                        unrealized = (pos.shares * book.bid - pos.size_usd) if book.bid else math.nan
                        print(f"[{iso()}] HOLD {cfg.label} bid={book.bid} unrealized={unrealized:.4f}")
            first, second = market.outcomes[0], market.outcomes[1]
            a = books[first.token_id]
            b = books[second.token_id]
            payload = {first.name: a.raw, second.name: b.raw}
            if a.ask is not None and b.ask is not None:
                fee = complete_set_taker_fee(a.ask, b.ask, active_fee_rate)
                edge = 1.0 - (a.ask + b.ask + fee + arb_buffer)
                if edge > 0:
                    record_arb_event(
                        con,
                        f"{market.slug} complete_set",
                        market.slug,
                        "buy_complete_set",
                        a.ask,
                        b.ask,
                        edge,
                        payload | {"fee": fee, "buffer": arb_buffer, "fee_rate": active_fee_rate},
                    )
                    print(f"[{iso()}] ARB buy both asks {first.name}={a.ask:.3f} {second.name}={b.ask:.3f} fee={fee:.5f} edge={edge:.4f}")
            if a.bid is not None and b.bid is not None:
                fee = complete_set_taker_fee(a.bid, b.bid, active_fee_rate)
                edge = (a.bid + b.bid - fee - arb_buffer) - 1.0
                if edge > 0:
                    record_arb_event(
                        con,
                        f"{market.slug} complete_set",
                        market.slug,
                        "sell_complete_set",
                        a.bid,
                        b.bid,
                        edge,
                        payload | {"fee": fee, "buffer": arb_buffer, "fee_rate": active_fee_rate},
                    )
                    print(f"[{iso()}] ARB sell both bids {first.name}={a.bid:.3f} {second.name}={b.bid:.3f} fee={fee:.5f} edge={edge:.4f}")
            print(f"[{iso()}] tick complete remaining={remaining}s")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, KeyError) as exc:
            print(f"[{iso()}] fetch/error: {exc}", file=sys.stderr)
        time.sleep(poll)
    clear_active_positions(con, market.slug)
    print(f"[{iso()}] auto paper watch ended: {market.title}")
    report(db_path, label_like=market.slug)


def watch_chain(
    url_or_slug: str,
    count: int,
    interval_seconds: int,
    db_path: Path,
    seconds: int | None,
    poll: int,
    size_usd: float,
    entry_min: float,
    entry_max: float,
    take_profit: float,
    stop_loss: float,
    hard_exit: float,
    late_seconds: int,
    min_distance_usd: float,
    entry_window_seconds: int,
    max_trades_per_label_per_market: int,
    market_lock_after_loss: bool,
    no_entry_after_seconds: int,
    max_sum_asks: float,
    arb_buffer: float,
    fee_rate: float,
    end_buffer_seconds: int,
    lookup_timeout_seconds: int,
    presets: list[dict[str, Any]] | str | None = None,
) -> None:
    if count < 1:
        raise ValueError("--chain-count must be at least 1")
    first_slug = parse_slug(url_or_slug)
    print(f"[{iso()}] chain started from {first_slug}; markets={count}; interval={interval_seconds}s")
    for index in range(count):
        current = url_or_slug_with_offset(url_or_slug, interval_seconds, index)
        current_slug = parse_slug(current)
        print(f"\n[{iso()}] chain item {index + 1}/{count}: {current_slug}")
        watch_url(
            url_or_slug=current,
            db_path=db_path,
            seconds=seconds,
            poll=poll,
            size_usd=size_usd,
            entry_min=entry_min,
            entry_max=entry_max,
            take_profit=take_profit,
            stop_loss=stop_loss,
            hard_exit=hard_exit,
            late_seconds=late_seconds,
            min_distance_usd=min_distance_usd,
            entry_window_seconds=entry_window_seconds,
            max_trades_per_label_per_market=max_trades_per_label_per_market,
            market_lock_after_loss=market_lock_after_loss,
            no_entry_after_seconds=no_entry_after_seconds,
            max_sum_asks=max_sum_asks,
            arb_buffer=arb_buffer,
            fee_rate=fee_rate,
            end_buffer_seconds=end_buffer_seconds,
            lookup_timeout_seconds=lookup_timeout_seconds,
            presets=presets,
        )
    print(f"\n[{iso()}] chain ended; aggregate summary:")
    summary(db_path, limit=max(20, count))


def report(db_path: Path, label_like: str | None = None) -> None:
    con = init_db(db_path)
    trade_where = ""
    trade_params: tuple[str, ...] = ()
    arb_where = ""
    arb_params: tuple[str, ...] = ()
    if label_like:
        trade_where = "WHERE label LIKE ?"
        trade_params = (f"%{label_like}%",)
        arb_where = "WHERE label LIKE ?"
        arb_params = (f"%{label_like}%",)
    rows = con.execute(
        f"""
        SELECT label, COUNT(*), SUM(pnl), AVG(pnl), MIN(pnl), MAX(pnl)
        FROM paper_trades
        {trade_where}
        GROUP BY label
        ORDER BY label
        """,
        trade_params,
    ).fetchall()
    print("Paper trades")
    if not rows:
        print("  No paper trades yet.")
    for label, count, total, avg, worst, best in rows:
        print(f"  {label}: trades={count} pnl={total:.4f} avg={avg:.4f} worst={worst:.4f} best={best:.4f}")
    exits = con.execute(
        f"""
        SELECT exit_reason, COUNT(*), SUM(pnl)
        FROM paper_trades
        {trade_where}
        GROUP BY exit_reason
        ORDER BY COUNT(*) DESC
        """,
        trade_params,
    ).fetchall()
    if exits:
        print("\nBy exit reason")
        for reason, count, total in exits:
            print(f"  {reason}: trades={count} pnl={total:.4f}")
    arb_rows = con.execute(
        f"""
        SELECT kind, COUNT(*), MAX(edge), AVG(edge)
        FROM arb_events
        {arb_where}
        GROUP BY kind
        """,
        arb_params,
    ).fetchall()
    print("\nArb events")
    if not arb_rows:
        print("  No arb events yet.")
    for kind, count, max_edge, avg_edge in arb_rows:
        print(f"  {kind}: events={count} max_edge={max_edge:.4f} avg_edge={avg_edge:.4f}")


def win_rate_expr() -> str:
    return "100.0 * SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) / COUNT(*)"


def summary(db_path: Path, limit: int, export_csv: Path | None = None) -> None:
    con = init_db(db_path)
    total = con.execute(
        """
        SELECT COUNT(*), COALESCE(SUM(pnl), 0), COALESCE(AVG(pnl), 0),
               COALESCE(MIN(pnl), 0), COALESCE(MAX(pnl), 0),
               COALESCE(SUM(size_usd), 0)
        FROM paper_trades
        """
    ).fetchone()
    count, pnl, avg, worst, best, volume = total
    print("Overall paper performance")
    if count == 0:
        print("  No paper trades yet.")
    else:
        win_rate = con.execute(f"SELECT {win_rate_expr()} FROM paper_trades").fetchone()[0]
        roi = (pnl / volume * 100.0) if volume else 0.0
        print(f"  trades={count} pnl={pnl:.4f} avg={avg:.4f} win_rate={win_rate:.1f}% roi_on_paper_size={roi:.1f}%")
        print(f"  worst={worst:.4f} best={best:.4f} paper_size={volume:.2f}")

    print("\nBy market")
    market_rows = con.execute(
        f"""
        SELECT {slug_expr()} AS slug, COUNT(*) AS trades, SUM(pnl) AS pnl,
               AVG(pnl) AS avg_pnl, {win_rate_expr()} AS win_rate,
               MIN(pnl) AS worst, MAX(pnl) AS best
        FROM paper_trades
        GROUP BY slug
        ORDER BY MAX(exit_ts) DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    if not market_rows:
        print("  No paper trades yet.")
    for slug, trades, row_pnl, row_avg, row_wr, row_worst, row_best in market_rows:
        print(
            f"  {slug}: trades={trades} pnl={row_pnl:.4f} avg={row_avg:.4f} "
            f"win_rate={row_wr:.1f}% worst={row_worst:.4f} best={row_best:.4f}"
        )

    print("\nBy outcome")
    outcome_rows = con.execute(
        f"""
        SELECT {outcome_expr()} AS out, COUNT(*) AS trades, SUM(pnl) AS pnl,
               AVG(pnl) AS avg_pnl, {win_rate_expr()} AS win_rate
        FROM paper_trades
        GROUP BY out
        ORDER BY pnl DESC
        """
    ).fetchall()
    if not outcome_rows:
        print("  No paper trades yet.")
    for out, trades, row_pnl, row_avg, row_wr in outcome_rows:
        print(f"  {out}: trades={trades} pnl={row_pnl:.4f} avg={row_avg:.4f} win_rate={row_wr:.1f}%")

    print("\nBy exit reason")
    exit_rows = con.execute(
        f"""
        SELECT exit_reason, COUNT(*) AS trades, SUM(pnl) AS pnl,
               AVG(pnl) AS avg_pnl, {win_rate_expr()} AS win_rate
        FROM paper_trades
        GROUP BY exit_reason
        ORDER BY trades DESC
        """
    ).fetchall()
    if not exit_rows:
        print("  No paper trades yet.")
    for reason, trades, row_pnl, row_avg, row_wr in exit_rows:
        print(f"  {reason}: trades={trades} pnl={row_pnl:.4f} avg={row_avg:.4f} win_rate={row_wr:.1f}%")

    print("\nArb summary")
    arb_rows = con.execute(
        f"""
        SELECT kind, COUNT(*) AS events, MAX(edge) AS max_edge, AVG(edge) AS avg_edge
        FROM arb_events
        GROUP BY kind
        ORDER BY events DESC
        """
    ).fetchall()
    if not arb_rows:
        print("  No arb events yet.")
    for kind, events, max_edge, avg_edge in arb_rows:
        print(f"  {kind}: events={events} max_edge={max_edge:.4f} avg_edge={avg_edge:.4f}")

    if export_csv:
        export_csv.parent.mkdir(parents=True, exist_ok=True)
        rows = con.execute(
            f"""
            SELECT {slug_expr()} AS market_slug, strategy, {outcome_expr()} AS outcome,
                   label, entry_ts, exit_ts, entry_price, exit_price, shares,
                   size_usd, pnl, entry_reason, exit_reason
            FROM paper_trades
            ORDER BY exit_ts
            """
        ).fetchall()
        headers = [
            "market_slug",
            "strategy",
            "outcome",
            "label",
            "entry_ts",
            "exit_ts",
            "entry_price",
            "exit_price",
            "shares",
            "size_usd",
            "pnl",
            "entry_reason",
            "exit_reason",
        ]
        with export_csv.open("w", encoding="utf-8", newline="") as file:
            file.write(",".join(headers) + "\n")
            for row in rows:
                file.write(",".join(csv_cell(value) for value in row) + "\n")
        print(f"\nExported trades CSV: {export_csv}")


def csv_cell(value: Any) -> str:
    text = "" if value is None else str(value)
    if any(char in text for char in [",", '"', "\n"]):
        text = '"' + text.replace('"', '""') + '"'
    return text


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def read_run_metadata(con: sqlite3.Connection) -> dict[str, Any]:
    try:
        rows = con.execute("SELECT key, value FROM run_metadata").fetchall()
    except sqlite3.OperationalError:
        return {}
    metadata: dict[str, Any] = {}
    for row in rows:
        value = row["value"] if isinstance(row, sqlite3.Row) else row[1]
        key = row["key"] if isinstance(row, sqlite3.Row) else row[0]
        try:
            metadata[str(key)] = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            metadata[str(key)] = value
    return metadata


def empty_dashboard_payload(db_path: Path) -> dict[str, Any]:
    return {
        "generated_at": iso(),
        "db_path": str(db_path),
        "run": {},
        "overall": {
            "trades": 0,
            "pnl": 0.0,
            "avg_pnl": 0.0,
            "worst": 0.0,
            "best": 0.0,
            "paper_size": 0.0,
            "markets": 0,
            "win_rate": 0.0,
            "roi": 0.0,
            "active_positions": 0,
            "active_unrealized_pnl": 0.0,
            "active_size_usd": 0.0,
            "total_with_active_pnl": 0.0,
        },
        "equity": [],
        "preset_equity": [],
        "preset_summary": [],
        "markets": [],
        "outcomes": [],
        "exits": [],
        "trades": [],
        "active_positions": [],
        "snapshots": [],
        "arb": [],
        "arb_events": [],
        "health": {
            "hard_exit_count": 0,
            "stop_loss_count": 0,
            "top_3_pnl": 0.0,
            "top_3_share": 0.0,
            "largest_loss": 0.0,
        },
    }


def dashboard_payload(db_path: Path) -> dict[str, Any]:
    if db_path == empty_dashboard_db_path() and not db_path.exists():
        return empty_dashboard_payload(db_path)
    con = init_db(db_path)
    con.row_factory = sqlite3.Row
    run_metadata = read_run_metadata(con)
    preset_names = preset_name_map(run_metadata)
    total = con.execute(
        """
        SELECT COUNT(*) AS trades,
               COALESCE(SUM(pnl), 0) AS pnl,
               COALESCE(AVG(pnl), 0) AS avg_pnl,
               COALESCE(MIN(pnl), 0) AS worst,
               COALESCE(MAX(pnl), 0) AS best,
               COALESCE(SUM(size_usd), 0) AS paper_size,
               COUNT(DISTINCT COALESCE(market_slug, substr(label, 1, instr(label || ' ', ' ') - 1))) AS markets
        FROM paper_trades
        """
    ).fetchone()
    win_rate = con.execute(f"SELECT COALESCE({win_rate_expr()}, 0) AS win_rate FROM paper_trades").fetchone()["win_rate"]
    overall = dict(total)
    overall["win_rate"] = win_rate
    overall["roi"] = (overall["pnl"] / overall["paper_size"] * 100.0) if overall["paper_size"] else 0.0

    equity_rows = con.execute(
        f"""
        SELECT exit_ts AS ts, pnl, {slug_expr()} AS market_slug, {outcome_expr()} AS outcome,
               {preset_expr()} AS preset_key, exit_reason
        FROM paper_trades
        ORDER BY exit_ts, id
        """
    ).fetchall()
    cumulative = 0.0
    preset_totals: dict[str, float] = {}
    equity: list[dict[str, Any]] = []
    preset_equity: list[dict[str, Any]] = []
    for row in equity_rows:
        cumulative += float(row["pnl"])
        item = dict(row)
        item["cumulative"] = cumulative
        item["time"] = iso(int(row["ts"])) if row["ts"] else ""
        item["preset_name"] = preset_names.get(str(row["preset_key"]), str(row["preset_key"]))
        equity.append(item)
        preset_key = str(row["preset_key"])
        preset_totals[preset_key] = preset_totals.get(preset_key, 0.0) + float(row["pnl"])
        preset_item = dict(item)
        preset_item["cumulative"] = preset_totals[preset_key]
        preset_equity.append(preset_item)

    preset_summary = rows_to_dicts(
        con.execute(
            f"""
            SELECT {preset_expr()} AS preset_key, COUNT(*) AS trades, SUM(pnl) AS pnl,
                   AVG(pnl) AS avg_pnl, {win_rate_expr()} AS win_rate,
                   AVG(entry_price) AS avg_entry, AVG(exit_price) AS avg_exit,
                   MIN(pnl) AS worst, MAX(pnl) AS best,
                   SUM(CASE WHEN exit_reason IN ('hard_exit', 'stop_loss') THEN 1 ELSE 0 END) AS risk_exits
            FROM paper_trades
            GROUP BY preset_key
            ORDER BY pnl DESC
            """
        ).fetchall()
    )
    add_preset_names(preset_summary, preset_names)

    markets = rows_to_dicts(
        con.execute(
            f"""
            SELECT {slug_expr()} AS market_slug, {preset_expr()} AS preset_key,
                   COUNT(*) AS trades, SUM(pnl) AS pnl,
                   AVG(pnl) AS avg_pnl, {win_rate_expr()} AS win_rate,
                   MIN(pnl) AS worst, MAX(pnl) AS best, MAX(exit_ts) AS last_exit
            FROM paper_trades
            GROUP BY market_slug, preset_key
            ORDER BY MAX(exit_ts) DESC
            LIMIT 50
            """
        ).fetchall()
    )
    add_preset_names(markets, preset_names)
    outcomes = rows_to_dicts(
        con.execute(
            f"""
            SELECT {outcome_expr()} AS outcome, {preset_expr()} AS preset_key,
                   COUNT(*) AS trades, SUM(pnl) AS pnl,
                   AVG(pnl) AS avg_pnl, {win_rate_expr()} AS win_rate,
                   AVG(entry_price) AS avg_entry, AVG(exit_price) AS avg_exit,
                   SUM(CASE WHEN exit_reason IN ('hard_exit', 'stop_loss') THEN 1 ELSE 0 END) AS risk_exits
            FROM paper_trades
            GROUP BY outcome, preset_key
            ORDER BY pnl DESC
            """
        ).fetchall()
    )
    add_preset_names(outcomes, preset_names)
    exits = rows_to_dicts(
        con.execute(
            f"""
            SELECT exit_reason, COUNT(*) AS trades, SUM(pnl) AS pnl,
                   AVG(pnl) AS avg_pnl, {win_rate_expr()} AS win_rate
            FROM paper_trades
            GROUP BY exit_reason
            ORDER BY trades DESC
            """
        ).fetchall()
    )
    trades = rows_to_dicts(
        con.execute(
            f"""
            SELECT {slug_expr()} AS market_slug, strategy, label, {preset_expr()} AS preset_key,
                   {outcome_expr()} AS outcome,
                   entry_ts, exit_ts, entry_price, exit_price, shares, size_usd, pnl,
                   entry_reason, exit_reason
            FROM paper_trades
            ORDER BY exit_ts DESC, id DESC
            LIMIT 250
            """
        ).fetchall()
    )
    for trade in trades:
        trade["entry_time"] = iso(int(trade["entry_ts"])) if trade["entry_ts"] else ""
        trade["exit_time"] = iso(int(trade["exit_ts"])) if trade["exit_ts"] else ""
        trade["hold_seconds"] = int(trade["exit_ts"] or 0) - int(trade["entry_ts"] or 0)
    add_preset_names(trades, preset_names)

    active_positions = rows_to_dicts(
        con.execute(
            """
            SELECT label,
                   CASE
                       WHEN instr(label, 'preset:') > 0 THEN substr(label, instr(label, 'preset:') + 7)
                       ELSE 'legacy'
                   END AS preset_key,
                   market_slug, outcome, token_id, entry_ts, updated_ts, entry_price, bid, ask,
                   shares, size_usd, unrealized_pnl, unrealized_roi, btc_price, entry_reason
            FROM active_positions
            ORDER BY updated_ts DESC
            LIMIT 50
            """
        ).fetchall()
    )
    for active in active_positions:
        active["entry_time"] = iso(int(active["entry_ts"])) if active["entry_ts"] else ""
        active["updated_time"] = iso(int(active["updated_ts"])) if active["updated_ts"] else ""
        active["hold_seconds"] = int(active["updated_ts"] or 0) - int(active["entry_ts"] or 0)
    add_preset_names(active_positions, preset_names)
    active_unrealized_pnl = sum(
        float(row["unrealized_pnl"])
        for row in active_positions
        if row.get("unrealized_pnl") is not None
    )
    active_size_usd = sum(float(row["size_usd"]) for row in active_positions if row.get("size_usd") is not None)
    overall["active_positions"] = len(active_positions)
    overall["active_unrealized_pnl"] = active_unrealized_pnl
    overall["active_size_usd"] = active_size_usd
    overall["total_with_active_pnl"] = overall["pnl"] + active_unrealized_pnl

    latest_snapshots = rows_to_dicts(
        con.execute(
            """
            SELECT s.ts, s.label,
                   CASE
                       WHEN instr(s.label, 'preset:') > 0 THEN substr(s.label, instr(s.label, 'preset:') + 7)
                       ELSE 'legacy'
                   END AS preset_key,
                   s.token_id, s.bid, s.ask, s.last, s.bid_size, s.ask_size, s.btc_price
            FROM snapshots s
            JOIN (
                SELECT label, token_id, MAX(ts) AS max_ts
                FROM snapshots
                GROUP BY label, token_id
            ) latest
              ON latest.label = s.label AND latest.token_id = s.token_id AND latest.max_ts = s.ts
            ORDER BY s.ts DESC
            LIMIT 40
            """
        ).fetchall()
    )
    for snap in latest_snapshots:
        snap["time"] = iso(int(snap["ts"])) if snap["ts"] else ""
    add_preset_names(latest_snapshots, preset_names)

    arb = rows_to_dicts(
        con.execute(
            """
            SELECT kind, COUNT(*) AS events, MAX(edge) AS max_edge, AVG(edge) AS avg_edge
            FROM arb_events
            GROUP BY kind
            ORDER BY events DESC
            """
        ).fetchall()
    )
    arb_events = rows_to_dicts(
        con.execute(
            """
            SELECT ts, COALESCE(market_slug, substr(label, 1, instr(label || ' ', ' ') - 1)) AS market_slug,
                   kind, yes_price, no_price, edge
            FROM arb_events
            ORDER BY ts DESC, id DESC
            LIMIT 100
            """
        ).fetchall()
    )
    for event in arb_events:
        event["time"] = iso(int(event["ts"])) if event["ts"] else ""

    hard_exit_count = sum(int(row["trades"]) for row in exits if row["exit_reason"] == "hard_exit")
    stop_loss_count = sum(int(row["trades"]) for row in exits if row["exit_reason"] == "stop_loss")
    top_pnl = con.execute("SELECT pnl FROM paper_trades ORDER BY pnl DESC LIMIT 3").fetchall()
    top_sum = sum(float(row["pnl"]) for row in top_pnl)
    health = {
        "hard_exit_count": hard_exit_count,
        "stop_loss_count": stop_loss_count,
        "top_3_pnl": top_sum,
        "top_3_share": (top_sum / overall["pnl"] * 100.0) if overall["pnl"] else 0.0,
        "largest_loss": overall["worst"],
    }
    payload = {
        "generated_at": iso(),
        "db_path": str(db_path),
        "run": run_metadata,
        "overall": overall,
        "equity": equity,
        "preset_equity": preset_equity,
        "preset_summary": preset_summary,
        "markets": markets,
        "outcomes": outcomes,
        "exits": exits,
        "trades": trades,
        "active_positions": active_positions,
        "snapshots": latest_snapshots,
        "arb": arb,
        "arb_events": arb_events,
        "health": health,
    }
    con.close()
    return payload


BOT_LOCK = threading.Lock()
BOT_PROCESS: subprocess.Popen[bytes] | None = None
BOT_STARTED_AT: int | None = None
BOT_CONFIG: dict[str, Any] | None = None
BOT_DB_PATH: Path | None = None
DASHBOARD_DB_PATH: Path = DEFAULT_DB


def optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def bounded_float(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def preset_defaults_by_key() -> dict[str, dict[str, Any]]:
    return {str(preset["key"]): dict(preset) for preset in DEFAULT_PRESETS}


def parse_presets_value(value: Any) -> list[dict[str, Any]] | None:
    if value in (None, ""):
        return None
    if isinstance(value, list):
        raw = value
    elif isinstance(value, str):
        parsed = json.loads(value)
        if not isinstance(parsed, list):
            raise ValueError("presets must be a JSON list")
        raw = parsed
    else:
        raise ValueError("presets must be a list")
    presets: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("each preset must be an object")
        presets.append(item)
    return presets


def normalize_presets(payload: dict[str, Any]) -> list[dict[str, Any]]:
    defaults = DEFAULT_PRESETS
    submitted = parse_presets_value(payload.get("presets"))
    legacy_fields = {
        "entry_min",
        "entry_max",
        "take_profit",
        "stop_loss",
        "hard_exit",
        "late_seconds",
        "min_distance_usd",
        "max_trades_per_label_per_market",
    }
    if submitted is None and any(key in payload and payload.get(key) not in (None, "") for key in legacy_fields):
        submitted = [
            single_preset_from_args(
                entry_min=bounded_float(payload.get("entry_min"), DEFAULT_ENTRY_MIN, 0.01, 0.99),
                entry_max=bounded_float(payload.get("entry_max"), DEFAULT_ENTRY_MAX, 0.01, 0.99),
                take_profit=bounded_float(payload.get("take_profit"), DEFAULT_TAKE_PROFIT, 0.01, 0.99),
                stop_loss=bounded_float(payload.get("stop_loss"), DEFAULT_STOP_LOSS, 0.01, 0.99),
                hard_exit=bounded_float(payload.get("hard_exit"), DEFAULT_HARD_EXIT, 0.01, 0.99),
                late_seconds=bounded_int(payload.get("late_seconds"), DEFAULT_LATE_SECONDS, 0, 300),
                min_distance_usd=bounded_float(payload.get("min_distance_usd"), DEFAULT_MIN_DISTANCE_USD, 0.0, 100_000.0),
                max_trades_per_label_per_market=bounded_int(
                    payload.get("max_trades_per_label_per_market"),
                    DEFAULT_MAX_TRADES_PER_LABEL_PER_MARKET,
                    0,
                    100,
                ),
            )
        ]
    if submitted is None:
        submitted = defaults
    by_key = preset_defaults_by_key()
    presets: list[dict[str, Any]] = []
    used: set[str] = set()
    for index, item in enumerate(submitted):
        raw_key = str(item.get("key") or f"preset{index + 1}").strip().lower()
        key = re.sub(r"[^a-z0-9_-]+", "_", raw_key).strip("_") or f"preset{index + 1}"
        if key in used:
            key = f"{key}_{index + 1}"
        used.add(key)
        fallback = by_key.get(raw_key, defaults[min(index, len(defaults) - 1)])
        preset = {
            "key": key,
            "name": str(item.get("name") or fallback.get("name") or key).strip() or key,
            "enabled": bool_value(item.get("enabled"), bool(fallback.get("enabled", True))),
            "entry_min": bounded_float(item.get("entry_min"), float(fallback["entry_min"]), 0.01, 0.99),
            "entry_max": bounded_float(item.get("entry_max"), float(fallback["entry_max"]), 0.01, 0.99),
            "take_profit": bounded_float(item.get("take_profit"), float(fallback["take_profit"]), 0.01, 0.99),
            "stop_loss": bounded_float(item.get("stop_loss"), float(fallback["stop_loss"]), 0.01, 0.99),
            "hard_exit": bounded_float(item.get("hard_exit"), float(fallback["hard_exit"]), 0.01, 0.99),
            "late_seconds": bounded_int(item.get("late_seconds"), int(fallback["late_seconds"]), 0, 300),
            "min_distance_usd": bounded_float(
                item.get("min_distance_usd"),
                float(fallback["min_distance_usd"]),
                0.0,
                100_000.0,
            ),
            "max_trades_per_label_per_market": bounded_int(
                item.get("max_trades_per_label_per_market"),
                int(fallback["max_trades_per_label_per_market"]),
                0,
                100,
            ),
        }
        presets.append(preset)
    if not any(preset["enabled"] for preset in presets):
        raise ValueError("At least one preset must be enabled")
    return presets


def primary_preset(config: dict[str, Any]) -> dict[str, Any]:
    presets = config.get("presets") or DEFAULT_PRESETS
    for preset in presets:
        if preset.get("enabled", True):
            return preset
    return presets[0]


def single_preset_from_args(
    entry_min: float,
    entry_max: float,
    take_profit: float,
    stop_loss: float,
    hard_exit: float,
    late_seconds: int,
    min_distance_usd: float,
    max_trades_per_label_per_market: int,
) -> dict[str, Any]:
    return {
        "key": "custom",
        "name": "Custom",
        "enabled": True,
        "entry_min": entry_min,
        "entry_max": entry_max,
        "take_profit": take_profit,
        "stop_loss": stop_loss,
        "hard_exit": hard_exit,
        "late_seconds": late_seconds,
        "min_distance_usd": min_distance_usd,
        "max_trades_per_label_per_market": max_trades_per_label_per_market,
    }


def bool_value(value: Any, default: bool) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def compact_number(value: Any, digits: int = 3) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    text = f"{number:.{digits}f}".rstrip("0").rstrip(".")
    return text or "0"


def safe_filename_segment(value: str, max_len: int = 96) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    cleaned = re.sub(r"-+", "-", cleaned).strip("-._")
    return (cleaned or "run")[:max_len].strip("-._") or "run"


def strategy_label(config: dict[str, Any]) -> str:
    lock = "lock" if config.get("market_lock_after_loss") else "nolock"
    preset = primary_preset(config)
    enabled = [item for item in config.get("presets", []) if item.get("enabled", True)]
    preset_part = (
        "multi_" + "-".join(safe_filename_segment(str(item.get("key", "preset")), max_len=14) for item in enabled)
        if len(enabled) > 1
        else safe_filename_segment(str(preset.get("key", "preset")), max_len=24)
    )
    return (
        f"{preset_part}_e{compact_number(preset.get('entry_min'))}-{compact_number(preset.get('entry_max'))}"
        f"_tp{compact_number(preset.get('take_profit'))}"
        f"_sl{compact_number(preset.get('stop_loss'))}"
        f"_hard{compact_number(preset.get('hard_exit'))}"
        f"_dist{compact_number(preset.get('min_distance_usd'), 1)}"
        f"_cut{config.get('no_entry_after_seconds')}"
        f"_sum{compact_number(config.get('max_sum_asks'))}"
        f"_{lock}"
    )


def new_run_db_path(config: dict[str, Any], started_ts: int) -> Path:
    DEFAULT_RUNS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.fromtimestamp(started_ts, tz=timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    try:
        source_slug = parse_slug(str(config.get("url", "")))
    except ValueError:
        source_slug = "source"
    source = safe_filename_segment(source_slug, max_len=48)
    strategy = safe_filename_segment(strategy_label(config), max_len=112)
    base = f"{timestamp}_{source}_{strategy}"
    path = DEFAULT_RUNS_DIR / f"{base}.sqlite"
    suffix = 2
    while path.exists():
        path = DEFAULT_RUNS_DIR / f"{base}_{suffix}.sqlite"
        suffix += 1
    return path


def empty_dashboard_db_path() -> Path:
    return DEFAULT_RUNS_DIR / "_dashboard_empty.sqlite"


def latest_run_db_path() -> Path | None:
    if not DEFAULT_RUNS_DIR.exists():
        return None
    candidates = [
        path
        for path in DEFAULT_RUNS_DIR.glob("*.sqlite")
        if path.is_file() and not path.name.startswith("_")
    ]
    if not candidates:
        return None

    def run_sort_key(path: Path) -> tuple[str, float, str]:
        match = re.match(r"^(\d{8}_\d{6}Z)", path.name)
        timestamp = match.group(1) if match else ""
        return timestamp, path.stat().st_mtime, path.name

    return max(candidates, key=run_sort_key)


def initial_dashboard_db_path(db_path: Path) -> Path:
    if db_path != DEFAULT_DB:
        return db_path
    return latest_run_db_path() or empty_dashboard_db_path()


def write_run_metadata(db_path: Path, metadata: dict[str, Any]) -> None:
    con = init_db(db_path)
    try:
        con.executemany(
            """
            INSERT INTO run_metadata (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            [(key, json.dumps(value, ensure_ascii=False, separators=(",", ":"))) for key, value in metadata.items()],
        )
        con.commit()
    finally:
        con.close()


def current_dashboard_db_path() -> Path:
    return DASHBOARD_DB_PATH


def tail_file(path: Path, max_lines: int = 120) -> list[str]:
    if not path.exists():
        return []
    try:
        with path.open("rb") as file:
            file.seek(0, os.SEEK_END)
            size = file.tell()
            file.seek(max(0, size - 64_000))
            text = file.read().decode("utf-8", errors="replace")
    except OSError:
        return []
    return text.splitlines()[-max_lines:]


def clear_bot_logs() -> None:
    DEFAULT_BOT_STDOUT.parent.mkdir(parents=True, exist_ok=True)
    DEFAULT_BOT_STDOUT.write_text("", encoding="utf-8")
    DEFAULT_BOT_STDERR.write_text("", encoding="utf-8")


def bot_running_unlocked() -> bool:
    return BOT_PROCESS is not None and BOT_PROCESS.poll() is None


def discover_bot_process_ids() -> list[int]:
    if os.name != "nt":
        return []
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                (
                    "Get-CimInstance Win32_Process -Filter \"name = 'python.exe'\" | "
                    "Where-Object { $_.ProcessId -ne $PID -and $_.CommandLine -like '*paper_bot.py*' "
                    "-and ($_.CommandLine -match ' watch-url | watch-directional | watch-arb ') } | "
                    "Select-Object -ExpandProperty ProcessId"
                ),
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if result.returncode != 0:
        return []
    pids: list[int] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            pid = int(line)
        except ValueError:
            continue
        if pid != os.getpid() and pid not in pids:
            pids.append(pid)
    return pids


def stop_process_id(pid: int) -> None:
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, timeout=8)
    else:
        os.kill(pid, 15)


def bot_command(config: dict[str, Any], db_path: Path) -> list[str]:
    preset = primary_preset(config)
    args = [
        sys.executable,
        "-u",
        str(Path(__file__).resolve()),
        "--db",
        str(db_path),
        "watch-url",
        config["url"],
        "--poll",
        str(config["poll"]),
        "--size-usd",
        str(config["size_usd"]),
        "--entry-min",
        str(preset["entry_min"]),
        "--entry-max",
        str(preset["entry_max"]),
        "--take-profit",
        str(preset["take_profit"]),
        "--stop-loss",
        str(preset["stop_loss"]),
        "--hard-exit",
        str(preset["hard_exit"]),
        "--late-seconds",
        str(preset["late_seconds"]),
        "--min-distance-usd",
        str(preset["min_distance_usd"]),
        "--entry-window-seconds",
        str(config["entry_window_seconds"]),
        "--no-entry-after-seconds",
        str(config["no_entry_after_seconds"]),
        "--max-sum-asks",
        str(config["max_sum_asks"]),
        "--arb-buffer",
        str(config["arb_buffer"]),
        "--fee-rate",
        str(config["fee_rate"]),
        "--end-buffer-seconds",
        str(config["end_buffer_seconds"]),
        "--chain-count",
        str(config["chain_count"]),
        "--chain-interval-seconds",
        str(config["chain_interval_seconds"]),
        "--lookup-timeout-seconds",
        str(config["lookup_timeout_seconds"]),
    ]
    args.extend(["--presets", json.dumps(config["presets"], separators=(",", ":"))])
    args.append("--market-lock-after-loss" if config["market_lock_after_loss"] else "--no-market-lock-after-loss")
    if config.get("seconds") is not None:
        args.extend(["--seconds", str(config["seconds"])])
    return args


def normalize_bot_config(payload: dict[str, Any]) -> dict[str, Any]:
    url = str(payload.get("url") or "").strip()
    if not url:
        raise ValueError("URL or slug is required")
    presets = normalize_presets(payload)
    preset = next((item for item in presets if item["enabled"]), presets[0])
    return {
        "url": url,
        "poll": bounded_int(payload.get("poll"), DEFAULT_POLL_SECONDS, 1, 60),
        "chain_count": bounded_int(payload.get("chain_count"), DEFAULT_CHAIN_COUNT, 1, 100),
        "chain_interval_seconds": bounded_int(payload.get("chain_interval_seconds"), 300, 1, 3600),
        "lookup_timeout_seconds": bounded_int(payload.get("lookup_timeout_seconds"), 60, 1, 600),
        "end_buffer_seconds": bounded_int(payload.get("end_buffer_seconds"), 1, 0, 60),
        "seconds": optional_int(payload.get("seconds")),
        "size_usd": bounded_float(payload.get("size_usd"), 1.0, 0.01, 10_000.0),
        "entry_min": preset["entry_min"],
        "entry_max": preset["entry_max"],
        "take_profit": preset["take_profit"],
        "stop_loss": preset["stop_loss"],
        "hard_exit": preset["hard_exit"],
        "late_seconds": preset["late_seconds"],
        "min_distance_usd": preset["min_distance_usd"],
        "presets": presets,
        "entry_window_seconds": bounded_int(
            payload.get("entry_window_seconds"),
            DEFAULT_ENTRY_WINDOW_SECONDS,
            0,
            3600,
        ),
        "max_trades_per_label_per_market": bounded_int(
            payload.get("max_trades_per_label_per_market"),
            preset["max_trades_per_label_per_market"],
            0,
            100,
        ),
        "market_lock_after_loss": bool_value(payload.get("market_lock_after_loss"), DEFAULT_MARKET_LOCK_AFTER_LOSS),
        "no_entry_after_seconds": bounded_int(
            payload.get("no_entry_after_seconds"),
            DEFAULT_NO_ENTRY_AFTER_SECONDS,
            0,
            3600,
        ),
        "max_sum_asks": bounded_float(payload.get("max_sum_asks"), DEFAULT_MAX_SUM_ASKS, 0.0, 2.0),
        "arb_buffer": bounded_float(payload.get("arb_buffer"), 0.01, 0.0, 1.0),
        "fee_rate": bounded_float(payload.get("fee_rate"), DEFAULT_TAKER_FEE_RATE, 0.0, 1.0),
    }


def start_bot(payload: dict[str, Any], db_path: Path) -> dict[str, Any]:
    global BOT_PROCESS, BOT_STARTED_AT, BOT_CONFIG, BOT_DB_PATH, DASHBOARD_DB_PATH
    config = normalize_bot_config(payload)
    with BOT_LOCK:
        if bot_running_unlocked():
            raise RuntimeError("Bot is already running")
        started_ts = utc_now()
        run_db_path = new_run_db_path(config, started_ts) if db_path == DEFAULT_DB else db_path
        run_metadata = {
            "run_id": run_db_path.stem,
            "started_at": iso(started_ts),
            "source": config["url"],
            "strategy": strategy_label(config),
            "db_path": str(run_db_path),
            "config": config,
        }
        write_run_metadata(run_db_path, run_metadata)
        clear_bot_logs()
        command = bot_command(config, run_db_path)
        stdout = DEFAULT_BOT_STDOUT.open("ab")
        stderr = DEFAULT_BOT_STDERR.open("ab")
        try:
            BOT_PROCESS = subprocess.Popen(
                command,
                cwd=ROOT,
                stdout=stdout,
                stderr=stderr,
            )
        finally:
            stdout.close()
            stderr.close()
        BOT_STARTED_AT = started_ts
        BOT_CONFIG = config
        BOT_DB_PATH = run_db_path
        DASHBOARD_DB_PATH = run_db_path
    return bot_status()


def stop_bot() -> dict[str, Any]:
    global BOT_PROCESS
    with BOT_LOCK:
        process = BOT_PROCESS
        if process is None or process.poll() is not None:
            process = None
    stopped_pids: set[int] = set()
    if process is not None:
        try:
            process.terminate()
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=8)
        stopped_pids.add(process.pid)
    for pid in discover_bot_process_ids():
        if pid in stopped_pids:
            continue
        try:
            stop_process_id(pid)
            stopped_pids.add(pid)
        except (OSError, subprocess.SubprocessError):
            continue
    with BOT_LOCK:
        if BOT_PROCESS is not None and BOT_PROCESS.poll() is not None:
            BOT_PROCESS = None
    return bot_status()


def reset_data(db_path: Path) -> dict[str, Any]:
    with BOT_LOCK:
        if bot_running_unlocked():
            raise RuntimeError("Stop the bot before clearing data")
    db_path = current_dashboard_db_path()
    if db_path.exists():
        db_path.unlink()
    if db_path == empty_dashboard_db_path():
        return bot_status()
    init_db(db_path).close()
    return bot_status()


def bot_status() -> dict[str, Any]:
    with BOT_LOCK:
        process = BOT_PROCESS
        running = bot_running_unlocked()
        pid = process.pid if process is not None else None
        returncode = process.poll() if process is not None else None
        config = dict(BOT_CONFIG or {})
        started_at = iso(BOT_STARTED_AT) if BOT_STARTED_AT else None
        db_path = BOT_DB_PATH or DASHBOARD_DB_PATH
    orphan_pids = [] if running else discover_bot_process_ids()
    if orphan_pids:
        running = True
        pid = orphan_pids[0]
        returncode = None
    run_metadata: dict[str, Any] = {}
    if db_path.exists():
        con = init_db(db_path)
        con.row_factory = sqlite3.Row
        try:
            run_metadata = read_run_metadata(con)
        finally:
            con.close()
    return {
        "running": running,
        "pid": pid,
        "returncode": returncode,
        "started_at": started_at,
        "config": config,
        "db_path": str(db_path),
        "data_file": None if db_path == empty_dashboard_db_path() and not run_metadata else db_path.name,
        "run": run_metadata,
        "stdout_log": str(DEFAULT_BOT_STDOUT),
        "stderr_log": str(DEFAULT_BOT_STDERR),
        "stdout_tail": tail_file(DEFAULT_BOT_STDOUT) if BOT_STARTED_AT or running else [],
        "stderr_tail": tail_file(DEFAULT_BOT_STDERR) if BOT_STARTED_AT or running else [],
    }


DASHBOARD_HTML_OLD = r"""<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Bảng điều khiển Polymarket Paper</title>
  <style>
    :root {
      --bg: #0d1110;
      --bg-2: #101716;
      --surface: #151b1a;
      --surface-2: #1b2422;
      --ink: #eef4f1;
      --muted: #91a09b;
      --border: #2a3633;
      --accent: #1aa896;
      --accent-2: #d6a93d;
      --good: #49d18f;
      --bad: #ff6b6b;
      --warn: #f4bd50;
      --info: #6db7ff;
      --soft: #1c2a27;
      --line: #24302d;
      --terminal: #090d0c;
      --radius: 8px;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100dvh; background: radial-gradient(circle at top left, #14201d 0, var(--bg) 420px); color: var(--ink); }
    header { position: sticky; top: 0; z-index: 2; background: rgba(13,17,16,.9); border-bottom: 1px solid var(--border); backdrop-filter: blur(14px); }
    .wrap { width: min(100% - 48px, 1880px); margin: 0 auto; padding: 14px 0; }
    .topbar { display: flex; justify-content: space-between; gap: 16px; align-items: center; }
    h1 { font-size: 22px; margin: 0; line-height: 1.15; letter-spacing: 0; }
    .subtle { color: var(--muted); font-size: 13px; }
    .statusline { display: flex; gap: 10px; align-items: center; justify-content: flex-end; flex-wrap: wrap; }
    .dot { width: 9px; height: 9px; border-radius: 50%; background: #60706a; display: inline-block; }
    .dot.on { background: var(--good); box-shadow: 0 0 0 4px rgba(22,118,79,.12); }
    .tabs { display: flex; gap: 6px; margin-top: 16px; }
    .tab { border: 1px solid var(--border); background: var(--surface); color: var(--muted); padding: 8px 12px; border-radius: var(--radius); cursor: pointer; font-size: 14px; }
    .tab:hover { color: var(--ink); background: var(--surface-2); }
    .tab.active { background: var(--accent); color: #06100e; border-color: var(--accent); }
    main { width: min(100% - 48px, 1880px); margin: 0 auto; padding: 16px 0 40px; }
    .grid { display: grid; gap: 12px; }
    .metrics { grid-template-columns: repeat(4, minmax(0, 1fr)); }
    .two { grid-template-columns: minmax(0, 1.4fr) minmax(320px, .8fr); }
    .run-grid { grid-template-columns: minmax(620px, .95fr) minmax(760px, 1.25fr); align-items: start; }
    .panel { background: linear-gradient(180deg, rgba(27,36,34,.96), rgba(21,27,26,.96)); border: 1px solid var(--border); border-radius: var(--radius); padding: 14px; box-shadow: inset 0 1px 0 rgba(255,255,255,.03), 0 18px 40px rgba(0,0,0,.22); }
    .metric .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
    .metric .value { font-size: 24px; margin-top: 8px; font-weight: 680; }
    .pos { color: var(--good); }
    .neg { color: var(--bad); }
    h2 { font-size: 15px; margin: 0 0 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { text-align: left; border-bottom: 1px solid var(--border); padding: 9px 8px; vertical-align: top; }
    th { color: var(--muted); font-weight: 620; background: #111715; position: sticky; top: 0; }
    .table-scroll { max-height: 520px; overflow: auto; border: 1px solid var(--border); border-radius: var(--radius); }
    .chart { width: 100%; height: 260px; display: block; background: linear-gradient(#101614, #0d1110); border: 1px solid var(--border); border-radius: var(--radius); }
    .hidden { display: none; }
    .row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 12px; font-weight: 650; }
    input, select { width: 100%; border: 1px solid var(--border); border-radius: var(--radius); padding: 8px 10px; background: #0f1514; min-height: 36px; color: var(--ink); }
    input::placeholder { color: #60706a; }
    input:disabled, select:disabled { opacity: .62; cursor: not-allowed; background: #111716; }
    input:focus, select:focus, button:focus { outline: 2px solid rgba(15,118,110,.28); outline-offset: 2px; }
    button { border: 1px solid var(--border); background: #111715; color: var(--ink); padding: 8px 12px; border-radius: var(--radius); cursor: pointer; min-height: 36px; font-weight: 650; }
    button:hover { border-color: #3b4c47; background: #18211f; }
    button:disabled { cursor: not-allowed; opacity: .45; }
    button:active { transform: translateY(1px); }
    .primary { background: var(--accent); border-color: var(--accent); color: #06100e; }
    .primary:hover:not(:disabled) { background: #35d5c2; border-color: #35d5c2; color: #03100e; box-shadow: 0 0 0 3px rgba(53,213,194,.16); }
    .primary:disabled:hover { background: var(--accent); border-color: var(--accent); color: #06100e; }
    .danger { border-color: #e5b7b7; color: var(--bad); }
    .control-panel { height: auto; min-height: 0; overflow: visible; }
    .terminal-panel { height: min(760px, calc(100dvh - 165px)); min-height: 560px; }
    .terminal-panel { display: flex; flex-direction: column; min-height: 0; }
    .terminal { flex: 1; background: var(--terminal); color: #c8d6d1; border: 1px solid #26322f; border-radius: var(--radius); padding: 12px; min-height: 0; max-height: none; overflow: auto; overflow-wrap: anywhere; scrollbar-width: none; -ms-overflow-style: none; font: 12px/1.55 ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace; white-space: pre-wrap; box-shadow: inset 0 0 0 1px rgba(255,255,255,.02); }
    .terminal::-webkit-scrollbar { display: none; width: 0; height: 0; }
    .terminal .log-line { display: block; min-height: 18px; }
    .terminal .log-time { color: #708079; }
    .terminal .log-start { color: var(--accent); font-weight: 700; }
    .terminal .log-target { color: #ffd84d; font-weight: 800; text-shadow: 0 0 12px rgba(255, 216, 77, .22); }
    .terminal .log-watch { color: #aab7b2; }
    .terminal .log-enter { color: var(--good); font-weight: 700; }
    .terminal .log-exit { color: var(--info); font-weight: 700; }
    .terminal .log-arb { color: var(--warn); font-weight: 700; }
    .terminal .log-hold { color: #8ec7ff; }
    .terminal .log-end { color: #d8c27a; }
    .terminal .log-error { color: var(--bad); font-weight: 700; }
    .terminal .log-muted { color: #73827d; }
    .form-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
    .form-grid .wide { grid-column: 1 / -1; }
    .span-2 { grid-column: span 2; }
    .section-title { grid-column: 1 / -1; color: var(--accent-2); font-size: 12px; font-weight: 750; margin-top: 4px; padding-top: 8px; border-top: 1px solid var(--line); }
    .checkline { display: flex; align-items: center; gap: 9px; min-height: 36px; padding: 8px 10px; border: 1px solid var(--border); border-radius: var(--radius); background: #0f1514; color: var(--ink); }
    .checkline input { width: 16px; min-height: 16px; accent-color: var(--accent); }
    .command-row { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }
    .notice { margin-top: 12px; padding: 10px; background: var(--soft); border: 1px solid var(--border); border-radius: var(--radius); color: var(--ink); font-size: 13px; }
    .pill { display: inline-flex; padding: 3px 8px; border-radius: 999px; background: var(--soft); color: var(--ink); font-size: 12px; }
    .data-pill { max-width: min(720px, 52vw); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .stacked { display: grid; gap: 3px; min-width: 0; }
    .stacked strong { color: var(--ink); font-weight: 720; }
    .pnl-pair { display: inline-grid; gap: 2px; font-weight: 760; }
    .risk { border-left: 4px solid var(--warn); }
    .empty { color: var(--muted); padding: 24px; text-align: center; }
    @media (max-width: 1320px) { .run-grid { grid-template-columns: 1fr; } .form-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
    @media (max-width: 900px) { .wrap, main { width: min(100% - 28px, 1880px); } .metrics, .two, .run-grid, .form-grid { grid-template-columns: 1fr; } .form-grid .wide, .span-2 { grid-column: auto; } .topbar { align-items: flex-start; flex-direction: column; } .statusline { justify-content: flex-start; } .control-panel { height: auto; min-height: 0; } .terminal-panel { height: 560px; } }
  </style>
</head>
<body>
  <header>
    <div class="wrap">
      <div class="topbar">
        <div>
          <h1>Bảng điều khiển Polymarket Paper</h1>
          <div class="subtle" id="generated">Đang tải dữ liệu paper...</div>
        </div>
        <div class="statusline">
          <span class="dot" id="botDot"></span>
          <span class="subtle" id="botStatus">Bot đang nghỉ</span>
          <span class="pill data-pill" id="dataFile">Data: chưa có run</span>
        </div>
      </div>
      <nav class="tabs">
        <button class="tab active" data-tab="run">Chạy bot</button>
        <button class="tab" data-tab="overview">Tổng quan</button>
        <button class="tab" data-tab="trades">Lệnh</button>
        <button class="tab" data-tab="live">Theo dõi</button>
      </nav>
    </div>
  </header>
  <main>
    <section id="run">
      <div class="grid run-grid">
        <div class="panel control-panel">
          <h2>Điều khiển bot</h2>
          <form id="runForm">
            <div class="form-grid">
              <div class="section-title">Nguồn kèo</div>
              <label class="wide">URL hoặc slug Polymarket
                <input id="url" name="url" placeholder="https://polymarket.com/event/btc-updown-5m-1779609900" required>
              </label>
              <label>Số kèo chạy nối tiếp
                <input id="chain_count" name="chain_count" type="number" min="1" max="100" step="1" value="6">
              </label>
              <label>Giây mỗi lần quét
                <input id="poll" name="poll" type="number" min="1" max="60" step="1" value="2">
              </label>
              <label>Size paper USD
                <input id="size_usd" name="size_usd" type="number" min="0.01" step="0.01" value="1">
              </label>
              <label>Giây chạy override
                <input id="seconds" name="seconds" type="number" min="1" step="1" placeholder="Tự động">
              </label>

              <div class="section-title">Chiến lược vào/thoát</div>
              <label>Giá vào min
                <input id="entry_min" name="entry_min" type="number" min="0.01" max="0.99" step="0.01" value="0.65">
              </label>
              <label>Giá vào max
                <input id="entry_max" name="entry_max" type="number" min="0.01" max="0.99" step="0.01" value="0.68">
              </label>
              <label>Chốt lời
                <input id="take_profit" name="take_profit" type="number" min="0.01" max="0.99" step="0.01" value="0.88">
              </label>
              <label>Cắt lỗ
                <input id="stop_loss" name="stop_loss" type="number" min="0.01" max="0.99" step="0.01" value="0.58">
              </label>
              <label>Thoát cứng
                <input id="hard_exit" name="hard_exit" type="number" min="0.01" max="0.99" step="0.01" value="0.58">
              </label>
              <label>Giây late exit
                <input id="late_seconds" name="late_seconds" type="number" min="0" max="300" step="1" value="30">
              </label>
              <label>Khoảng cách BTC tối thiểu
                <input id="min_distance_usd" name="min_distance_usd" type="number" min="0" step="1" value="100">
              </label>
              <label>Chỉ vào khi còn <= giây
                <input id="entry_window_seconds" name="entry_window_seconds" type="number" min="0" max="3600" step="1" value="120">
              </label>

              <div class="section-title">Bộ lọc rủi ro</div>
              <label>Max lệnh mỗi label
                <input id="max_trades_per_label_per_market" name="max_trades_per_label_per_market" type="number" min="0" max="100" step="1" value="1">
              </label>
              <label>Không vào sau giây
                <input id="no_entry_after_seconds" name="no_entry_after_seconds" type="number" min="0" max="3600" step="1" value="0">
              </label>
              <label>Tổng ask tối đa
                <input id="max_sum_asks" name="max_sum_asks" type="number" min="0" max="2" step="0.001" value="1.03">
              </label>
              <input type="hidden" name="market_lock_after_loss" value="false">
              <label class="checkline">
                <input id="market_lock_after_loss" name="market_lock_after_loss" type="checkbox" value="true" checked>
                <span>Khóa market sau lệnh lỗ</span>
              </label>

              <div class="section-title">Hạ tầng</div>
              <label>Buffer arb
                <input id="arb_buffer" name="arb_buffer" type="number" min="0" max="1" step="0.001" value="0.01">
              </label>
              <label>Fee dự phòng
                <input id="fee_rate" name="fee_rate" type="number" min="0" max="1" step="0.001" value="0.07">
              </label>
              <label>Giây giữa các kèo
                <input id="chain_interval_seconds" name="chain_interval_seconds" type="number" min="1" step="1" value="300">
              </label>
              <label>Timeout tìm kèo
                <input id="lookup_timeout_seconds" name="lookup_timeout_seconds" type="number" min="1" step="1" value="60">
              </label>
              <label>Giây đệm kết thúc
                <input id="end_buffer_seconds" name="end_buffer_seconds" type="number" min="0" step="1" value="1">
              </label>
            </div>
            <div class="command-row">
              <button class="primary" id="startBtn" type="submit">Chạy bot</button>
              <button id="stopBtn" type="button">Dừng bot</button>
              <button class="danger" id="resetBtn" type="button">Xóa dữ liệu</button>
              <button id="closeAppBtn" type="button">Đóng app</button>
            </div>
            <div class="notice" id="controlNotice">Sẵn sàng.</div>
          </form>
        </div>
        <div class="panel terminal-panel">
          <div class="row" style="justify-content:space-between;margin-bottom:12px">
            <h2 style="margin:0">Nhật ký</h2>
            <span class="pill" id="pidPill">Chưa chạy</span>
          </div>
          <pre class="terminal" id="terminal">Đang chờ log bot...</pre>
        </div>
      </div>
    </section>
    <section id="overview" class="hidden">
      <div class="grid metrics" id="metrics"></div>
      <div class="panel" style="margin-top:12px">
        <div class="row" style="justify-content:space-between;margin-bottom:12px">
          <h2 style="margin:0">Lệnh đang mở</h2>
          <span class="pill" id="activeOverviewCount">0 lệnh</span>
        </div>
        <div class="table-scroll"><table id="activeOverview"></table></div>
      </div>
      <div class="grid two" style="margin-top:12px">
        <div class="panel">
          <h2>Đường vốn</h2>
          <svg id="equity" class="chart" role="img" aria-label="PnL paper tích lũy"></svg>
        </div>
        <div class="panel risk">
          <h2>Sức khỏe chiến lược</h2>
          <div id="health"></div>
        </div>
      </div>
      <div class="grid two" style="margin-top:12px">
        <div class="panel">
          <h2>Theo market</h2>
          <div class="table-scroll"><table id="markets"></table></div>
        </div>
        <div class="panel">
          <h2>Theo outcome</h2>
          <div class="table-scroll"><table id="outcomes"></table></div>
        </div>
      </div>
    </section>
    <section id="trades" class="hidden">
      <div class="panel">
        <div class="row" style="justify-content:space-between;margin-bottom:12px">
          <h2 style="margin:0">Nhật ký lệnh</h2>
          <div class="row">
            <input id="filter" placeholder="Lọc market, outcome, lý do">
            <select id="resultFilter">
              <option value="all">Tất cả lệnh</option>
              <option value="wins">Lệnh lãi</option>
              <option value="losses">Lệnh lỗ</option>
            </select>
          </div>
        </div>
        <div class="table-scroll"><table id="tradesTable"></table></div>
      </div>
    </section>
    <section id="live" class="hidden">
      <div class="panel" style="margin-bottom:12px">
        <h2>Lệnh đang mở</h2>
        <div class="table-scroll"><table id="activePositions"></table></div>
      </div>
      <div class="grid two">
        <div class="panel">
          <h2>Snapshot orderbook mới nhất</h2>
          <div class="table-scroll"><table id="snapshots"></table></div>
        </div>
        <div class="panel">
          <h2>Sự kiện arbitrage</h2>
          <div id="arbSummary"></div>
          <div class="table-scroll" style="margin-top:12px"><table id="arbEvents"></table></div>
        </div>
      </div>
    </section>
  </main>
  <script>
    let state = null;
    let bot = null;
    let formDirty = false;
    let formHydrated = false;
    const fmt = (n, d=4) => Number(n || 0).toFixed(d);
    const cls = n => Number(n || 0) >= 0 ? 'pos' : 'neg';
    const money = n => `<span class="${cls(n)}">${Number(n || 0) >= 0 ? '+' : ''}${fmt(n)}</span>`;
    const pct = n => `${Number(n || 0).toFixed(1)}%`;
    const valueMaybe = (n, d=2) => n === null || n === undefined ? '<span class="subtle">n/a</span>' : fmt(n, d);
    const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    function activePositionCell(r) {
      return `<div class="stacked">
        <strong>${esc(r.market_slug || 'n/a')} ${esc(r.outcome || '')}</strong>
        <span class="subtle">Giá vốn ${valueMaybe(r.size_usd,2)} · Shares ${valueMaybe(r.shares,4)} · Entry ${valueMaybe(r.entry_price,2)}</span>
        <span class="subtle">Giữ ${Number(r.hold_seconds || 0)}s</span>
      </div>`;
    }
    function activePriceCell(r) {
      return `<div class="stacked">
        <strong>${valueMaybe(r.bid,2)}</strong>
        <span class="subtle">Ask ${valueMaybe(r.ask,2)}</span>
      </div>`;
    }
    function activePnlCell(r) {
      if (r.unrealized_pnl === null || r.unrealized_pnl === undefined) return '<span class="subtle">n/a</span>';
      const klass = cls(r.unrealized_pnl);
      const roi = r.unrealized_roi === null || r.unrealized_roi === undefined ? 'n/a' : pct(r.unrealized_roi);
      return `<span class="pnl-pair ${klass}"><span>${Number(r.unrealized_pnl) >= 0 ? '+' : ''}${fmt(r.unrealized_pnl)}</span><span>${roi}</span></span>`;
    }
    function setTab(name) {
      document.querySelectorAll('main > section').forEach(el => el.classList.toggle('hidden', el.id !== name));
      document.querySelectorAll('.tab').forEach(el => el.classList.toggle('active', el.dataset.tab === name));
    }
    document.querySelectorAll('.tab').forEach(btn => btn.addEventListener('click', () => setTab(btn.dataset.tab)));
    document.getElementById('runForm').addEventListener('input', () => {
      if (!bot?.running) formDirty = true;
    });
    document.getElementById('runForm').addEventListener('change', () => {
      if (!bot?.running) formDirty = true;
    });
    function table(id, headers, rows, empty='Chưa có dữ liệu') {
      const el = document.getElementById(id);
      if (!rows.length) { el.innerHTML = `<tr><td class="empty">${empty}</td></tr>`; return; }
      el.innerHTML = `<thead><tr>${headers.map(h => `<th>${h[0]}</th>`).join('')}</tr></thead><tbody>` +
        rows.map(row => `<tr>${headers.map(h => `<td>${h[1](row)}</td>`).join('')}</tr>`).join('') + '</tbody>';
    }
    function drawEquity(rows) {
      const svg = document.getElementById('equity');
      const w = svg.clientWidth || 800, h = svg.clientHeight || 260, p = 24;
      if (!rows.length) { svg.innerHTML = `<text x="50%" y="50%" text-anchor="middle" fill="#91a09b">Chưa có lệnh paper đã đóng</text>`; return; }
      const ys = rows.map(r => Number(r.cumulative));
      const minY = Math.min(0, ...ys), maxY = Math.max(0, ...ys);
      const span = Math.max(.0001, maxY - minY);
      const x = i => p + (rows.length === 1 ? 0 : i * (w - p * 2) / (rows.length - 1));
      const y = v => h - p - ((v - minY) / span) * (h - p * 2);
      const pts = rows.map((r, i) => `${x(i)},${y(Number(r.cumulative))}`).join(' ');
      const zero = y(0);
      svg.setAttribute('viewBox', `0 0 ${w} ${h}`);
      svg.innerHTML = `<line x1="${p}" x2="${w-p}" y1="${zero}" y2="${zero}" stroke="#2a3633"/>` +
        `<polyline points="${pts}" fill="none" stroke="#1aa896" stroke-width="3" stroke-linejoin="round" stroke-linecap="round"/>` +
        rows.map((r,i) => `<circle cx="${x(i)}" cy="${y(Number(r.cumulative))}" r="4" fill="${Number(r.pnl) >= 0 ? '#49d18f' : '#ff6b6b'}"><title>${r.market_slug}: ${fmt(r.cumulative)}</title></circle>`).join('');
    }
    function logClass(line) {
      if (/stderr|fetch\/error|HTTP Error|Traceback|Error:/i.test(line)) return 'log-error';
      if (/Market target price:/i.test(line)) return 'log-target';
      if (/PAPER ENTER| ENTER /.test(line)) return 'log-enter';
      if (/PAPER EXIT| EXIT /.test(line)) return 'log-exit';
      if (/ARB /.test(line)) return 'log-arb';
      if (/ HOLD /.test(line)) return 'log-hold';
      if (/ WATCH |watch bid=|tick complete/.test(line)) return 'log-watch';
      if (/started|Slug:|Market end:|Watcher end:|Outcomes:|Strategies:|No wallet/.test(line)) return 'log-start';
      if (/ended|Paper trades|Arb events|No paper trades|No arb events|summary/i.test(line)) return 'log-end';
      if (/^\s*$/.test(line)) return 'log-muted';
      return 'log-line';
    }
    function colorizeLogLine(line) {
      const safe = esc(line).replace(/^(\[[^\]]+\])/, '<span class="log-time">$1</span>');
      return `<span class="log-line ${logClass(line)}">${safe}</span>`;
    }
    function renderTerminal(stdoutLines, stderrLines) {
      const lines = [];
      for (const line of stdoutLines || []) lines.push(line);
      if ((stderrLines || []).length) {
        if (lines.length) lines.push('');
        lines.push('[stderr]');
        for (const line of stderrLines) lines.push(line);
      }
      const terminal = document.getElementById('terminal');
      if (!lines.length) {
        terminal.textContent = 'Đang chờ log bot...';
        return;
      }
      terminal.innerHTML = lines.map(colorizeLogLine).join('\n');
      requestAnimationFrame(() => {
        terminal.scrollTop = terminal.scrollHeight;
      });
    }
    function formPayload() {
      const form = new FormData(document.getElementById('runForm'));
      const payload = {};
      for (const [key, value] of form.entries()) payload[key] = String(value).trim();
      for (const key of ['seconds']) if (!payload[key]) delete payload[key];
      return payload;
    }
    function formFieldIsFocused() {
      const active = document.activeElement;
      return Boolean(active && document.getElementById('runForm').contains(active) && ['INPUT', 'SELECT'].includes(active.tagName));
    }
    function syncFormConfig(config) {
      for (const [key, value] of Object.entries(config)) {
        const input = document.getElementById(key);
        if (input && value !== null && value !== undefined) {
          if (input.type === 'checkbox') input.checked = Boolean(value);
          else input.value = value;
        }
      }
      formHydrated = true;
    }
    async function postJSON(url, payload={}) {
      const res = await fetch(url, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Request failed: ${res.status}`);
      return data;
    }
    function renderBotStatus(data, options={}) {
      bot = data;
      document.getElementById('botDot').classList.toggle('on', Boolean(data.running));
      document.getElementById('botStatus').textContent = data.running
        ? `Đang chạy PID ${data.pid} từ ${data.started_at || 'bây giờ'}`
        : (data.returncode === null ? 'Bot đang nghỉ' : `Bot đã dừng, mã ${data.returncode}`);
      document.getElementById('pidPill').textContent = data.running ? `PID ${data.pid}` : 'Chưa chạy';
      document.getElementById('dataFile').textContent = data.data_file ? `Data: ${data.data_file}` : 'Data: chưa có run';
      document.getElementById('dataFile').title = data.db_path || '';
      document.getElementById('startBtn').disabled = Boolean(data.running);
      document.getElementById('stopBtn').disabled = !data.running;
      document.getElementById('resetBtn').disabled = Boolean(data.running);
      document.querySelectorAll('#runForm input, #runForm select').forEach(input => {
        input.disabled = Boolean(data.running);
      });
      const hasConfig = Boolean(data.config && data.config.url);
      const shouldSyncForm = hasConfig && (
        options.forceFormSync ||
        data.running ||
        !formHydrated ||
        !formDirty
      ) && (options.forceFormSync || !formFieldIsFocused());
      if (shouldSyncForm) {
        syncFormConfig(data.config);
      }
      renderTerminal(data.stdout_tail || [], data.stderr_tail || []);
    }
    async function refreshBot() {
      const res = await fetch('/api/bot/status', { cache: 'no-store' });
      renderBotStatus(await res.json());
    }
    async function runCommand(action) {
      const notice = document.getElementById('controlNotice');
      try {
        if (action === 'start') {
          notice.textContent = 'Đang khởi động bot...';
          const status = await postJSON('/api/bot/start', formPayload());
          formDirty = false;
          renderBotStatus(status, {forceFormSync: true});
          await refresh();
          notice.textContent = 'Bot đã chạy.';
        } else if (action === 'stop') {
          notice.textContent = 'Đang dừng bot...';
          renderBotStatus(await postJSON('/api/bot/stop'), {forceFormSync: true});
          await refresh();
          notice.textContent = 'Bot đã dừng.';
        } else if (action === 'reset') {
          notice.textContent = 'Đang xóa dữ liệu local...';
          renderBotStatus(await postJSON('/api/data/reset'));
          await refresh();
          notice.textContent = 'Đã xóa dữ liệu local.';
        } else if (action === 'close') {
          notice.textContent = 'Đang đóng app và dừng bot...';
          await postJSON('/api/app/shutdown');
          notice.textContent = 'App đã đóng. Có thể đóng tab trình duyệt.';
          setTimeout(() => window.close(), 300);
        }
      } catch (error) {
        notice.textContent = error.message;
      }
    }
    function render(data) {
      state = data;
      document.getElementById('generated').textContent = `Cập nhật ${data.generated_at}`;
      const o = data.overall;
      document.getElementById('metrics').innerHTML = [
        ['Số lệnh', o.trades],
        ['PnL paper', money(o.pnl)],
        ['Win rate', pct(o.win_rate)],
        ['ROI trên size', pct(o.roi)],
        ['Market đã test', o.markets],
        ['PnL trung bình', money(o.avg_pnl)],
        ['Lệnh tốt nhất', money(o.best)],
        ['Lệnh tệ nhất', money(o.worst)]
      ].map(m => `<div class="panel metric"><div class="label">${m[0]}</div><div class="value">${m[1]}</div></div>`).join('');
      const activeRows = data.active_positions || [];
      document.getElementById('activeOverviewCount').textContent = `${activeRows.length} lệnh`;
      table('activeOverview', [
        ['Giá vốn / Shares / Entry', activePositionCell],
        ['Giá hiện tại', activePriceCell],
        ['PnL / %PnL', activePnlCell]
      ], activeRows, 'Không có lệnh đang mở');
      drawEquity(data.equity);
      document.getElementById('health').innerHTML = `
        <p><span class="pill">Thoát cứng</span> ${data.health.hard_exit_count}</p>
        <p><span class="pill">Cắt lỗ</span> ${data.health.stop_loss_count}</p>
        <p><span class="pill">Lỗ lớn nhất</span> ${money(data.health.largest_loss)}</p>
        <p><span class="pill">Tỷ trọng top 3 PnL</span> ${pct(data.health.top_3_share)}</p>
        <p class="subtle">Nếu phần lớn lợi nhuận đến từ vài lệnh, tiếp tục thu thập dữ liệu trước khi tăng size.</p>`;
      table('markets', [['Market', r=>r.market_slug], ['Lệnh', r=>r.trades], ['PnL', r=>money(r.pnl)], ['Win', r=>pct(r.win_rate)], ['Tệ nhất', r=>money(r.worst)]], data.markets);
      table('outcomes', [['Kết quả', r=>r.outcome], ['Lệnh', r=>r.trades], ['PnL', r=>money(r.pnl)], ['Win', r=>pct(r.win_rate)], ['Entry TB', r=>fmt(r.avg_entry,2)], ['Exit TB', r=>fmt(r.avg_exit,2)], ['Thoát rủi ro', r=>r.risk_exits]], data.outcomes);
      renderTrades();
      table('activePositions', [
        ['Giá vốn / Shares / Entry', activePositionCell],
        ['Giá hiện tại', activePriceCell],
        ['PnL / %PnL', activePnlCell]
      ], activeRows, 'Không có lệnh đang mở');
      table('snapshots', [['Thời gian', r=>r.time], ['Label', r=>r.label], ['Bid', r=>fmt(r.bid,2)], ['Ask', r=>fmt(r.ask,2)], ['Bid size', r=>fmt(r.bid_size,2)], ['Ask size', r=>fmt(r.ask_size,2)]], data.snapshots);
      document.getElementById('arbSummary').innerHTML = data.arb.length ? data.arb.map(r => `<p><span class="pill">${r.kind}</span> sự kiện=${r.events} max=${fmt(r.max_edge)} avg=${fmt(r.avg_edge)}</p>`).join('') : '<p class="empty">Chưa có sự kiện arb.</p>';
      table('arbEvents', [['Thời gian', r=>r.time], ['Market', r=>r.market_slug], ['Loại', r=>r.kind], ['Up/Yes', r=>fmt(r.yes_price,2)], ['Down/No', r=>fmt(r.no_price,2)], ['Edge', r=>money(r.edge)]], data.arb_events);
    }
    function renderTrades() {
      if (!state) return;
      const q = document.getElementById('filter').value.toLowerCase();
      const mode = document.getElementById('resultFilter').value;
      const rows = state.trades.filter(r => {
        const hay = `${r.market_slug} ${r.outcome} ${r.exit_reason} ${r.entry_reason}`.toLowerCase();
        if (q && !hay.includes(q)) return false;
        if (mode === 'wins' && Number(r.pnl) <= 0) return false;
        if (mode === 'losses' && Number(r.pnl) >= 0) return false;
        return true;
      });
      table('tradesTable', [
        ['Market', r=>r.market_slug], ['Kết quả', r=>r.outcome], ['Vào', r=>r.entry_time], ['Thoát', r=>r.exit_time],
        ['Giữ', r=>`${r.hold_seconds}s`], ['Giá vào', r=>fmt(r.entry_price,2)], ['Giá thoát', r=>fmt(r.exit_price,2)],
        ['PnL', r=>money(r.pnl)], ['Lý do thoát', r=>r.exit_reason]
      ], rows);
    }
    document.getElementById('filter').addEventListener('input', renderTrades);
    document.getElementById('resultFilter').addEventListener('change', renderTrades);
    document.getElementById('runForm').addEventListener('submit', event => {
      event.preventDefault();
      runCommand('start');
    });
    document.getElementById('stopBtn').addEventListener('click', () => runCommand('stop'));
    document.getElementById('resetBtn').addEventListener('click', () => runCommand('reset'));
    document.getElementById('closeAppBtn').addEventListener('click', () => runCommand('close'));
    async function refresh() {
      const res = await fetch('/api/dashboard', { cache: 'no-store' });
      render(await res.json());
    }
    refresh();
    refreshBot();
    setInterval(refresh, 2000);
    setInterval(refreshBot, 2000);
  </script>
</body>
</html>"""


DASHBOARD_HTML = r"""<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Polymarket Paper Console</title>
  <style>
    :root {
      --bg:#0b0f0e; --surface:#111816; --surface2:#17211e; --ink:#edf4f1; --muted:#91a09b;
      --border:#2a3633; --line:#22302c; --good:#4fd18b; --warn:#e7b84b; --bad:#ef735f;
      --info:#7bb7ff; --accent:#20b7a2; --terminal:#080c0b; --radius:8px;
      font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
      font-variant-numeric:tabular-nums;
    }
    *{box-sizing:border-box} body{margin:0;min-height:100dvh;background:var(--bg);color:var(--ink)}
    header{position:sticky;top:0;z-index:5;background:rgba(11,15,14,.94);border-bottom:1px solid var(--border);backdrop-filter:blur(14px)}
    .wrap,main{width:calc(100% - 32px);margin:0 auto}.wrap{padding:14px 0 12px}main{display:flex;flex-direction:column;min-height:calc(100dvh - 116px);padding:12px 0 16px}main>section{min-height:0;width:100%}main>section:not(.hidden){flex:1}
    .topbar,.statusline,.row,.command-row,.toolbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.topbar{justify-content:space-between}
    h1{margin:0;font-size:21px;line-height:1.15;letter-spacing:0}h2{margin:0 0 12px;font-size:14px;font-weight:760}h3{margin:0;font-size:13px}
    .subtle{color:var(--muted);font-size:12px}.dot{width:9px;height:9px;border-radius:50%;background:#65736e;display:inline-block}
    .dot.on{background:var(--good);box-shadow:0 0 0 4px rgba(79,209,139,.12)}
    .pill{display:inline-flex;align-items:center;gap:6px;padding:3px 8px;border:1px solid var(--border);border-radius:999px;background:#0f1514;color:var(--ink);font-size:12px;white-space:nowrap}
    .data-pill{max-width:min(760px,52vw);overflow:hidden;text-overflow:ellipsis}.tabs{display:flex;gap:6px;margin-top:14px}
    .tab,button{border:1px solid var(--border);background:#101614;color:var(--ink);padding:8px 12px;border-radius:var(--radius);min-height:36px;cursor:pointer;font-weight:680}
    .tab{color:var(--muted)}.tab:hover,button:hover{background:var(--surface2);border-color:#3a4b46;color:var(--ink)}
    .tab.active,.primary{background:var(--accent);border-color:var(--accent);color:#04110f}.danger{color:var(--bad);border-color:rgba(239,115,95,.55)}
    button:disabled,input:disabled,select:disabled{opacity:.48;cursor:not-allowed}input,select{width:100%;min-height:34px;padding:7px 9px;border:1px solid var(--border);border-radius:6px;background:#0e1413;color:var(--ink)}
    input:focus,select:focus,button:focus{outline:2px solid rgba(32,183,162,.35);outline-offset:2px}label{display:grid;gap:5px;color:var(--muted);font-size:12px;font-weight:650}
    .grid{display:grid;gap:12px}#run:not(.hidden){display:flex;flex:1;min-height:0}.run-grid{width:100%;min-height:0;grid-template-columns:minmax(0,1.35fr) minmax(460px,.65fr);align-items:stretch}#runForm{min-height:0;grid-template-rows:auto minmax(0,1fr)}#runForm>.panel:last-of-type{min-height:0}
    .two{grid-template-columns:minmax(0,1.25fr) minmax(360px,.75fr)}.metrics{grid-template-columns:repeat(6,minmax(0,1fr))}
    .panel{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:14px;box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
    .metric .label{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.04em}.metric .value{margin-top:7px;font-size:22px;font-weight:780}
    .setup-grid{display:grid;grid-template-columns:minmax(280px,2fr) repeat(4,minmax(110px,1fr));gap:10px}
    .risk-grid{display:grid;grid-template-columns:repeat(5,minmax(110px,1fr));gap:10px;margin-top:12px}
    .checkline{display:flex;align-items:center;gap:8px;min-height:34px;padding:8px 10px;border:1px solid var(--border);border-radius:6px;background:#0e1413;color:var(--ink)}
    .checkline input{width:16px;min-height:16px;accent-color:var(--accent)}
    .preset-board{display:grid;gap:10px;margin-top:12px}.preset-row{display:grid;grid-template-columns:96px repeat(8,minmax(62px,1fr)) minmax(100px,.9fr);gap:8px;align-items:end;border:1px solid var(--border);border-left-width:4px;border-radius:var(--radius);padding:10px;background:#0e1413}
    .preset-row[data-preset=safe]{border-left-color:var(--good)}.preset-row[data-preset=balanced]{border-left-color:var(--warn)}.preset-row[data-preset=aggressive]{border-left-color:var(--bad)}
    .preset-title{display:grid;gap:6px;align-self:center}.preset-title label{display:flex;align-items:center;gap:8px;color:var(--ink)}.preset-title input{width:16px;min-height:16px;accent-color:var(--accent)}
    .rr-box{display:grid;gap:3px;padding:8px;border:1px solid var(--line);border-radius:6px;background:#101815}.rr-box strong{font-size:15px}.rr-box.good strong{color:var(--good)}.rr-box.thin strong{color:var(--warn)}.rr-box.bad strong{color:var(--bad)}
    .notice{margin-top:12px;padding:10px;border:1px solid var(--border);border-radius:var(--radius);background:#101815;color:var(--ink);font-size:13px}
    .terminal-panel{display:flex;flex-direction:column;min-height:0;height:auto}
    .terminal{flex:1;min-height:0;overflow:auto;overflow-wrap:anywhere;scrollbar-width:none;-ms-overflow-style:none;margin:0;padding:12px;border:1px solid #26322f;border-radius:var(--radius);background:var(--terminal);color:#c8d6d1;font:12px/1.55 ui-monospace,SFMono-Regular,Consolas,"Liberation Mono",monospace;white-space:pre-wrap}
    .terminal::-webkit-scrollbar{display:none;width:0;height:0}
    .terminal.small{height:330px;flex:none}.terminal .log-line{display:block;min-height:18px}.terminal .log-time{color:#708079}.terminal .log-start{color:var(--accent);font-weight:700}.terminal .log-target{color:#ffd84d;font-weight:800}.terminal .log-watch{color:#aab7b2}.terminal .log-enter{color:var(--good);font-weight:700}.terminal .log-exit{color:var(--info);font-weight:700}.terminal .log-arb{color:var(--warn);font-weight:700}.terminal .log-hold{color:#8ec7ff}.terminal .log-end{color:#d8c27a}.terminal .log-error{color:var(--bad);font-weight:700}.terminal .log-muted{color:#73827d}
    .hidden{display:none}table{width:100%;border-collapse:collapse;font-size:12.5px}th,td{text-align:left;border-bottom:1px solid var(--border);padding:9px 8px;vertical-align:top}th{position:sticky;top:0;background:#101614;color:var(--muted);font-weight:720;z-index:1}
    .table-scroll{max-height:520px;overflow:auto;border:1px solid var(--border);border-radius:var(--radius)}.chart{width:100%;height:280px;display:block;border:1px solid var(--border);border-radius:var(--radius);background:#0d1211}
    .stacked{display:grid;gap:3px;min-width:0}.stacked strong{font-weight:760;color:var(--ink)}.pos{color:var(--good)}.neg{color:var(--bad)}.empty{color:var(--muted);padding:24px;text-align:center}
    .preset-chip{border-color:currentColor;background:transparent}.preset-chip.safe{color:var(--good)}.preset-chip.balanced{color:var(--warn)}.preset-chip.aggressive{color:var(--bad)}
    .decision-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}.decision-card{border:1px solid var(--border);border-left-width:4px;border-radius:var(--radius);padding:10px;background:#0f1514}.decision-card.safe{border-left-color:var(--good)}.decision-card.balanced{border-left-color:var(--warn)}.decision-card.aggressive{border-left-color:var(--bad)}
    @media(min-width:1321px){body{height:100dvh;overflow:hidden}main{height:calc(100dvh - 116px);overflow:hidden}}
    @media(max-width:1320px){main{min-height:auto}.run-grid,.two{grid-template-columns:1fr}.setup-grid,.risk-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.preset-row{grid-template-columns:repeat(4,minmax(0,1fr))}.preset-title,.rr-box{grid-column:1/-1}.metrics,.decision-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.terminal-panel{min-height:620px}}
    @media(max-width:760px){.wrap,main{width:calc(100% - 24px)}.setup-grid,.risk-grid,.metrics,.decision-grid{grid-template-columns:1fr}.topbar{align-items:flex-start;flex-direction:column}.terminal-panel{min-height:520px}}
  </style>
</head>
<body>
  <header><div class="wrap"><div class="topbar"><div><h1>Polymarket Paper Console</h1><div class="subtle" id="generated">Dang tai du lieu paper...</div></div><div class="statusline"><span class="dot" id="botDot"></span><span class="subtle" id="botStatus">Bot dang nghi</span><span class="pill data-pill" id="dataFile">Data: chua co run</span></div></div><nav class="tabs"><button class="tab active" data-tab="run">Chay bot</button><button class="tab" data-tab="overview">Tong quan</button><button class="tab" data-tab="trades">Lenh</button><button class="tab" data-tab="live">Theo doi</button></nav></div></header>
  <main>
    <section id="run"><div class="grid run-grid"><form id="runForm" class="grid"><div class="panel"><h2>Run setup</h2><div class="setup-grid"><label>URL hoac slug Polymarket<input id="url" name="url" placeholder="https://polymarket.com/event/btc-updown-5m-1779609900" required></label><label>So keo noi tiep<input id="chain_count" name="chain_count" type="number" min="1" max="100" step="1" value="6"></label><label>Giay moi lan quet<input id="poll" name="poll" type="number" min="1" max="60" step="1" value="2"></label><label>Size paper USD<input id="size_usd" name="size_usd" type="number" min="0.01" step="0.01" value="1"></label><label>Giay chay override<input id="seconds" name="seconds" type="number" min="1" step="1" placeholder="Tu dong"></label></div><div class="risk-grid"><label>Khong vao sau giay<input id="no_entry_after_seconds" name="no_entry_after_seconds" type="number" min="0" max="3600" step="1" value="0"></label><label>Tong ask toi da<input id="max_sum_asks" name="max_sum_asks" type="number" min="0" max="2" step="0.001" value="1.03"></label><label>Chi vao khi con <= giay<input id="entry_window_seconds" name="entry_window_seconds" type="number" min="0" max="3600" step="1" value="120"></label><label>Buffer arb<input id="arb_buffer" name="arb_buffer" type="number" min="0" max="1" step="0.001" value="0.01"></label><label>Fee du phong<input id="fee_rate" name="fee_rate" type="number" min="0" max="1" step="0.001" value="0.07"></label><label>Giay giua cac keo<input id="chain_interval_seconds" name="chain_interval_seconds" type="number" min="1" step="1" value="300"></label><label>Timeout tim keo<input id="lookup_timeout_seconds" name="lookup_timeout_seconds" type="number" min="1" step="1" value="60"></label><label>Giay dem ket thuc<input id="end_buffer_seconds" name="end_buffer_seconds" type="number" min="0" step="1" value="1"></label><input type="hidden" name="market_lock_after_loss" value="false"><label class="checkline"><input id="market_lock_after_loss" name="market_lock_after_loss" type="checkbox" value="true" checked><span>Khoa market sau lenh lo</span></label></div></div>
    <div class="panel"><div class="row" style="justify-content:space-between;margin-bottom:10px"><h2 style="margin:0">Preset strategy matrix</h2><span class="subtle">R:R cap nhat realtime theo entry / TP / SL</span></div><div class="preset-board" id="presetBoard">
      <div class="preset-row" data-preset="safe"><div class="preset-title"><h3>An toan</h3><label><input data-preset-field="enabled" type="checkbox" checked> Enabled</label></div><label>Entry min<input data-preset-field="entry_min" type="number" min="0.01" max="0.99" step="0.01" value="0.65"></label><label>Entry max<input data-preset-field="entry_max" type="number" min="0.01" max="0.99" step="0.01" value="0.68"></label><label>TP<input data-preset-field="take_profit" type="number" min="0.01" max="0.99" step="0.01" value="0.84"></label><label>SL<input data-preset-field="stop_loss" type="number" min="0.01" max="0.99" step="0.01" value="0.60"></label><label>Hard<input data-preset-field="hard_exit" type="number" min="0.01" max="0.99" step="0.01" value="0.60"></label><label>Late s<input data-preset-field="late_seconds" type="number" min="0" max="300" step="1" value="30"></label><label>BTC dist<input data-preset-field="min_distance_usd" type="number" min="0" step="1" value="100"></label><label>Max trades<input data-preset-field="max_trades_per_label_per_market" type="number" min="0" max="100" step="1" value="1"></label><div class="rr-box" data-rr-box><strong>0.00R</strong><span class="subtle">Worst 0.00R / Best 0.00R</span></div></div>
      <div class="preset-row" data-preset="balanced"><div class="preset-title"><h3>Can bang</h3><label><input data-preset-field="enabled" type="checkbox" checked> Enabled</label></div><label>Entry min<input data-preset-field="entry_min" type="number" min="0.01" max="0.99" step="0.01" value="0.65"></label><label>Entry max<input data-preset-field="entry_max" type="number" min="0.01" max="0.99" step="0.01" value="0.70"></label><label>TP<input data-preset-field="take_profit" type="number" min="0.01" max="0.99" step="0.01" value="0.86"></label><label>SL<input data-preset-field="stop_loss" type="number" min="0.01" max="0.99" step="0.01" value="0.58"></label><label>Hard<input data-preset-field="hard_exit" type="number" min="0.01" max="0.99" step="0.01" value="0.58"></label><label>Late s<input data-preset-field="late_seconds" type="number" min="0" max="300" step="1" value="30"></label><label>BTC dist<input data-preset-field="min_distance_usd" type="number" min="0" step="1" value="75"></label><label>Max trades<input data-preset-field="max_trades_per_label_per_market" type="number" min="0" max="100" step="1" value="1"></label><div class="rr-box" data-rr-box><strong>0.00R</strong><span class="subtle">Worst 0.00R / Best 0.00R</span></div></div>
      <div class="preset-row" data-preset="aggressive"><div class="preset-title"><h3>Aggressive</h3><label><input data-preset-field="enabled" type="checkbox" checked> Enabled</label></div><label>Entry min<input data-preset-field="entry_min" type="number" min="0.01" max="0.99" step="0.01" value="0.65"></label><label>Entry max<input data-preset-field="entry_max" type="number" min="0.01" max="0.99" step="0.01" value="0.72"></label><label>TP<input data-preset-field="take_profit" type="number" min="0.01" max="0.99" step="0.01" value="0.88"></label><label>SL<input data-preset-field="stop_loss" type="number" min="0.01" max="0.99" step="0.01" value="0.56"></label><label>Hard<input data-preset-field="hard_exit" type="number" min="0.01" max="0.99" step="0.01" value="0.56"></label><label>Late s<input data-preset-field="late_seconds" type="number" min="0" max="300" step="1" value="30"></label><label>BTC dist<input data-preset-field="min_distance_usd" type="number" min="0" step="1" value="75"></label><label>Max trades<input data-preset-field="max_trades_per_label_per_market" type="number" min="0" max="100" step="1" value="1"></label><div class="rr-box" data-rr-box><strong>0.00R</strong><span class="subtle">Worst 0.00R / Best 0.00R</span></div></div>
    </div><div class="command-row" style="margin-top:12px"><button class="primary" id="startBtn" type="submit">Chay bot</button><button id="stopBtn" type="button">Dung bot</button><button class="danger" id="resetBtn" type="button">Xoa du lieu</button><button id="closeAppBtn" type="button">Dong app</button></div><div class="notice" id="controlNotice">San sang.</div></div></form><div class="panel terminal-panel"><div class="row" style="justify-content:space-between;margin-bottom:12px"><h2 style="margin:0">Nhat ky</h2><span class="pill" id="pidPill">Chua chay</span></div><pre class="terminal" id="terminal">Dang cho log bot...</pre></div></div></section>
    <section id="overview" class="hidden"><div class="grid metrics" id="metrics"></div><div class="grid two" style="margin-top:12px"><div class="panel"><h2>Preset performance</h2><div class="table-scroll"><table id="presetSummary"></table></div></div><div class="panel"><h2>Suc khoe chien luoc</h2><div id="health"></div></div></div><div class="grid two" style="margin-top:12px"><div class="panel"><h2>Equity curve by preset</h2><svg id="equity" class="chart" role="img" aria-label="PnL paper by preset"></svg></div><div class="panel"><div class="row" style="justify-content:space-between;margin-bottom:12px"><h2 style="margin:0">Lenh dang mo</h2><span class="pill" id="activeOverviewCount">0 lenh</span></div><div class="table-scroll"><table id="activeOverview"></table></div></div></div><div class="grid two" style="margin-top:12px"><div class="panel"><h2>Theo market</h2><div class="table-scroll"><table id="markets"></table></div></div><div class="panel"><h2>Theo outcome</h2><div class="table-scroll"><table id="outcomes"></table></div></div></div></section>
    <section id="trades" class="hidden"><div class="panel"><div class="row" style="justify-content:space-between;margin-bottom:12px"><h2 style="margin:0">Trade journal</h2><div class="toolbar"><input id="filter" placeholder="Loc market, outcome, ly do"><select id="presetFilter"><option value="all">Tat ca preset</option></select><select id="resultFilter"><option value="all">Tat ca lenh</option><option value="wins">Lenh lai</option><option value="losses">Lenh lo</option></select></div></div><div class="table-scroll"><table id="tradesTable"></table></div></div></section>
    <section id="live" class="hidden"><div class="grid two"><div class="panel"><h2>Preset decision board</h2><div class="decision-grid" id="decisionBoard"></div></div><div class="panel"><h2>Orderbook moi nhat</h2><div class="table-scroll"><table id="snapshots"></table></div></div></div><div class="panel" style="margin-top:12px"><h2>Lenh dang mo</h2><div class="table-scroll"><table id="activePositions"></table></div></div><div class="grid two" style="margin-top:12px"><div class="panel"><h2>Terminal log</h2><pre class="terminal small" id="terminalLive">Dang cho log bot...</pre></div><div class="panel"><h2>Arbitrage</h2><div id="arbSummary"></div><div class="table-scroll" style="margin-top:12px"><table id="arbEvents"></table></div></div></div></section>
  </main>
  <script>
    let state=null,bot=null,formDirty=false,formHydrated=false;
    const presetNames={safe:'An toan',balanced:'Can bang',aggressive:'Aggressive',legacy:'Legacy'};
    const presetColors={safe:'#4fd18b',balanced:'#e7b84b',aggressive:'#ef735f',legacy:'#7bb7ff'};
    const fmt=(n,d=4)=>Number(n||0).toFixed(d),cls=n=>Number(n||0)>=0?'pos':'neg',money=n=>`<span class="${cls(n)}">${Number(n||0)>=0?'+':''}${fmt(n)}</span>`,pct=n=>`${Number(n||0).toFixed(1)}%`;
    const esc=value=>String(value??'').replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch])),valueMaybe=(n,d=2)=>n===null||n===undefined?'<span class="subtle">n/a</span>':fmt(n,d);
    function presetChip(row){const key=row.preset_key||row.key||'legacy';return `<span class="pill preset-chip ${esc(key)}">${esc(row.preset_name||presetNames[key]||key)}</span>`}
    function rrMetrics(p){const min=Number(p.entry_min),max=Number(p.entry_max),tp=Number(p.take_profit),sl=Number(p.stop_loss),mid=(min+max)/2,rr=entry=>entry>sl?(tp-entry)/(entry-sl):NaN;return{avg:rr(mid),worst:rr(max),best:rr(min)}}
    function rrClass(worst){if(!Number.isFinite(worst)||worst<1)return'bad';if(worst<1.5)return'thin';return'good'}
    function collectPresets(){return[...document.querySelectorAll('.preset-row')].map(row=>{const key=row.dataset.preset,get=field=>row.querySelector(`[data-preset-field="${field}"]`);return{key,name:row.querySelector('h3').textContent.trim(),enabled:get('enabled').checked,entry_min:get('entry_min').value,entry_max:get('entry_max').value,take_profit:get('take_profit').value,stop_loss:get('stop_loss').value,hard_exit:get('hard_exit').value,late_seconds:get('late_seconds').value,min_distance_usd:get('min_distance_usd').value,max_trades_per_label_per_market:get('max_trades_per_label_per_market').value}})}
    function updatePresetRisk(){for(const row of document.querySelectorAll('.preset-row')){const preset=collectPresets().find(item=>item.key===row.dataset.preset),rr=rrMetrics(preset),box=row.querySelector('[data-rr-box]');box.className=`rr-box ${rrClass(rr.worst)}`;box.innerHTML=`<strong>${Number.isFinite(rr.avg)?rr.avg.toFixed(2)+'R':'Invalid'}</strong><span class="subtle">Worst ${Number.isFinite(rr.worst)?rr.worst.toFixed(2)+'R':'Invalid'} / Best ${Number.isFinite(rr.best)?rr.best.toFixed(2)+'R':'Invalid'}</span>`}}
    function activePositionCell(r){return`<div class="stacked"><strong>${presetChip(r)} ${esc(r.market_slug||'n/a')} ${esc(r.outcome||'')}</strong><span class="subtle">Size ${valueMaybe(r.size_usd,2)} | Shares ${valueMaybe(r.shares,4)} | Entry ${valueMaybe(r.entry_price,2)}</span><span class="subtle">Giu ${Number(r.hold_seconds||0)}s</span></div>`}
    function activePriceCell(r){return`<div class="stacked"><strong>${valueMaybe(r.bid,2)}</strong><span class="subtle">Ask ${valueMaybe(r.ask,2)}</span></div>`}
    function activePnlCell(r){if(r.unrealized_pnl===null||r.unrealized_pnl===undefined)return'<span class="subtle">n/a</span>';const roi=r.unrealized_roi===null||r.unrealized_roi===undefined?'n/a':pct(r.unrealized_roi);return`<span class="${cls(r.unrealized_pnl)}"><strong>${Number(r.unrealized_pnl)>=0?'+':''}${fmt(r.unrealized_pnl)}</strong><br>${roi}</span>`}
    function setTab(name){document.querySelectorAll('main > section').forEach(el=>el.classList.toggle('hidden',el.id!==name));document.querySelectorAll('.tab').forEach(el=>el.classList.toggle('active',el.dataset.tab===name))}
    document.querySelectorAll('.tab').forEach(btn=>btn.addEventListener('click',()=>setTab(btn.dataset.tab)));
    document.getElementById('runForm').addEventListener('input',()=>{if(!bot?.running)formDirty=true;updatePresetRisk()});document.getElementById('runForm').addEventListener('change',()=>{if(!bot?.running)formDirty=true;updatePresetRisk()});
    function table(id,headers,rows,empty='Chua co du lieu'){const el=document.getElementById(id);if(!rows.length){el.innerHTML=`<tr><td class="empty">${empty}</td></tr>`;return}el.innerHTML=`<thead><tr>${headers.map(h=>`<th>${h[0]}</th>`).join('')}</tr></thead><tbody>`+rows.map(row=>`<tr>${headers.map(h=>`<td>${h[1](row)}</td>`).join('')}</tr>`).join('')+'</tbody>'}
    function drawEquity(rows){const svg=document.getElementById('equity'),w=svg.clientWidth||900,h=svg.clientHeight||280,p=28;if(!rows.length){svg.innerHTML='<text x="50%" y="50%" text-anchor="middle" fill="#91a09b">Chua co lenh da dong</text>';return}const ys=rows.map(r=>Number(r.cumulative)),minY=Math.min(0,...ys),maxY=Math.max(0,...ys),span=Math.max(.0001,maxY-minY),grouped={};for(const row of rows)(grouped[row.preset_key||'legacy']||=[]).push(row);const all=rows.slice().sort((a,b)=>Number(a.ts)-Number(b.ts)),indexOf=new Map(all.map((r,i)=>[r,i])),x=row=>p+(all.length===1?0:indexOf.get(row)*(w-p*2)/(all.length-1)),y=v=>h-p-((v-minY)/span)*(h-p*2),zero=y(0);const lines=Object.entries(grouped).map(([key,items])=>`<polyline points="${items.map(r=>`${x(r)},${y(Number(r.cumulative))}`).join(' ')}" fill="none" stroke="${presetColors[key]||'#7bb7ff'}" stroke-width="3" stroke-linejoin="round" stroke-linecap="round"/>`).join(''),markers=rows.map(r=>`<circle cx="${x(r)}" cy="${y(Number(r.cumulative))}" r="4.5" fill="${Number(r.pnl)>=0?'#4fd18b':'#ef735f'}" stroke="#0b0f0e" stroke-width="1.5"><title>${esc(r.preset_name||r.preset_key||'preset')} | ${esc(r.market_slug||'n/a')} | PnL ${Number(r.pnl)>=0?'+':''}${fmt(r.pnl)}</title></circle>`).join('');svg.setAttribute('viewBox',`0 0 ${w} ${h}`);svg.innerHTML=`<line x1="${p}" x2="${w-p}" y1="${zero}" y2="${zero}" stroke="#2a3633"/>${lines}${markers}`}
    function logClass(line){if(/stderr|fetch\/error|HTTP Error|Traceback|Error:/i.test(line))return'log-error';if(/Market target price:/i.test(line))return'log-target';if(/PAPER ENTER| ENTER /.test(line))return'log-enter';if(/PAPER EXIT| EXIT /.test(line))return'log-exit';if(/ARB /.test(line))return'log-arb';if(/ HOLD /.test(line))return'log-hold';if(/ WATCH |watch bid=|tick complete/.test(line))return'log-watch';if(/started|Slug:|Market end:|Watcher end:|Outcomes:|Strategies:|No wallet/.test(line))return'log-start';if(/ended|Paper trades|Arb events|No paper trades|No arb events|summary/i.test(line))return'log-end';if(/^\s*$/.test(line))return'log-muted';return'log-line'}
    function colorizeLogLine(line){const safe=esc(line).replace(/^(\[[^\]]+\])/,'<span class="log-time">$1</span>');return`<span class="log-line ${logClass(line)}">${safe}</span>`}
    function renderTerminal(stdoutLines,stderrLines){const lines=[];for(const line of stdoutLines||[])lines.push(line);if((stderrLines||[]).length){if(lines.length)lines.push('');lines.push('[stderr]');for(const line of stderrLines)lines.push(line)}for(const id of['terminal','terminalLive']){const terminal=document.getElementById(id);if(!lines.length){terminal.textContent='Dang cho log bot...';continue}terminal.innerHTML=lines.map(colorizeLogLine).join('\n');requestAnimationFrame(()=>{terminal.scrollTop=terminal.scrollHeight})}}
    function formPayload(){const form=new FormData(document.getElementById('runForm')),payload={};for(const[key,value]of form.entries())payload[key]=String(value).trim();for(const key of['seconds'])if(!payload[key])delete payload[key];payload.presets=collectPresets();return payload}
    function formFieldIsFocused(){const active=document.activeElement;return Boolean(active&&document.getElementById('runForm').contains(active)&&['INPUT','SELECT'].includes(active.tagName))}
    function syncPresetConfig(presets){if(!Array.isArray(presets))return;for(const preset of presets){const row=document.querySelector(`.preset-row[data-preset="${preset.key}"]`);if(!row)continue;for(const[key,value]of Object.entries(preset)){const input=row.querySelector(`[data-preset-field="${key}"]`);if(!input)continue;if(input.type==='checkbox')input.checked=Boolean(value);else input.value=value}}updatePresetRisk()}
    function syncFormConfig(config){for(const[key,value]of Object.entries(config)){if(key==='presets')continue;const input=document.getElementById(key);if(input&&value!==null&&value!==undefined){if(input.type==='checkbox')input.checked=Boolean(value);else input.value=value}}syncPresetConfig(config.presets);formHydrated=true}
    async function postJSON(url,payload={}){const res=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)}),data=await res.json();if(!res.ok)throw new Error(data.error||`Request failed: ${res.status}`);return data}
    function renderBotStatus(data,options={}){bot=data;document.getElementById('botDot').classList.toggle('on',Boolean(data.running));document.getElementById('botStatus').textContent=data.running?`Dang chay PID ${data.pid} tu ${data.started_at||'bay gio'}`:(data.returncode===null?'Bot dang nghi':`Bot da dung, ma ${data.returncode}`);document.getElementById('pidPill').textContent=data.running?`PID ${data.pid}`:'Chua chay';document.getElementById('dataFile').textContent=data.data_file?`Data: ${data.data_file}`:'Data: chua co run';document.getElementById('dataFile').title=data.db_path||'';document.getElementById('startBtn').disabled=Boolean(data.running);document.getElementById('stopBtn').disabled=!data.running;document.getElementById('resetBtn').disabled=Boolean(data.running);document.querySelectorAll('#runForm input,#runForm select').forEach(input=>{input.disabled=Boolean(data.running)});const hasConfig=Boolean(data.config&&data.config.url),shouldSyncForm=hasConfig&&(options.forceFormSync||data.running||!formHydrated||!formDirty)&&(options.forceFormSync||!formFieldIsFocused());if(shouldSyncForm)syncFormConfig(data.config);renderTerminal(data.stdout_tail||[],data.stderr_tail||[])}
    async function refreshBot(){const res=await fetch('/api/bot/status',{cache:'no-store'});renderBotStatus(await res.json())}
    async function runCommand(action){const notice=document.getElementById('controlNotice');try{if(action==='start'){notice.textContent='Dang khoi dong bot...';const status=await postJSON('/api/bot/start',formPayload());formDirty=false;renderBotStatus(status,{forceFormSync:true});await refresh();notice.textContent='Bot da chay.'}else if(action==='stop'){notice.textContent='Dang dung bot...';renderBotStatus(await postJSON('/api/bot/stop'),{forceFormSync:true});await refresh();notice.textContent='Bot da dung.'}else if(action==='reset'){notice.textContent='Dang xoa du lieu local...';renderBotStatus(await postJSON('/api/data/reset'));await refresh();notice.textContent='Da xoa du lieu local.'}else if(action==='close'){notice.textContent='Dang dong app va dung bot...';await postJSON('/api/app/shutdown');notice.textContent='App da dong. Co the dong tab trinh duyet.';setTimeout(()=>window.close(),300)}}catch(error){notice.textContent=error.message}}
    function presetConfigByKey(){const config=state?.run?.config?.presets||collectPresets();return Object.fromEntries(config.map(p=>[p.key,p]))}
    function plannedRR(row){const p=presetConfigByKey()[row.preset_key];if(!p)return'<span class="subtle">n/a</span>';const rr=rrMetrics(p);return Number.isFinite(rr.avg)?`${rr.avg.toFixed(2)}R`:'<span class="neg">Invalid</span>'}
    function renderPresetFilter(){const select=document.getElementById('presetFilter'),current=select.value,keys=new Map();for(const p of collectPresets())keys.set(p.key,p.name);for(const row of state?.trades||[])keys.set(row.preset_key,row.preset_name||row.preset_key);select.innerHTML='<option value="all">Tat ca preset</option>'+[...keys.entries()].map(([key,name])=>`<option value="${esc(key)}">${esc(name)}</option>`).join('');select.value=keys.has(current)?current:'all'}
    function renderDecisionBoard(data){const active=data.active_positions||[],snaps=data.snapshots||[],keys=collectPresets().map(p=>p.key);document.getElementById('decisionBoard').innerHTML=keys.map(key=>{const open=active.filter(r=>r.preset_key===key),latest=snaps.find(r=>r.preset_key===key),status=open.length?'HOLD':(latest?'WATCH':'WAIT');return`<div class="decision-card ${esc(key)}"><div class="row" style="justify-content:space-between"><strong>${esc(presetNames[key]||key)}</strong><span class="pill">${status}</span></div><p class="subtle">Open: ${open.length} | Bid ${valueMaybe(latest?.bid,2)} | Ask ${valueMaybe(latest?.ask,2)}</p><p class="subtle">BTC ${valueMaybe(latest?.btc_price,2)} | ${esc(latest?.time||'no snapshot')}</p></div>`}).join('')}
    function render(data){state=data;document.getElementById('generated').textContent=`Cap nhat ${data.generated_at}`;renderPresetFilter();const o=data.overall;document.getElementById('metrics').innerHTML=[['Trades',o.trades],['PnL closed',money(o.pnl)],['Open PnL',money(o.active_unrealized_pnl)],['Total PnL',money(o.total_with_active_pnl)],['Win rate',pct(o.win_rate)],['ROI size',pct(o.roi)]].map(m=>`<div class="panel metric"><div class="label">${m[0]}</div><div class="value">${m[1]}</div></div>`).join('');table('presetSummary',[['Preset',presetChip],['Lenh',r=>r.trades],['PnL',r=>money(r.pnl)],['Win',r=>pct(r.win_rate)],['Avg entry',r=>fmt(r.avg_entry,2)],['Avg PnL',r=>money(r.avg_pnl)],['Risk exits',r=>r.risk_exits]],data.preset_summary||[],'Chua co lenh da dong');const activeRows=data.active_positions||[];document.getElementById('activeOverviewCount').textContent=`${activeRows.length} lenh`;table('activeOverview',[['Vi the',activePositionCell],['Gia',activePriceCell],['PnL',activePnlCell]],activeRows,'Khong co lenh dang mo');drawEquity(data.preset_equity||data.equity||[]);document.getElementById('health').innerHTML=`<p><span class="pill">Thoat cung</span> ${data.health.hard_exit_count}</p><p><span class="pill">Cat lo</span> ${data.health.stop_loss_count}</p><p><span class="pill">Lo lon nhat</span> ${money(data.health.largest_loss)}</p><p><span class="pill">Ty trong top 3 PnL</span> ${pct(data.health.top_3_share)}</p><p class="subtle">Neu loi nhuan tap trung vao vai lenh, tiep tuc chay paper truoc khi tang size.</p>`;table('markets',[['Preset',presetChip],['Market',r=>r.market_slug],['Lenh',r=>r.trades],['PnL',r=>money(r.pnl)],['Win',r=>pct(r.win_rate)],['Worst',r=>money(r.worst)]],data.markets||[]);table('outcomes',[['Preset',presetChip],['Outcome',r=>r.outcome],['Lenh',r=>r.trades],['PnL',r=>money(r.pnl)],['Win',r=>pct(r.win_rate)],['Entry TB',r=>fmt(r.avg_entry,2)],['Risk exits',r=>r.risk_exits]],data.outcomes||[]);renderTrades();table('activePositions',[['Vi the',activePositionCell],['Gia',activePriceCell],['PnL',activePnlCell],['Reason',r=>esc(r.entry_reason||'')]],activeRows,'Khong co lenh dang mo');table('snapshots',[['Time',r=>r.time],['Preset',presetChip],['Label',r=>esc(r.label)],['Bid',r=>fmt(r.bid,2)],['Ask',r=>fmt(r.ask,2)],['Bid size',r=>fmt(r.bid_size,2)],['Ask size',r=>fmt(r.ask_size,2)],['BTC',r=>valueMaybe(r.btc_price,2)]],data.snapshots||[]);renderDecisionBoard(data);document.getElementById('arbSummary').innerHTML=data.arb.length?data.arb.map(r=>`<p><span class="pill">${esc(r.kind)}</span> events=${r.events} max=${fmt(r.max_edge)} avg=${fmt(r.avg_edge)}</p>`).join(''):'<p class="empty">Chua co su kien arb.</p>';table('arbEvents',[['Time',r=>r.time],['Market',r=>r.market_slug],['Loai',r=>r.kind],['Up/Yes',r=>fmt(r.yes_price,2)],['Down/No',r=>fmt(r.no_price,2)],['Edge',r=>money(r.edge)]],data.arb_events||[])}
    function renderTrades(){if(!state)return;const q=document.getElementById('filter').value.toLowerCase(),presetMode=document.getElementById('presetFilter').value,mode=document.getElementById('resultFilter').value,rows=state.trades.filter(r=>{const hay=`${r.market_slug} ${r.outcome} ${r.exit_reason} ${r.entry_reason} ${r.preset_name}`.toLowerCase();if(q&&!hay.includes(q))return false;if(presetMode!=='all'&&r.preset_key!==presetMode)return false;if(mode==='wins'&&Number(r.pnl)<=0)return false;if(mode==='losses'&&Number(r.pnl)>=0)return false;return true});table('tradesTable',[['Preset',presetChip],['Market',r=>r.market_slug],['Outcome',r=>r.outcome],['Vao',r=>r.entry_time],['Thoat',r=>r.exit_time],['Giu',r=>`${r.hold_seconds}s`],['Entry',r=>fmt(r.entry_price,2)],['Exit',r=>fmt(r.exit_price,2)],['PnL',r=>money(r.pnl)],['R:R',plannedRR],['Exit reason',r=>esc(r.exit_reason)]],rows,'Khong co lenh phu hop')}
    document.getElementById('filter').addEventListener('input',renderTrades);document.getElementById('presetFilter').addEventListener('change',renderTrades);document.getElementById('resultFilter').addEventListener('change',renderTrades);document.getElementById('runForm').addEventListener('submit',event=>{event.preventDefault();runCommand('start')});document.getElementById('stopBtn').addEventListener('click',()=>runCommand('stop'));document.getElementById('resetBtn').addEventListener('click',()=>runCommand('reset'));document.getElementById('closeAppBtn').addEventListener('click',()=>runCommand('close'));
    async function refresh(){const res=await fetch('/api/dashboard',{cache:'no-store'});render(await res.json())}
    updatePresetRisk();refresh();refreshBot();setInterval(refresh,2000);setInterval(refreshBot,2000);
  </script>
</body>
</html>"""


class DashboardHandler(BaseHTTPRequestHandler):
    db_path: Path = DEFAULT_DB

    def log_message(self, format: str, *args: Any) -> None:
        return

    def read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("Expected a JSON object")
        return payload

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/":
            self.send_text(DASHBOARD_HTML, "text/html; charset=utf-8")
            return
        if parsed.path == "/api/dashboard":
            self.send_json(dashboard_payload(current_dashboard_db_path()))
            return
        if parsed.path == "/api/bot/status":
            self.send_json(bot_status())
            return
        self.send_error(404)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        try:
            if parsed.path == "/api/bot/start":
                self.send_json(start_bot(self.read_json_body(), self.db_path))
                return
            if parsed.path == "/api/bot/stop":
                self.send_json(stop_bot())
                return
            if parsed.path == "/api/data/reset":
                self.send_json(reset_data(current_dashboard_db_path()))
                return
            if parsed.path == "/api/app/shutdown":
                status = stop_bot()
                self.send_json({"ok": True, "bot": status})
                threading.Thread(target=self.server.shutdown, daemon=True).start()
                return
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
            self.send_json({"error": str(exc)}, status=400)
            return
        self.send_error(404)

    def send_json(self, data: dict[str, Any], status: int = 200) -> None:
        payload = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def send_text(self, text: str, content_type: str) -> None:
        payload = text.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)


def serve_dashboard(db_path: Path, host: str, port: int, open_browser: bool) -> None:
    global DASHBOARD_DB_PATH
    DASHBOARD_DB_PATH = initial_dashboard_db_path(db_path)
    if DASHBOARD_DB_PATH != empty_dashboard_db_path():
        init_db(DASHBOARD_DB_PATH).close()
    clear_bot_logs()
    handler = type("BoundDashboardHandler", (DashboardHandler,), {"db_path": db_path})
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    url = f"http://{host}:{port}"
    print(f"Dashboard: {url}")
    print("Dùng nút Đóng app trong dashboard, đóng cửa sổ này, hoặc nhấn Ctrl+C để dừng.")
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
    finally:
        stop_bot()
        server.server_close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only Polymarket realtime paper tester")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="SQLite database path")
    sub = parser.add_subparsers(dest="cmd", required=True)

    auto = sub.add_parser("watch-url", help="Auto paper-test all built-in strategies for a Polymarket URL/slug")
    auto.add_argument("url_or_slug", help="Polymarket event URL or slug")
    auto.add_argument("--seconds", type=int, help="Optional runtime override. Default: run until market end + buffer")
    auto.add_argument("--poll", type=int, default=DEFAULT_POLL_SECONDS)
    auto.add_argument("--size-usd", type=float, default=1.0)
    auto.add_argument("--entry-min", type=float, default=DEFAULT_ENTRY_MIN)
    auto.add_argument("--entry-max", type=float, default=DEFAULT_ENTRY_MAX)
    auto.add_argument("--take-profit", type=float, default=DEFAULT_TAKE_PROFIT)
    auto.add_argument("--stop-loss", type=float, default=DEFAULT_STOP_LOSS)
    auto.add_argument("--hard-exit", type=float, default=DEFAULT_HARD_EXIT)
    auto.add_argument("--late-seconds", type=int, default=DEFAULT_LATE_SECONDS)
    auto.add_argument("--min-distance-usd", type=float, default=DEFAULT_MIN_DISTANCE_USD)
    auto.add_argument(
        "--entry-window-seconds",
        type=int,
        default=DEFAULT_ENTRY_WINDOW_SECONDS,
        help="Only enter when the market has this many seconds or less remaining. Use 0 to disable.",
    )
    auto.add_argument("--max-trades-per-label-per-market", type=int, default=DEFAULT_MAX_TRADES_PER_LABEL_PER_MARKET)
    auto.add_argument(
        "--market-lock-after-loss",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_MARKET_LOCK_AFTER_LOSS,
        help="Stop opening new positions in the current market after any losing trade.",
    )
    auto.add_argument("--no-entry-after-seconds", type=int, default=DEFAULT_NO_ENTRY_AFTER_SECONDS, help="Do not enter after this many seconds from market start. Use 0 to disable.")
    auto.add_argument("--max-sum-asks", type=float, default=DEFAULT_MAX_SUM_ASKS, help="Do not enter if selected ask + opposite ask is above this value. Use 0 to disable.")
    auto.add_argument("--arb-buffer", type=float, default=0.01, help="Extra slippage/safety buffer after taker fees")
    auto.add_argument(
        "--fee-rate",
        type=float,
        default=DEFAULT_TAKER_FEE_RATE,
        help="Fallback Polymarket taker fee rate if /fee-rate lookup fails. Crypto default: 0.07",
    )
    auto.add_argument("--end-buffer-seconds", type=int, default=1, help="Stop this many seconds after market end")
    auto.add_argument("--chain-count", type=int, default=1, help="Run this many consecutive timestamped markets")
    auto.add_argument("--chain-interval-seconds", type=int, default=300, help="Seconds to add to the slug timestamp for each next market")
    auto.add_argument("--lookup-timeout-seconds", type=int, default=60, help="How long to wait for a next market slug to appear")
    auto.add_argument("--presets", help="JSON list of enabled dashboard presets")

    directional = sub.add_parser("watch-directional", help="Paper-test one outcome token")
    directional.add_argument("--token-id", required=True)
    directional.add_argument("--label", required=True)
    directional.add_argument("--target-price", type=float)
    directional.add_argument("--direction", choices=["UP", "DOWN"], default="UP")
    directional.add_argument("--size-usd", type=float, default=1.0)
    directional.add_argument("--entry-min", type=float, default=0.65)
    directional.add_argument("--entry-max", type=float, default=0.72)
    directional.add_argument("--take-profit", type=float, default=0.85)
    directional.add_argument("--stop-loss", type=float, default=0.55)
    directional.add_argument("--hard-exit", type=float, default=0.50)
    directional.add_argument("--late-seconds", type=int, default=30)
    directional.add_argument("--entry-window-seconds", type=int, default=0, help="Only enter with this many seconds or less remaining. Use 0 to disable.")
    directional.add_argument("--min-distance-usd", type=float, default=50.0)
    directional.add_argument("--poll", type=int, default=DEFAULT_POLL_SECONDS)
    directional.add_argument("--seconds", type=int, default=900)

    arb = sub.add_parser("watch-arb", help="Watch YES+NO complete-set pricing")
    arb.add_argument("--yes-token-id", required=True)
    arb.add_argument("--no-token-id", required=True)
    arb.add_argument("--label", required=True)
    arb.add_argument("--buffer", type=float, default=0.01, help="Extra slippage/safety buffer after taker fees")
    arb.add_argument(
        "--fee-rate",
        type=float,
        default=DEFAULT_TAKER_FEE_RATE,
        help="Fallback Polymarket taker fee rate if /fee-rate lookup fails. Crypto default: 0.07",
    )
    arb.add_argument("--poll", type=int, default=DEFAULT_POLL_SECONDS)
    arb.add_argument("--seconds", type=int, default=900)

    report_parser = sub.add_parser("report", help="Summarize local paper results")
    report_parser.add_argument("--web", action="store_true", help="Open the local dashboard after printing report")
    report_parser.add_argument("--port", type=int, default=8765, help="Dashboard port when using --web")
    report_parser.add_argument("--host", default="127.0.0.1", help="Dashboard host when using --web")
    summary_parser = sub.add_parser("summary", help="Aggregate performance across many runs")
    summary_parser.add_argument("--limit", type=int, default=20, help="Number of recent markets to show")
    summary_parser.add_argument("--export-csv", type=Path, help="Optional CSV export path for all paper trades")
    dashboard_parser = sub.add_parser("dashboard", help="Open the local web dashboard")
    dashboard_parser.add_argument("--port", type=int, default=8765)
    dashboard_parser.add_argument("--host", default="127.0.0.1")
    dashboard_parser.add_argument("--no-open", action="store_true", help="Do not open the browser automatically")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.cmd == "watch-directional":
            cfg = DirectionalConfig(
                token_id=args.token_id,
                label=args.label,
                preset_key="manual",
                preset_name="Manual",
                market_slug=None,
                outcome=None,
                target_price=args.target_price,
                direction=args.direction,
                size_usd=args.size_usd,
                entry_min=args.entry_min,
                entry_max=args.entry_max,
                take_profit=args.take_profit,
                stop_loss=args.stop_loss,
                hard_exit=args.hard_exit,
                late_seconds=args.late_seconds,
                entry_window_seconds=max(0, args.entry_window_seconds) or None,
                min_distance_usd=args.min_distance_usd,
                poll=args.poll,
                seconds=args.seconds,
            )
            watch_directional(cfg, args.db)
        elif args.cmd == "watch-arb":
            watch_arb(
                yes_token_id=args.yes_token_id,
                no_token_id=args.no_token_id,
                label=args.label,
                buffer=args.buffer,
                fee_rate=args.fee_rate,
                poll=args.poll,
                seconds=args.seconds,
                db_path=args.db,
            )
        elif args.cmd == "report":
            report(args.db)
            if args.web:
                serve_dashboard(args.db, host=args.host, port=args.port, open_browser=True)
        elif args.cmd == "summary":
            summary(args.db, limit=args.limit, export_csv=args.export_csv)
        elif args.cmd == "dashboard":
            serve_dashboard(args.db, host=args.host, port=args.port, open_browser=not args.no_open)
        elif args.cmd == "watch-url":
            if args.chain_count > 1:
                watch_chain(
                    url_or_slug=args.url_or_slug,
                    count=args.chain_count,
                    interval_seconds=args.chain_interval_seconds,
                    db_path=args.db,
                    seconds=args.seconds,
                    poll=args.poll,
                    size_usd=args.size_usd,
                    entry_min=args.entry_min,
                    entry_max=args.entry_max,
                    take_profit=args.take_profit,
                    stop_loss=args.stop_loss,
                    hard_exit=args.hard_exit,
                    late_seconds=args.late_seconds,
                    min_distance_usd=args.min_distance_usd,
                    entry_window_seconds=max(0, args.entry_window_seconds),
                    max_trades_per_label_per_market=max(0, args.max_trades_per_label_per_market),
                    market_lock_after_loss=args.market_lock_after_loss,
                    no_entry_after_seconds=max(0, args.no_entry_after_seconds),
                    max_sum_asks=max(0.0, args.max_sum_asks),
                    arb_buffer=args.arb_buffer,
                    fee_rate=args.fee_rate,
                    end_buffer_seconds=args.end_buffer_seconds,
                    lookup_timeout_seconds=args.lookup_timeout_seconds,
                    presets=args.presets,
                )
            else:
                watch_url(
                    url_or_slug=args.url_or_slug,
                    db_path=args.db,
                    seconds=args.seconds,
                    poll=args.poll,
                    size_usd=args.size_usd,
                    entry_min=args.entry_min,
                    entry_max=args.entry_max,
                    take_profit=args.take_profit,
                    stop_loss=args.stop_loss,
                    hard_exit=args.hard_exit,
                    late_seconds=args.late_seconds,
                    min_distance_usd=args.min_distance_usd,
                    entry_window_seconds=max(0, args.entry_window_seconds),
                    max_trades_per_label_per_market=max(0, args.max_trades_per_label_per_market),
                    market_lock_after_loss=args.market_lock_after_loss,
                    no_entry_after_seconds=max(0, args.no_entry_after_seconds),
                    max_sum_asks=max(0.0, args.max_sum_asks),
                    arb_buffer=args.arb_buffer,
                    fee_rate=args.fee_rate,
                    end_buffer_seconds=args.end_buffer_seconds,
                    lookup_timeout_seconds=args.lookup_timeout_seconds,
                    presets=args.presets,
                )
        return 0
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
