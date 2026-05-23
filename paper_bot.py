#!/usr/bin/env python3
"""Read-only realtime paper tester for Polymarket strategies."""

from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
import sys
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
USER_AGENT = "SecondBrainPolymarketPaperBot/0.1"


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
    outcomes: list[Outcome]


def utc_now() -> int:
    return int(time.time())


def iso(ts: int | None = None) -> str:
    return datetime.fromtimestamp(ts or utc_now(), tz=timezone.utc).isoformat()


def http_json(url: str, timeout: int = 20) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


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
    return MarketInfo(
        slug=slug,
        title=title,
        end_ts=parse_ts(market.get("endDate")),
        outcomes=outcomes,
    )


def fetch_market_info_with_retry(url_or_slug: str, timeout_seconds: int) -> MarketInfo:
    deadline = utc_now() + max(0, timeout_seconds)
    last_error: ValueError | None = None
    while True:
        try:
            return fetch_market_info(url_or_slug)
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


def should_enter(cfg: DirectionalConfig, book: BookTop, btc_price: float | None) -> tuple[bool, str]:
    if book.ask is None:
        return False, "no_ask"
    if not (cfg.entry_min <= book.ask <= cfg.entry_max):
        return False, "ask_outside_entry_band"
    ok, reason = target_distance_ok(cfg, btc_price)
    if not ok:
        return False, reason
    return True, f"entry_band:{book.ask:.3f};{reason}"


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


def watch_directional(cfg: DirectionalConfig, db_path: Path) -> None:
    con = init_db(db_path)
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
                enter, reason = should_enter(cfg, book, btc)
                if enter and book.ask:
                    shares = cfg.size_usd / book.ask
                    position = PaperPosition(utc_now(), book.ask, shares, cfg.size_usd, reason)
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
                    print(f"[{iso()}] PAPER EXIT bid={book.bid:.3f} pnl={pnl:.4f} reason={reason}")
                    position = None
                else:
                    unrealized = (position.shares * book.bid - position.size_usd) if book.bid else math.nan
                    print(
                        f"[{iso()}] hold bid={book.bid} ask={book.ask} unrealized={unrealized:.4f} "
                        f"remaining={remaining}s"
                    )
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, KeyError) as exc:
            print(f"[{iso()}] fetch/error: {exc}", file=sys.stderr)
        time.sleep(cfg.poll)
    print(f"[{iso()}] paper watch ended: {cfg.label}")


def watch_arb(
    yes_token_id: str,
    no_token_id: str,
    label: str,
    buffer: float,
    poll: int,
    seconds: int,
    db_path: Path,
) -> None:
    con = init_db(db_path)
    end_ts = utc_now() + seconds
    print(f"[{iso()}] arb watch started: {label}")
    while utc_now() < end_ts:
        try:
            yes = fetch_book(yes_token_id)
            no = fetch_book(no_token_id)
            payload = {"yes": yes.raw, "no": no.raw}
            if yes.ask is not None and no.ask is not None:
                buy_total = yes.ask + no.ask + buffer
                edge = 1.0 - buy_total
                if edge > 0:
                    record_arb_event(con, label, None, "buy_complete_set", yes.ask, no.ask, edge, payload)
                    print(f"[{iso()}] ARB buy both asks yes={yes.ask:.3f} no={no.ask:.3f} edge={edge:.4f}")
            if yes.bid is not None and no.bid is not None:
                sell_total = yes.bid + no.bid - buffer
                edge = sell_total - 1.0
                if edge > 0:
                    record_arb_event(con, label, None, "sell_complete_set", yes.bid, no.bid, edge, payload)
                    print(f"[{iso()}] ARB sell both bids yes={yes.bid:.3f} no={no.bid:.3f} edge={edge:.4f}")
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
    target_price: float | None,
    min_distance_usd: float,
    arb_buffer: float,
    end_buffer_seconds: int,
    lookup_timeout_seconds: int,
) -> None:
    market = fetch_market_info_with_retry(url_or_slug, lookup_timeout_seconds)
    if len(market.outcomes) < 2:
        raise ValueError("watch-url needs at least two outcomes")
    end_ts = default_end_ts(market, seconds, end_buffer_seconds)
    run_seconds = max(0, end_ts - utc_now())
    con = init_db(db_path)
    positions: dict[str, PaperPosition | None] = {outcome.token_id: None for outcome in market.outcomes}
    configs: list[DirectionalConfig] = []
    for index, outcome in enumerate(market.outcomes):
        configs.append(
            DirectionalConfig(
                token_id=outcome.token_id,
                label=f"{market.slug} {outcome.name} band_scalp",
                market_slug=market.slug,
                outcome=outcome.name,
                target_price=target_price,
                direction=outcome_direction(outcome.name, index),
                size_usd=size_usd,
                entry_min=0.65,
                entry_max=0.72,
                take_profit=0.85,
                stop_loss=0.55,
                hard_exit=0.50,
                late_seconds=30,
                min_distance_usd=min_distance_usd,
                poll=poll,
                seconds=run_seconds,
            )
        )
    print(f"[{iso()}] auto paper watch started: {market.title}")
    print(f"Slug: {market.slug}")
    if market.end_ts:
        print(f"Market end: {iso(market.end_ts)}")
    print(f"Watcher end: {iso(end_ts)}")
    print("Outcomes:")
    for outcome in market.outcomes:
        print(f"  {outcome.name}: {outcome.token_id}")
    print("Strategies: band_scalp for every outcome + complete_set_arb for first two outcomes")
    print("No wallet. No private key. No real orders.")
    while utc_now() < end_ts:
        remaining = max(0, end_ts - utc_now())
        try:
            books = {cfg.token_id: fetch_book(cfg.token_id) for cfg in configs}
            btc = fetch_btc_price() if target_price is not None else None
            for cfg in configs:
                book = books[cfg.token_id]
                save_snapshot(con, cfg.label, cfg.token_id, book, btc)
                pos = positions[cfg.token_id]
                if pos is None:
                    enter, reason = should_enter(cfg, book, btc)
                    if enter and book.ask:
                        shares = cfg.size_usd / book.ask
                        positions[cfg.token_id] = PaperPosition(utc_now(), book.ask, shares, cfg.size_usd, reason)
                        print(f"[{iso()}] ENTER {cfg.label} ask={book.ask:.3f} shares={shares:.4f} reason={reason}")
                    else:
                        print(f"[{iso()}] WATCH {cfg.label} bid={book.bid} ask={book.ask} reason={reason}")
                else:
                    exit_now, reason = should_exit(cfg, pos, book, btc, remaining)
                    if exit_now and book.bid is not None:
                        pnl = record_trade(con, cfg, pos, book.bid, reason, strategy="auto_band_scalp")
                        positions[cfg.token_id] = None
                        print(f"[{iso()}] EXIT {cfg.label} bid={book.bid:.3f} pnl={pnl:.4f} reason={reason}")
                    else:
                        unrealized = (pos.shares * book.bid - pos.size_usd) if book.bid else math.nan
                        print(f"[{iso()}] HOLD {cfg.label} bid={book.bid} unrealized={unrealized:.4f}")
            first, second = market.outcomes[0], market.outcomes[1]
            a = books[first.token_id]
            b = books[second.token_id]
            payload = {first.name: a.raw, second.name: b.raw}
            if a.ask is not None and b.ask is not None:
                edge = 1.0 - (a.ask + b.ask + arb_buffer)
                if edge > 0:
                    record_arb_event(con, f"{market.slug} complete_set", market.slug, "buy_complete_set", a.ask, b.ask, edge, payload)
                    print(f"[{iso()}] ARB buy both asks {first.name}={a.ask:.3f} {second.name}={b.ask:.3f} edge={edge:.4f}")
            if a.bid is not None and b.bid is not None:
                edge = (a.bid + b.bid - arb_buffer) - 1.0
                if edge > 0:
                    record_arb_event(con, f"{market.slug} complete_set", market.slug, "sell_complete_set", a.bid, b.bid, edge, payload)
                    print(f"[{iso()}] ARB sell both bids {first.name}={a.bid:.3f} {second.name}={b.bid:.3f} edge={edge:.4f}")
            print(f"[{iso()}] tick complete remaining={remaining}s")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, KeyError) as exc:
            print(f"[{iso()}] fetch/error: {exc}", file=sys.stderr)
        time.sleep(poll)
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
    target_price: float | None,
    min_distance_usd: float,
    arb_buffer: float,
    end_buffer_seconds: int,
    lookup_timeout_seconds: int,
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
            target_price=target_price,
            min_distance_usd=min_distance_usd,
            arb_buffer=arb_buffer,
            end_buffer_seconds=end_buffer_seconds,
            lookup_timeout_seconds=lookup_timeout_seconds,
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


def dashboard_payload(db_path: Path) -> dict[str, Any]:
    con = init_db(db_path)
    con.row_factory = sqlite3.Row
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
        SELECT exit_ts AS ts, pnl, {slug_expr()} AS market_slug, {outcome_expr()} AS outcome, exit_reason
        FROM paper_trades
        ORDER BY exit_ts, id
        """
    ).fetchall()
    cumulative = 0.0
    equity: list[dict[str, Any]] = []
    for row in equity_rows:
        cumulative += float(row["pnl"])
        item = dict(row)
        item["cumulative"] = cumulative
        item["time"] = iso(int(row["ts"])) if row["ts"] else ""
        equity.append(item)

    markets = rows_to_dicts(
        con.execute(
            f"""
            SELECT {slug_expr()} AS market_slug, COUNT(*) AS trades, SUM(pnl) AS pnl,
                   AVG(pnl) AS avg_pnl, {win_rate_expr()} AS win_rate,
                   MIN(pnl) AS worst, MAX(pnl) AS best, MAX(exit_ts) AS last_exit
            FROM paper_trades
            GROUP BY market_slug
            ORDER BY MAX(exit_ts) DESC
            LIMIT 50
            """
        ).fetchall()
    )
    outcomes = rows_to_dicts(
        con.execute(
            f"""
            SELECT {outcome_expr()} AS outcome, COUNT(*) AS trades, SUM(pnl) AS pnl,
                   AVG(pnl) AS avg_pnl, {win_rate_expr()} AS win_rate,
                   AVG(entry_price) AS avg_entry, AVG(exit_price) AS avg_exit,
                   SUM(CASE WHEN exit_reason IN ('hard_exit', 'stop_loss') THEN 1 ELSE 0 END) AS risk_exits
            FROM paper_trades
            GROUP BY outcome
            ORDER BY pnl DESC
            """
        ).fetchall()
    )
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
            SELECT {slug_expr()} AS market_slug, strategy, {outcome_expr()} AS outcome,
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

    latest_snapshots = rows_to_dicts(
        con.execute(
            """
            SELECT s.ts, s.label, s.token_id, s.bid, s.ask, s.last, s.bid_size, s.ask_size, s.btc_price
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
    return {
        "generated_at": iso(),
        "overall": overall,
        "equity": equity,
        "markets": markets,
        "outcomes": outcomes,
        "exits": exits,
        "trades": trades,
        "snapshots": latest_snapshots,
        "arb": arb,
        "arb_events": arb_events,
        "health": health,
    }


DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Polymarket Paper Dashboard</title>
  <style>
    :root {
      --bg: #f6f7f3;
      --surface: #ffffff;
      --ink: #1f2428;
      --muted: #667078;
      --border: #d8ddd4;
      --accent: #245b4f;
      --accent-2: #2f6f9f;
      --good: #16764f;
      --bad: #b33a3a;
      --warn: #9a6b16;
      --soft: #eef2ea;
      --radius: 8px;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--ink); }
    header { position: sticky; top: 0; z-index: 2; background: rgba(246,247,243,.94); border-bottom: 1px solid var(--border); backdrop-filter: blur(10px); }
    .wrap { max-width: 1280px; margin: 0 auto; padding: 18px 20px; }
    .topbar { display: flex; justify-content: space-between; gap: 16px; align-items: center; }
    h1 { font-size: 22px; margin: 0; line-height: 1.15; letter-spacing: 0; }
    .subtle { color: var(--muted); font-size: 13px; }
    .tabs { display: flex; gap: 6px; margin-top: 16px; }
    .tab { border: 1px solid var(--border); background: var(--surface); color: var(--ink); padding: 8px 12px; border-radius: var(--radius); cursor: pointer; font-size: 14px; }
    .tab.active { background: var(--accent); color: white; border-color: var(--accent); }
    main { max-width: 1280px; margin: 0 auto; padding: 18px 20px 40px; }
    .grid { display: grid; gap: 12px; }
    .metrics { grid-template-columns: repeat(4, minmax(0, 1fr)); }
    .two { grid-template-columns: minmax(0, 1.4fr) minmax(320px, .8fr); }
    .panel { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 14px; }
    .metric .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
    .metric .value { font-size: 24px; margin-top: 8px; font-weight: 680; }
    .pos { color: var(--good); }
    .neg { color: var(--bad); }
    h2 { font-size: 15px; margin: 0 0 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { text-align: left; border-bottom: 1px solid var(--border); padding: 9px 8px; vertical-align: top; }
    th { color: var(--muted); font-weight: 620; background: #fafbf8; position: sticky; top: 0; }
    .table-scroll { max-height: 520px; overflow: auto; border: 1px solid var(--border); border-radius: var(--radius); }
    .chart { width: 100%; height: 260px; display: block; background: linear-gradient(#fff, #fbfcfa); border: 1px solid var(--border); border-radius: var(--radius); }
    .hidden { display: none; }
    .row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
    input, select { border: 1px solid var(--border); border-radius: var(--radius); padding: 8px 10px; background: white; min-height: 36px; }
    .pill { display: inline-flex; padding: 3px 8px; border-radius: 999px; background: var(--soft); color: var(--ink); font-size: 12px; }
    .risk { border-left: 4px solid var(--warn); }
    .empty { color: var(--muted); padding: 24px; text-align: center; }
    @media (max-width: 900px) { .metrics, .two { grid-template-columns: 1fr; } .topbar { align-items: flex-start; flex-direction: column; } }
  </style>
</head>
<body>
  <header>
    <div class="wrap">
      <div class="topbar">
        <div>
          <h1>Polymarket Paper Dashboard</h1>
          <div class="subtle" id="generated">Loading local paper data...</div>
        </div>
        <div class="subtle">Read-only SQLite dashboard</div>
      </div>
      <nav class="tabs">
        <button class="tab active" data-tab="overview">Overview</button>
        <button class="tab" data-tab="trades">Trades</button>
        <button class="tab" data-tab="live">Live</button>
      </nav>
    </div>
  </header>
  <main>
    <section id="overview">
      <div class="grid metrics" id="metrics"></div>
      <div class="grid two" style="margin-top:12px">
        <div class="panel">
          <h2>Equity Curve</h2>
          <svg id="equity" class="chart" role="img" aria-label="Cumulative paper PnL"></svg>
        </div>
        <div class="panel risk">
          <h2>Strategy Health</h2>
          <div id="health"></div>
        </div>
      </div>
      <div class="grid two" style="margin-top:12px">
        <div class="panel">
          <h2>By Market</h2>
          <div class="table-scroll"><table id="markets"></table></div>
        </div>
        <div class="panel">
          <h2>By Outcome</h2>
          <div class="table-scroll"><table id="outcomes"></table></div>
        </div>
      </div>
    </section>
    <section id="trades" class="hidden">
      <div class="panel">
        <div class="row" style="justify-content:space-between;margin-bottom:12px">
          <h2 style="margin:0">Trade Log</h2>
          <div class="row">
            <input id="filter" placeholder="Filter market, outcome, reason">
            <select id="resultFilter">
              <option value="all">All trades</option>
              <option value="wins">Profitable</option>
              <option value="losses">Losing</option>
            </select>
          </div>
        </div>
        <div class="table-scroll"><table id="tradesTable"></table></div>
      </div>
    </section>
    <section id="live" class="hidden">
      <div class="grid two">
        <div class="panel">
          <h2>Latest Orderbook Snapshots</h2>
          <div class="table-scroll"><table id="snapshots"></table></div>
        </div>
        <div class="panel">
          <h2>Arbitrage Events</h2>
          <div id="arbSummary"></div>
          <div class="table-scroll" style="margin-top:12px"><table id="arbEvents"></table></div>
        </div>
      </div>
    </section>
  </main>
  <script>
    let state = null;
    const fmt = (n, d=4) => Number(n || 0).toFixed(d);
    const cls = n => Number(n || 0) >= 0 ? 'pos' : 'neg';
    const money = n => `<span class="${cls(n)}">${Number(n || 0) >= 0 ? '+' : ''}${fmt(n)}</span>`;
    const pct = n => `${Number(n || 0).toFixed(1)}%`;
    function setTab(name) {
      document.querySelectorAll('main > section').forEach(el => el.classList.toggle('hidden', el.id !== name));
      document.querySelectorAll('.tab').forEach(el => el.classList.toggle('active', el.dataset.tab === name));
    }
    document.querySelectorAll('.tab').forEach(btn => btn.addEventListener('click', () => setTab(btn.dataset.tab)));
    function table(id, headers, rows, empty='No data yet') {
      const el = document.getElementById(id);
      if (!rows.length) { el.innerHTML = `<tr><td class="empty">${empty}</td></tr>`; return; }
      el.innerHTML = `<thead><tr>${headers.map(h => `<th>${h[0]}</th>`).join('')}</tr></thead><tbody>` +
        rows.map(row => `<tr>${headers.map(h => `<td>${h[1](row)}</td>`).join('')}</tr>`).join('') + '</tbody>';
    }
    function drawEquity(rows) {
      const svg = document.getElementById('equity');
      const w = svg.clientWidth || 800, h = svg.clientHeight || 260, p = 24;
      if (!rows.length) { svg.innerHTML = `<text x="50%" y="50%" text-anchor="middle" fill="#667078">No closed paper trades yet</text>`; return; }
      const ys = rows.map(r => Number(r.cumulative));
      const minY = Math.min(0, ...ys), maxY = Math.max(0, ...ys);
      const span = Math.max(.0001, maxY - minY);
      const x = i => p + (rows.length === 1 ? 0 : i * (w - p * 2) / (rows.length - 1));
      const y = v => h - p - ((v - minY) / span) * (h - p * 2);
      const pts = rows.map((r, i) => `${x(i)},${y(Number(r.cumulative))}`).join(' ');
      const zero = y(0);
      svg.setAttribute('viewBox', `0 0 ${w} ${h}`);
      svg.innerHTML = `<line x1="${p}" x2="${w-p}" y1="${zero}" y2="${zero}" stroke="#d8ddd4"/>` +
        `<polyline points="${pts}" fill="none" stroke="#245b4f" stroke-width="3" stroke-linejoin="round" stroke-linecap="round"/>` +
        rows.map((r,i) => `<circle cx="${x(i)}" cy="${y(Number(r.cumulative))}" r="4" fill="${Number(r.pnl) >= 0 ? '#16764f' : '#b33a3a'}"><title>${r.market_slug}: ${fmt(r.cumulative)}</title></circle>`).join('');
    }
    function render(data) {
      state = data;
      document.getElementById('generated').textContent = `Updated ${data.generated_at}`;
      const o = data.overall;
      document.getElementById('metrics').innerHTML = [
        ['Trades', o.trades],
        ['Paper PnL', money(o.pnl)],
        ['Win rate', pct(o.win_rate)],
        ['ROI on paper size', pct(o.roi)],
        ['Markets tested', o.markets],
        ['Average PnL', money(o.avg_pnl)],
        ['Best trade', money(o.best)],
        ['Worst trade', money(o.worst)]
      ].map(m => `<div class="panel metric"><div class="label">${m[0]}</div><div class="value">${m[1]}</div></div>`).join('');
      drawEquity(data.equity);
      document.getElementById('health').innerHTML = `
        <p><span class="pill">Hard exits</span> ${data.health.hard_exit_count}</p>
        <p><span class="pill">Stop losses</span> ${data.health.stop_loss_count}</p>
        <p><span class="pill">Largest loss</span> ${money(data.health.largest_loss)}</p>
        <p><span class="pill">Top 3 PnL share</span> ${pct(data.health.top_3_share)}</p>
        <p class="subtle">If most profit comes from a few trades, keep collecting data before trusting the strategy.</p>`;
      table('markets', [['Market', r=>r.market_slug], ['Trades', r=>r.trades], ['PnL', r=>money(r.pnl)], ['Win', r=>pct(r.win_rate)], ['Worst', r=>money(r.worst)]], data.markets);
      table('outcomes', [['Outcome', r=>r.outcome], ['Trades', r=>r.trades], ['PnL', r=>money(r.pnl)], ['Win', r=>pct(r.win_rate)], ['Avg entry', r=>fmt(r.avg_entry,2)], ['Avg exit', r=>fmt(r.avg_exit,2)], ['Risk exits', r=>r.risk_exits]], data.outcomes);
      renderTrades();
      table('snapshots', [['Time', r=>r.time], ['Label', r=>r.label], ['Bid', r=>fmt(r.bid,2)], ['Ask', r=>fmt(r.ask,2)], ['Bid size', r=>fmt(r.bid_size,2)], ['Ask size', r=>fmt(r.ask_size,2)]], data.snapshots);
      document.getElementById('arbSummary').innerHTML = data.arb.length ? data.arb.map(r => `<p><span class="pill">${r.kind}</span> events=${r.events} max=${fmt(r.max_edge)} avg=${fmt(r.avg_edge)}</p>`).join('') : '<p class="empty">No arb events yet.</p>';
      table('arbEvents', [['Time', r=>r.time], ['Market', r=>r.market_slug], ['Kind', r=>r.kind], ['Up/Yes', r=>fmt(r.yes_price,2)], ['Down/No', r=>fmt(r.no_price,2)], ['Edge', r=>money(r.edge)]], data.arb_events);
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
        ['Market', r=>r.market_slug], ['Outcome', r=>r.outcome], ['Entry', r=>r.entry_time], ['Exit', r=>r.exit_time],
        ['Hold', r=>`${r.hold_seconds}s`], ['Entry px', r=>fmt(r.entry_price,2)], ['Exit px', r=>fmt(r.exit_price,2)],
        ['PnL', r=>money(r.pnl)], ['Exit reason', r=>r.exit_reason]
      ], rows);
    }
    document.getElementById('filter').addEventListener('input', renderTrades);
    document.getElementById('resultFilter').addEventListener('change', renderTrades);
    async function refresh() {
      const res = await fetch('/api/dashboard', { cache: 'no-store' });
      render(await res.json());
    }
    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>"""


class DashboardHandler(BaseHTTPRequestHandler):
    db_path: Path = DEFAULT_DB

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/":
            self.send_text(DASHBOARD_HTML, "text/html; charset=utf-8")
            return
        if parsed.path == "/api/dashboard":
            payload = json.dumps(dashboard_payload(self.db_path)).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)
            return
        self.send_error(404)

    def send_text(self, text: str, content_type: str) -> None:
        payload = text.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def serve_dashboard(db_path: Path, host: str, port: int, open_browser: bool) -> None:
    init_db(db_path)
    handler = type("BoundDashboardHandler", (DashboardHandler,), {"db_path": db_path})
    server = ThreadingHTTPServer((host, port), handler)
    url = f"http://{host}:{port}"
    print(f"Dashboard: {url}")
    print("Press Ctrl+C to stop.")
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
    finally:
        server.server_close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only Polymarket realtime paper tester")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="SQLite database path")
    sub = parser.add_subparsers(dest="cmd", required=True)

    auto = sub.add_parser("watch-url", help="Auto paper-test all built-in strategies for a Polymarket URL/slug")
    auto.add_argument("url_or_slug", help="Polymarket event URL or slug")
    auto.add_argument("--seconds", type=int, help="Optional runtime override. Default: run until market end + buffer")
    auto.add_argument("--poll", type=int, default=5)
    auto.add_argument("--size-usd", type=float, default=1.0)
    auto.add_argument("--target-price", type=float, help="Optional fixed BTC target filter")
    auto.add_argument("--min-distance-usd", type=float, default=50.0)
    auto.add_argument("--arb-buffer", type=float, default=0.01)
    auto.add_argument("--end-buffer-seconds", type=int, default=1, help="Stop this many seconds after market end")
    auto.add_argument("--chain-count", type=int, default=1, help="Run this many consecutive timestamped markets")
    auto.add_argument("--chain-interval-seconds", type=int, default=300, help="Seconds to add to the slug timestamp for each next market")
    auto.add_argument("--lookup-timeout-seconds", type=int, default=60, help="How long to wait for a next market slug to appear")

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
    directional.add_argument("--min-distance-usd", type=float, default=50.0)
    directional.add_argument("--poll", type=int, default=5)
    directional.add_argument("--seconds", type=int, default=900)

    arb = sub.add_parser("watch-arb", help="Watch YES+NO complete-set pricing")
    arb.add_argument("--yes-token-id", required=True)
    arb.add_argument("--no-token-id", required=True)
    arb.add_argument("--label", required=True)
    arb.add_argument("--buffer", type=float, default=0.01, help="Fee/slippage safety buffer")
    arb.add_argument("--poll", type=int, default=5)
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
                    target_price=args.target_price,
                    min_distance_usd=args.min_distance_usd,
                    arb_buffer=args.arb_buffer,
                    end_buffer_seconds=args.end_buffer_seconds,
                    lookup_timeout_seconds=args.lookup_timeout_seconds,
                )
            else:
                watch_url(
                    url_or_slug=args.url_or_slug,
                    db_path=args.db,
                    seconds=args.seconds,
                    poll=args.poll,
                    size_usd=args.size_usd,
                    target_price=args.target_price,
                    min_distance_usd=args.min_distance_usd,
                    arb_buffer=args.arb_buffer,
                    end_buffer_seconds=args.end_buffer_seconds,
                    lookup_timeout_seconds=args.lookup_timeout_seconds,
                )
        return 0
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
