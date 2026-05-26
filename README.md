# Polymarket Paper Bot

Read-only realtime paper tester for Polymarket strategies.

This tool does not use a wallet, private key, API key, or real orders. It streams public Polymarket CLOB order books and Polymarket RTDS reference prices, then simulates trades with local rules.

## What It Tests

- Directional BTC up/down scalping:
  - enter when the ask is around `0.65-0.68` by default
  - for BTC 5-minute `watch-url`, auto-read the market target from Polymarket `eventMetadata.priceToBeat`
  - risk guards default to one trade per label, market lock after a loss, no entries after 180 seconds, and `ask + opposite ask <= 1.03`
  - exit on take profit, trailing stop, stop loss, or late ambiguous BTC/target state
- Complete-set arbitrage scanner:
  - logs when `YES ask + NO ask + taker fees + buffer < 1`
  - logs when `YES bid + NO bid - taker fees - buffer > 1`
  - taker fees follow Polymarket's formula: `shares * fee_rate * price * (1 - price)`
  - the bot queries `https://clob.polymarket.com/fee-rate?token_id=...` and falls back to `--fee-rate`

## Quick Start

Paste a Polymarket event URL and let the tool find the outcome token IDs automatically:

```powershell
python paper_bot.py watch-url `
  "https://polymarket.com/event/btc-updown-5m-1779549600" `
  --poll 2
```

This runs:

- band scalping on every outcome, such as `Up` and `Down`
- complete-set arbitrage scanning on the first two outcomes
- realtime UP/DOWN bid/ask from Polymarket CLOB WebSocket
- current BTC price from Polymarket RTDS `crypto_prices_chainlink`
- automatic stop at the market end time plus 1 second
- an automatic report for that market after the watcher stops

When started from the dashboard, each run gets a fresh SQLite file under `data/runs/`. The filename includes the UTC start time, market slug, and core strategy settings, and the same metadata is also stored inside the DB in `run_metadata`.

For BTC 5-minute `watch-url`, the bot reads the target price automatically, so you only provide the minimum distance:

```powershell
--min-distance-usd 25
```

The first source is Gamma `eventMetadata.priceToBeat`. If that field is missing while the Polymarket page already shows the value, the bot falls back to Polymarket `/api/past-results` and then the event page's hydrated price data.
If all sources are unavailable, the bot waits during lookup and then fails closed instead of silently trading without the distance filter. Use `--min-distance-usd 0` only if you intentionally want to disable the BTC-distance filter.

The optimized dashboard presets are:

```powershell
Safe:       entry 0.65-0.68, TP 0.84, SL 0.60, trail 0.05/0.04, BTC dist 100
Balanced:   entry 0.65-0.70, TP 0.86, SL 0.58, trail 0.05/0.05, BTC dist 75
Aggressive: entry 0.65-0.72, TP 0.88, SL 0.56, trail 0.08/0.06, BTC dist 75
```

Set `trail_start` and `trail_distance` to `0` to disable trailing. If either value is `0`, the bot normalizes both to `0` because a one-sided trailing config cannot arm a valid trailing stop.
Set `take_profit` to `0` to disable the fixed TP exit and let trailing, stop loss, and late exit manage the position.

Risk guard defaults:

```powershell
--max-trades-per-label-per-market 1 --market-lock-after-loss --no-entry-after-seconds 0 --max-sum-asks 1.03 --entry-window-seconds 120
```

Optionally limit entries to the final part of a market:

```powershell
--entry-window-seconds 120
```

Use `--entry-window-seconds 0` to disable the remaining-time gate. The default is 120 seconds, and `--no-entry-after-seconds` defaults to 0 so the late-window filter can operate without an elapsed-time cutoff conflict.

You can override the runtime if needed:

```powershell
--seconds 60
```

Run several BTC 5-minute markets in a row:

```powershell
python paper_bot.py watch-url `
  "https://polymarket.com/event/btc-updown-5m-1779550500" `
  --chain-count 3 `
  --poll 2
```

For timestamped BTC 5-minute slugs, the tool adds `300` seconds to the slug for each next market. It waits for each market to end plus 1 second, prints that market's report, moves to the next one, and prints an aggregate summary after the chain finishes.

## Manual Token Mode

```powershell
python paper_bot.py watch-directional `
  --token-id YOUR_OUTCOME_TOKEN_ID `
  --label "BTC 5m UP" `
  --target-price 75000 `
  --direction UP `
  --seconds 900 `
  --poll 2
```

Generate a report:

```powershell
python paper_bot.py report
```

Open the local web dashboard:

```powershell
python paper_bot.py dashboard
```

Or print the report and then open the dashboard:

```powershell
python paper_bot.py report --web
```

The dashboard opens at `http://127.0.0.1:8765` by default and refreshes from the local SQLite database every 2 seconds.

Aggregate performance after many runs:

```powershell
python paper_bot.py summary
```

Export all paper trades to CSV:

```powershell
python paper_bot.py summary `
  --export-csv reports\trades.csv
```

Compact old SQLite run files after long paper sessions:

```powershell
python paper_bot.py compact-db --include-runs
```

New runs store compact top-of-book snapshots by default instead of full raw order books. Snapshot rows are written every 2 seconds per label/token by default, while trade entries, exits, arbitrage events, and errors are still recorded immediately.
Set `POLYMARKET_SNAPSHOT_INTERVAL_SECONDS=10` to write fewer analysis snapshots, `POLYMARKET_SNAPSHOT_INTERVAL_SECONDS=0` to record every loop, or `POLYMARKET_STORE_RAW_SNAPSHOT_JSON=1` if you intentionally need full raw orderbook JSON.
The dashboard does not wait for those historical snapshots for live values: the bot also UPSERTs a bounded `latest_state` row per token, and the UI polls `/api/realtime` every 500 ms for current price, countdown, and latest bid/ask.

Realtime safeguards:

- the CLOB market WebSocket sends `PING` every 10 seconds and reconnects with backoff
- RTDS sends `PING` every 5 seconds and rejects stale current prices
- price-to-beat lookup starts 5 seconds after the market start; if the target is still missing, the bot retries every 3 seconds up to 10 times and then stops
- when chaining markets, a target that exactly matches the previous market target is treated as stale and retried instead of being shown as the new price to beat
- BTC up/down 5m markets use the timestamp in the slug as the trading window, so the old market watcher ends at `slug_ts + 300s` instead of waiting on stale metadata
- if the CLOB book stream closes after the market end, the bot treats it as normal market completion and moves to the next chained market
- strategy entries fail closed when UP/DOWN book data, current price, or Polymarket clock sync is stale
- REST `/book` is only used as a one-time bootstrap if the first WebSocket snapshot is delayed

The summary shows:

- total trades, total paper PnL, average PnL, win rate, and ROI on paper size
- performance by market
- performance by outcome, such as Up vs Down
- performance by exit reason
- complete-set arbitrage event stats

Scan complete-set opportunities:

```powershell
python paper_bot.py watch-arb `
  --yes-token-id YES_TOKEN_ID `
  --no-token-id NO_TOKEN_ID `
  --label "BTC 5m complete set" `
  --fee-rate 0.07 `
  --seconds 900 `
  --poll 2
```

The bot queries Polymarket's CLOB `/fee-rate` endpoint for the active token and uses `--fee-rate` only as a fallback.
`--fee-rate` defaults to `0.07`, matching Polymarket's Crypto taker fee rate at the time this bot was updated.
Use the current Polymarket fee docs for other categories, for example `0.03` for Sports, `0.04` for Finance/Politics/Tech, `0.05` for Other-style categories, or `0` for fee-free markets.
`--arb-buffer` / `--buffer` is now only an extra safety margin for slippage after protocol taker fees are included.

## Notes

- `watch-directional` assumes taker-style paper fills: enter at best ask, exit at best bid.
- This is conservative for market-taking and still imperfect during fast moves.
- Maker fill simulation is still approximate, but the bot now has WebSocket orderbook/trade updates for later analysis.
- Dashboard runs write separate databases under `data/runs/`; direct CLI commands still default to `data/paper.sqlite` unless `--db` is provided.
