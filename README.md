# Polymarket Paper Bot

Read-only realtime paper tester for Polymarket strategies.

This tool does not use a wallet, private key, API key, or real orders. It polls public Polymarket order books and BTC spot price, then simulates trades with local rules.

## What It Tests

- Directional BTC up/down scalping:
  - enter when the ask is around `0.65-0.72`
  - exit on take profit, stop loss, hard stop, or late ambiguous BTC/target state
- Complete-set arbitrage scanner:
  - logs when `YES ask + NO ask + buffer < 1`
  - logs when `YES bid + NO bid - buffer > 1`

## Quick Start

Paste a Polymarket event URL and let the tool find the outcome token IDs automatically:

```powershell
python paper_bot.py watch-url `
  "https://polymarket.com/event/btc-updown-5m-1779549600" `
  --poll 5
```

This runs:

- band scalping on every outcome, such as `Up` and `Down`
- complete-set arbitrage scanning on the first two outcomes
- automatic stop at the market end time plus 1 second
- an automatic report for that market after the watcher stops

If you know a fixed BTC target price and want the late-distance filter, add:

```powershell
--target-price 75000 --min-distance-usd 50
```

For BTC up/down 5-minute markets, the official resolution compares Chainlink's end price with the beginning price, so the target is the window's opening price. If you do not provide `--target-price`, the tool still paper-tests from share price/orderbook rules but skips the BTC-distance filter.

You can override the runtime if needed:

```powershell
--seconds 60
```

Run several BTC 5-minute markets in a row:

```powershell
python paper_bot.py watch-url `
  "https://polymarket.com/event/btc-updown-5m-1779550500" `
  --chain-count 3 `
  --poll 5
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
  --poll 5
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
  --seconds 900 `
  --poll 5
```

## Notes

- `watch-directional` assumes taker-style paper fills: enter at best ask, exit at best bid.
- This is conservative for market-taking and still imperfect during fast moves.
- It does not know whether a maker order would really fill. For that, collect WebSocket orderbook/trade streams later.
- The default database is `data/paper.sqlite`.
