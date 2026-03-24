# Binance wash-like volume monitor

This project watches Binance spot public market data and highlights persistent two-sided taker flow that can look like wash-like volume.

It is designed for cases like `NIGHTUSDT`, where you want to know whether the market is seeing:

- large taker buying and large taker selling at the same time
- high trade frequency
- limited net price movement
- persistence over the last 5 minutes and 10 minutes

## What it detects

The script uses only Binance public market data:

- `aggTrade`: identifies taker-side aggression
- `bookTicker`: tracks top-of-book churn and refill hints
- `depth@100ms` + REST depth snapshot: maintains a local order book for depth and slippage checks
- `kline_1m`: keeps a rolling baseline of normal 1-minute quote volume and trade count

The detector reports three states per rolling window:

- `WARMING_UP`
- `NORMAL`
- `TWO_SIDED_ACTIVE`
- `SUSPECTED_WASH_LIKE`

The classification is based on:

- quote volume vs. the recent 1-minute baseline
- trade count vs. the recent 1-minute baseline
- buy/sell taker balance
- taker side switch rate
- absolute price move inside the window
- top-of-book churn and simple refill hints
- 0.2% local order-book depth
- time-weighted 0.2% depth sampled once per second
- estimated slippage for a 10,000 quote market sweep
- repeated fixed-size trade concentration
- repeated fixed-notional trade concentration
- regularity of trade arrival intervals

## Important limit

This detects persistent two-sided taker flow, not the account identity behind it.

Binance public market data does not expose the trader identity, and it does not prove that every taker fill came from a literal `MARKET` order. In practice, it is best interpreted as aggressive taker flow, which can include market orders and aggressively priced limit orders.

## Requirements

- Python 3.10+
- Internet access to Binance public REST and WebSocket endpoints

Install dependency:

```bash
python -m pip install -r requirements.txt
```

## UI startup

Double-click:

```text
start_monitor_ui.bat
```

The script installs dependencies, starts the local server, and opens the browser automatically.

If you want a silent launch without the black terminal window, double-click:

```text
start_monitor_ui_hidden.vbs
```

This hidden launcher starts the UI with `pythonw`. Use it after dependencies are already installed.

If the page has already stopped but the folder is still occupied, double-click:

```text
stop_monitor_ui.bat
```

This force-stops the local UI/background Python process for this project.

Default address:

```text
http://127.0.0.1:8765
```

In the UI you can change:

- symbol list, for example `NIGHTUSDT,BTCUSDT,ETHUSDT`
- rolling windows in minutes, for example `5,10,15`
- log refresh interval in seconds
- baseline lookback minutes
- optional auto-stop runtime in seconds

The UI is conclusion-first:

- top banner: one-line overall verdict
- symbol overview row: one-line verdict per symbol
- each window: only three primary questions by default
  - whether two-sided taker flow is amplified
  - whether price is not moving enough for that flow
  - whether the behavior looks mechanical
- secondary metrics stay in a collapsible evidence section

While the monitor is already running, you can append symbols without stopping:

- type additional symbols in the symbol list input
- click `添加币种`
- the UI will keep existing symbols running and only start the missing ones

## CLI run

Default CLI run for `NIGHTUSDT`:

```bash
python monitor.py
```

Common examples:

```bash
python monitor.py --symbol NIGHTUSDT
python monitor.py --symbol NIGHTUSDT --report-interval 5
python monitor.py --symbol NIGHTUSDT --windows 300 600 900
python monitor.py --symbol NIGHTUSDT --runtime-seconds 120
python app.py --no-browser
```

## Output fields

- `WARMING_UP`: the script has not observed a full live window yet, so a sustained 5-minute or 10-minute judgment is not ready
- `quote`: taker quote volume in the window, plus baseline ratio
- `buy` / `sell`: taker buy and taker sell quote volume
- `balance`: `min(buy, sell) / max(buy, sell)`
- `trades`: trade count, plus baseline ratio
- `switch_rate`: how often taker side flips between buy and sell
- `move`: absolute price move inside the window
- `book_churn`: best bid/ask update rate
- `refill`: simple same-price top-of-book refill hints per minute
- `vol/depth`: window taker quote volume divided by current 0.2% one-sided minimum depth
- `vol/tw_depth`: window taker quote volume divided by median sampled 0.2% one-sided depth
- `tw_slip`: median sampled 10k sweep slippage inside the window
- `gap_cv`: coefficient of variation for inter-trade time gaps; lower often means more mechanical flow
- `size_top1`: share of trades using the single most common trade size
- `size_top5`: share of trades using the five most common trade sizes
- `notional_top1`: share of trades falling into the most common notional bucket
- `streak`: how long the current non-normal state has lasted

The UI summary also shows:

- current 0.2% bid depth and ask depth
- estimated slippage for a 10k buy sweep and 10k sell sweep

## Threshold tuning

Defaults are intentionally conservative:

- `--active-volume-ratio 1.40`
- `--active-trade-ratio 1.15`
- `--active-balance 0.68`
- `--active-switch-rate 0.30`
- `--suspicious-volume-ratio 2.20`
- `--suspicious-trade-ratio 1.60`
- `--suspicious-balance 0.82`
- `--suspicious-switch-rate 0.45`
- `--suspicious-max-return 0.0040`

For a noisier or more liquid symbol, raise the suspicious ratios. For a thin symbol, lower them slightly and watch the false positive rate.
