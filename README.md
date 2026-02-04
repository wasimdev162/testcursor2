
# HFT-Style Execution Algorithm (Bybit Testnet)

This repository contains a Rust-based, HFT-style execution engine that targets
**negative implementation shortfall** (average execution price better than the
decision price at order initiation) using **pure microstructure signals**—no
machine learning or AI-based trading logic.

## Highlights

- **Three instrument types** supported: Spot, Perpetual Futures (Linear), Options.
- **Microstructure-driven decisions**: order book imbalance, trade flow
  imbalance, spread dynamics, queue position, iceberg detection.
- **Adverse selection controls**: toxic flow detection, cancel stale orders,
  pull back on unfavorable book dynamics.
- **Execution tactics**: post-only passive orders for maker rebates when
  favorable, smart crossing when urgency or adverse selection risk rises.
- **Real-time metrics**: implementation shortfall, fill rate, execution VWAP,
  market VWAP.

---

## Quick Start

### 1) Set environment variables

```bash
export BYBIT_API_KEY="your_testnet_key"
export BYBIT_API_SECRET="your_testnet_secret"
```

### 2) Run

```bash
cargo run -- \
  --spot-symbol BTCUSDT \
  --perp-symbol BTCUSDT \
  --option-symbol BTC-30JUN25-50000-C \
  --spot-qty 0.001 \
  --perp-qty 0.001 \
  --option-qty 1 \
  --side buy
```

> **Note:** The default endpoints point to **Bybit Testnet**.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│ Market Data Handler (WS)                                       │
│   - orderbook.{depth}.{symbol}                                 │
│   - publicTrade.{symbol}                                       │
└──────────────┬─────────────────────────────────────────────────┘
               │ events
               ▼
┌────────────────────────────────────────────────────────────────┐
│ Decision Engine                                                 │
│   - microstructure signals                                      │
│   - adverse selection guard                                     │
│   - passive/aggressive mix                                      │
│   - child sizing & queue position                               │
└──────────────┬─────────────────────────────────────────────────┘
               │ order actions
               ▼
┌────────────────────────────────────────────────────────────────┐
│ Execution Engine (REST)                                         │
│   - signed order placement/cancel                               │
│   - fill polling (execution list)                               │
└──────────────┬─────────────────────────────────────────────────┘
               │ fills
               ▼
┌────────────────────────────────────────────────────────────────┐
│ Performance Tracker                                             │
│   - implementation shortfall                                    │
│   - execution VWAP vs market VWAP                               │
│   - fill rate                                                   │
└────────────────────────────────────────────────────────────────┘
```

---

## Core Microstructure Signals (Implemented)

1. **Order Book Imbalance**  
   Ratio of bid vs ask liquidity at the top of the book. Skews alpha up/down.

2. **Queue Position Estimation**  
   Records queue ahead at placement and updates as displayed size changes.

3. **Iceberg / Large Resting Orders**  
   Counts repeated size replenishment at top-of-book. High score reduces
   aggressiveness.

4. **Spread Dynamics**  
   Rolling spread mean/std → z-score. Wide spreads reduce crossing behavior.

5. **Trade Flow Imbalance**  
   Net aggressive volume (buy minus sell) within a short window.

6. **Momentum & Mean Reversion**  
   Short-term price change + large-trade reversal heuristic.

7. **Quote Stuffing Detection** (bonus)  
   Elevated update rate + widening spreads → hold off unless urgent.

---

## Execution Logic (How Negative Shortfall Is Targeted)

**For buys**:
1. If signals predict **near-term price drop**, place **post-only** on bid to
   capture price improvement.
2. If signals predict **upward pressure** or toxicity rises, **cross the spread**
   to avoid later adverse selection.

**For sells**:
1. If signals predict **near-term price rise**, post-only on ask to improve.
2. If signals predict **downward pressure**, cross aggressively.

## End-to-End Flow (Implementation Walkthrough)

1. **Subscribe** to Bybit Testnet order books and trades for spot, perp, and
   options via WebSocket.
2. **Normalize updates** into a shared event stream.
3. **Compute microstructure signals** on each tick:
   - imbalance, spread z-score, trade flow, momentum, mean reversion
   - iceberg score, queue position, quote stuffing risk
4. **Decide passive vs. aggressive**:
   - passive when signal expects price improvement
   - aggressive when signal predicts adverse move or urgency is high
5. **Size child orders** relative to visible liquidity and volatility.
6. **Place/cancel orders** through signed REST requests.
7. **Track performance** continuously:
   - implementation shortfall
   - execution VWAP vs market VWAP
   - fill rate per instrument

---

## Performance Metrics (Live)

Each instrument prints:

- **fill_rate**: executed quantity ÷ target quantity  
- **exec_vwap**: volume-weighted average execution price  
- **market_vwap**: market VWAP during the execution window  
- **shortfall**: signed implementation shortfall (negative is better)  

---

## Configuration Flags (CLI)

Key parameters (see `--help`):

- `--horizon-ms`: execution horizon
- `--max-participation`: liquidity participation cap
- `--order-stale-ms`: cancel stale orders
- `--min-alpha`: minimum signal strength to cross
- `--toxicity-threshold`: aggressive/cancel trigger
- `--decision-interval-ms`: decision loop frequency

---

## Notes / Limitations

- The execution engine uses Bybit **REST** for orders and **WS** for market data.
- Private WS is not required; fills are polled from `/v5/execution/list`.
- This is a testnet-focused reference implementation for microstructure logic.
