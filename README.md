HFT Execution Algorithm (Bybit Testnet)
=======================================

This project implements an HFT-style execution algorithm using traditional
market microstructure signals (no ML). It connects to Bybit Testnet for
spot, perpetual futures, and options and attempts to achieve negative
implementation shortfall while maintaining high fill rates.

Key Signals and Controls
------------------------
- Order book imbalance (top-of-book depth ratio)
- Spread dynamics (widening/tightening)
- Queue position estimation and cancel-on-stale logic
- Trade flow imbalance and short-term momentum
- Large resting order persistence (iceberg detection)
- Toxic flow detection (informed flow + momentum alignment)
- Passive vs aggressive switching with urgency control

Project Structure
-----------------
- src/market_data.rs: WebSocket market data ingestion
- src/execution.rs: REST execution + private WS fills
- src/strategy.rs: Microstructure decision logic
- src/performance.rs: Shortfall and VWAP tracking
- src/config.rs: Environment-based configuration

Setup
-----
1. Create a Bybit Testnet account and generate API keys.
2. Fund the account with testnet assets.
3. Export environment variables:

```
export BYBIT_API_KEY="..."
export BYBIT_API_SECRET="..."
```

Optional configuration (defaults shown):

```
export BYBIT_BASE_URL="https://api-testnet.bybit.com"
export BYBIT_WS_PUBLIC="wss://stream-testnet.bybit.com/v5/public"
export BYBIT_WS_PRIVATE="wss://stream-testnet.bybit.com/v5/private"
export DECISION_INTERVAL_MS="200"

export SPOT_SYMBOL="BTCUSDT"
export SPOT_SIDE="Buy"
export SPOT_QTY="0.01"

export PERP_SYMBOL="BTCUSDT"
export PERP_SIDE="Buy"
export PERP_QTY="0.01"

export OPTION_SYMBOL="BTC-30JUN25-50000-C"
export OPTION_SIDE="Buy"
export OPTION_QTY="0.01"
```

Run
---
```
cargo run
```

Notes
-----
- The algorithm places one child order per instrument at a time, sizing the
  child order relative to visible liquidity.
- Aggressive IOC orders are only sent when urgency is high or adverse
  selection risk increases.
- Performance logs include execution VWAP, market VWAP, and average shortfall.
