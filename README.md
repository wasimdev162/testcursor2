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
3. Create a `.env` file or export environment variables.

Example `.env`:

```
BYBIT_API_KEY=...
BYBIT_API_SECRET=...
```

Shell export alternative:

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

# Strategy tuning (examples)
export STRAT_IMBALANCE_WEIGHT="0.45"
export STRAT_FLOW_WEIGHT="0.25"
export STRAT_MOMENTUM_WEIGHT="0.15"
export STRAT_MEAN_REV_WEIGHT="0.10"
export STRAT_ICEBERG_WEIGHT="0.05"
export STRAT_ICEBERG_BIAS="0.60"
export STRAT_PASSIVE_ALPHA="0.05"
export STRAT_AGGRESSIVE_ALPHA="-0.08"
export STRAT_CANCEL_ALPHA="-0.10"
export STRAT_TOXICITY_CANCEL="0.35"
export STRAT_TOXICITY_PASSIVE="0.20"
export STRAT_TOXICITY_FLOW="0.15"
export STRAT_SPREAD_WIDEN_CANCEL="0.50"
export STRAT_SPREAD_WIDEN_PASSIVE="0.60"
export STRAT_MIN_DECISION_MS="150"
export STRAT_MEAN_REVERSION_MS="1200"
export STRAT_LARGE_TRADE_MULT="3.0"
export STRAT_ICEBERG_MULT="3.0"
export STRAT_ICEBERG_PERSIST="3"
export STRAT_ICEBERG_STABLE_PCT="0.05"
export STRAT_URGENCY_AGGR="0.70"
export STRAT_URGENCY_FORCE_AGGR="0.85"
export STRAT_FILL_PROB_AGGR="0.20"
export STRAT_SPREAD_MOVE_CANCEL="0.10"
export STRAT_IMBALANCE_DEPTH="5"
export STRAT_LIQUIDITY_DEPTH="3"
export STRAT_ICEBERG_DEPTH="3"
export STRAT_SPREAD_WINDOW="40"
export STRAT_MID_WINDOW="40"
export STRAT_TRADE_FLOW_WINDOW="50"
export STRAT_TRADE_QTY_WINDOW="50"
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
