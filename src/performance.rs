use crate::config::InstrumentConfig;
use crate::types::{ExecutionFill, InstrumentType, Side, Trade};
use crate::utils::now_millis;
use std::collections::HashMap;

#[derive(Clone, Debug)]
struct DecisionRecord {
    instrument: InstrumentType,
    symbol: String,
    side: Side,
    decision_price: f64,
    remaining_qty: f64,
    timestamp: u64,
}

#[derive(Clone, Debug, Default)]
struct InstrumentPerformance {
    filled_qty: f64,
    our_notional: f64,
    shortfall_notional: f64,
    market_notional: f64,
    market_volume: f64,
    last_report_ts: u64,
}

pub struct PerformanceTracker {
    decisions: HashMap<String, DecisionRecord>,
    performance: HashMap<InstrumentType, InstrumentPerformance>,
    report_interval_ms: u64,
}

impl PerformanceTracker {
    pub fn new(instruments: &[InstrumentConfig]) -> Self {
        let mut performance = HashMap::new();
        for instrument in instruments {
            performance.insert(instrument.instrument_type, InstrumentPerformance::default());
        }
        Self {
            decisions: HashMap::new(),
            performance,
            report_interval_ms: 5_000,
        }
    }

    pub fn record_decision(
        &mut self,
        client_id: String,
        instrument: InstrumentType,
        symbol: String,
        side: Side,
        decision_price: f64,
        qty: f64,
    ) {
        let record = DecisionRecord {
            instrument,
            symbol,
            side,
            decision_price,
            remaining_qty: qty,
            timestamp: now_millis(),
        };
        self.decisions.insert(client_id, record);
    }

    pub fn record_fill(&mut self, fill: ExecutionFill) {
        let Some(perf) = self.performance.get_mut(&fill.instrument) else {
            return;
        };
        let decision_price = fill
            .client_id
            .as_ref()
            .and_then(|client_id| self.decisions.get(client_id))
            .map(|record| record.decision_price)
            .unwrap_or(fill.price);

        let side_sign = fill.side.sign();
        let shortfall = (fill.price - decision_price) * side_sign * fill.qty;
        perf.shortfall_notional += shortfall;
        perf.filled_qty += fill.qty;
        perf.our_notional += fill.price * fill.qty;

        if let Some(client_id) = fill.client_id.as_ref() {
            if let Some(record) = self.decisions.get_mut(client_id) {
                record.remaining_qty -= fill.qty;
                if record.remaining_qty <= 0.0 {
                    self.decisions.remove(client_id);
                }
            }
        }

        tracing::info!(
            instrument = ?fill.instrument,
            symbol = %fill.symbol,
            order_id = %fill.order_id,
            price = fill.price,
            qty = fill.qty,
            shortfall = shortfall,
            "execution fill"
        );
    }

    pub fn record_market_trade(&mut self, instrument: InstrumentType, trade: &Trade) {
        if let Some(perf) = self.performance.get_mut(&instrument) {
            perf.market_notional += trade.price * trade.qty;
            perf.market_volume += trade.qty;
        }
    }

    pub fn maybe_report(&mut self) {
        let now = now_millis();
        for (instrument, perf) in self.performance.iter_mut() {
            if perf.last_report_ts != 0 && now - perf.last_report_ts < self.report_interval_ms {
                continue;
            }
            perf.last_report_ts = now;

            let execution_vwap = if perf.filled_qty > 0.0 {
                perf.our_notional / perf.filled_qty
            } else {
                0.0
            };
            let market_vwap = if perf.market_volume > 0.0 {
                perf.market_notional / perf.market_volume
            } else {
                0.0
            };
            let avg_shortfall = if perf.filled_qty > 0.0 {
                perf.shortfall_notional / perf.filled_qty
            } else {
                0.0
            };

            tracing::info!(
                instrument = ?instrument,
                execution_vwap = execution_vwap,
                market_vwap = market_vwap,
                avg_shortfall = avg_shortfall,
                filled_qty = perf.filled_qty,
                "performance snapshot"
            );
        }
    }
}
