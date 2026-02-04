use std::collections::HashMap;

use crate::types::{ExecutionTarget, Fill, Instrument, Side, Trade};

#[derive(Debug, Clone)]
struct DecisionRecord {
    decision_price: f64,
    side: Side,
}

#[derive(Debug, Clone)]
pub struct InstrumentMetrics {
    pub target_qty: f64,
    pub executed_qty: f64,
    pub executed_notional: f64,
    pub decision_notional: f64,
    pub shortfall_notional: f64,
    pub market_vwap_notional: f64,
    pub market_vwap_qty: f64,
}

impl InstrumentMetrics {
    fn new(target_qty: f64) -> Self {
        Self {
            target_qty,
            executed_qty: 0.0,
            executed_notional: 0.0,
            decision_notional: 0.0,
            shortfall_notional: 0.0,
            market_vwap_notional: 0.0,
            market_vwap_qty: 0.0,
        }
    }

    pub fn execution_vwap(&self) -> Option<f64> {
        if self.executed_qty > 0.0 {
            Some(self.executed_notional / self.executed_qty)
        } else {
            None
        }
    }

    pub fn market_vwap(&self) -> Option<f64> {
        if self.market_vwap_qty > 0.0 {
            Some(self.market_vwap_notional / self.market_vwap_qty)
        } else {
            None
        }
    }

    pub fn avg_shortfall(&self) -> Option<f64> {
        if self.executed_qty > 0.0 {
            Some(self.shortfall_notional / self.executed_qty)
        } else {
            None
        }
    }

    pub fn fill_rate(&self) -> f64 {
        if self.target_qty > 0.0 {
            self.executed_qty / self.target_qty
        } else {
            0.0
        }
    }
}

pub struct PerformanceTracker {
    metrics: HashMap<String, InstrumentMetrics>,
    decisions: HashMap<String, DecisionRecord>,
}

impl PerformanceTracker {
    pub fn new(targets: &[ExecutionTarget]) -> Self {
        let mut metrics = HashMap::new();
        for target in targets {
            metrics.insert(instrument_key(&target.instrument), InstrumentMetrics::new(target.total_qty));
        }
        Self {
            metrics,
            decisions: HashMap::new(),
        }
    }

    pub fn record_decision(
        &mut self,
        order_id: String,
        instrument: &Instrument,
        decision_price: f64,
        qty: f64,
        side: Side,
    ) {
        let record = DecisionRecord {
            decision_price,
            side,
        };
        self.decisions.insert(order_id, record);
        if let Some(metrics) = self.metrics.get_mut(&instrument_key(instrument)) {
            metrics.decision_notional += decision_price * qty;
        }
    }

    pub fn record_fill(&mut self, instrument: &Instrument, fill: &Fill) {
        let Some(decision) = self.decisions.get(&fill.order_id) else {
            return;
        };
        let shortfall = (fill.price - decision.decision_price) * decision.side.sign();
        if let Some(metrics) = self.metrics.get_mut(&instrument_key(instrument)) {
            metrics.executed_qty += fill.qty;
            metrics.executed_notional += fill.price * fill.qty;
            metrics.shortfall_notional += shortfall * fill.qty;
        }
    }

    pub fn record_trade(&mut self, instrument: &Instrument, trade: &Trade) {
        if let Some(metrics) = self.metrics.get_mut(&instrument_key(instrument)) {
            metrics.market_vwap_notional += trade.price * trade.qty;
            metrics.market_vwap_qty += trade.qty;
        }
    }

    pub fn metrics_summary(&self) -> Vec<(String, InstrumentMetrics)> {
        self.metrics
            .iter()
            .map(|(key, metrics)| (key.clone(), metrics.clone()))
            .collect()
    }
}

fn instrument_key(instrument: &Instrument) -> String {
    format!("{}:{}", instrument.category.as_str(), instrument.symbol)
}
