use super::{mpsc, oneshot};
use crate::engine::state::counters::Counters;
use log::{info, warn};

pub enum CounterCommand {
    Increment {
        symbol: String,
        value: u8,
    },
    GetAccuracy {
        symbol: String,
        respond_to: oneshot::Sender<Option<f64>>,
    },
    GetShiftedAccuracy {
        symbol: String,
        window: usize,
        respond_to: oneshot::Sender<Option<f64>>,
    },
    GetTotalAccuracy {
        respond_to: oneshot::Sender<f64>,
    },
    GetTotalShiftedAccuracy {
        window: usize,
        respond_to: oneshot::Sender<Option<f64>>,
    },
}

pub struct CounterActor {
    counters: Counters,
    inbox: mpsc::Receiver<CounterCommand>,
}

impl CounterActor {
    pub fn new(capacity: usize) -> (Self, mpsc::Sender<CounterCommand>) {
        let (tx, rx) = mpsc::channel(10);
        (
            Self {
                counters: Counters::new(capacity),
                inbox: rx,
            },
            tx,
        )
    }

    pub async fn run(mut self) {
        info!("CounterActor has started!");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                CounterCommand::Increment { symbol, value } => {
                    let counter = &mut self.counters;
                    counter.get_mut(&symbol.to_uppercase()).push(value);
                }

                CounterCommand::GetAccuracy { symbol, respond_to } => {
                    let acc = self
                        .counters
                        .get_option(&symbol.to_uppercase())
                        .map(|c| c.get_accuracy());
                    let _ = respond_to.send(acc);
                }

                CounterCommand::GetShiftedAccuracy {
                    symbol,
                    window,
                    respond_to,
                } => {
                    let acc = self
                        .counters
                        .get_option(&symbol.to_uppercase())
                        .and_then(|c| c.get_shifted_accuracy(window));
                    let _ = respond_to.send(acc);
                }

                CounterCommand::GetTotalAccuracy { respond_to } => {
                    let values = self.counters.symbols.values();
                    let acc = calculate_average_accuracy(values);
                    let _ = respond_to.send(acc);
                }

                CounterCommand::GetTotalShiftedAccuracy { window, respond_to } => {
                    let values = self.counters.symbols.values();
                    let acc = calculate_average_shifted_accuracy(values, window);
                    let _ = respond_to.send(Some(acc));
                }
            }
        }

        warn!("CounterActor has stopped!");
    }
}

fn calculate_average_accuracy<'a>(
    values: impl Iterator<Item = &'a crate::engine::state::counters::SymbolCounters<u8>>,
) -> f64 {
    let values: Vec<_> = values.collect();
    let count = values.len();

    if count == 0 {
        0.0
    } else {
        values.iter().map(|c| c.get_accuracy()).sum::<f64>() / count as f64
    }
}

fn calculate_average_shifted_accuracy<'a>(
    values: impl Iterator<Item = &'a crate::engine::state::counters::SymbolCounters<u8>>,
    window: usize,
) -> f64 {
    let values: Vec<_> = values.collect();
    let count = values.len();

    if count == 0 {
        0.0
    } else {
        values
            .iter()
            .map(|c| c.get_shifted_accuracy(window).unwrap_or(0.0))
            .sum::<f64>()
            / count as f64
    }
}
