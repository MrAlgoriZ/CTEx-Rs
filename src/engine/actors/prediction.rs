use super::{mpsc, oneshot};
use crate::engine::state::counters::SymbolCounters;

use anyhow::Result;
use log::info;
use std::collections::HashMap;

pub enum PredictionsCommand {
    AddPrediction {
        symbol: String,
        prediction: f64,
        respond_to: oneshot::Sender<Result<()>>,
    },
    ListPredictions {
        respond_to: oneshot::Sender<Option<HashMap<String, SymbolCounters<f64>>>>,
    },
    GetLastPrediction {
        symbol: String,
        respond_to: oneshot::Sender<Option<f64>>,
    },
    GetPredictions {
        symbol: String,
        respond_to: oneshot::Sender<Option<SymbolCounters<f64>>>,
    },
}

pub struct PredictionsActor {
    capacity: usize,
    predictions: HashMap<String, SymbolCounters<f64>>,
    inbox: mpsc::Receiver<PredictionsCommand>,
}

impl PredictionsActor {
    pub fn new(capacity: usize) -> (Self, mpsc::Sender<PredictionsCommand>) {
        let (tx, rx) = mpsc::channel(10);

        (
            Self {
                capacity,
                predictions: HashMap::new(),
                inbox: rx,
            },
            tx,
        )
    }

    pub async fn run(mut self) {
        info!("PredictionsActor has started!");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                PredictionsCommand::AddPrediction {
                    symbol,
                    prediction,
                    respond_to,
                } => {
                    let pred_counter = self
                        .predictions
                        .entry(symbol)
                        .or_insert_with(|| SymbolCounters::new(self.capacity));
                    pred_counter.push(prediction);
                    let _ = respond_to.send(Ok(()));
                }
                PredictionsCommand::GetLastPrediction { symbol, respond_to } => {
                    let pred_counter = self.predictions.get(&symbol);
                    if let Some(counter) = pred_counter {
                        let _ = respond_to.send(counter.data.back().cloned());
                    } else {
                        let _ = respond_to.send(None);
                    }
                }
                PredictionsCommand::GetPredictions { symbol, respond_to } => {
                    let pred_counter = self.predictions.get(&symbol);
                    let _ = respond_to.send(pred_counter.cloned());
                }
                PredictionsCommand::ListPredictions { respond_to } => {
                    let _ = respond_to.send(Some(self.predictions.clone()));
                }
            }
        }
    }
}
