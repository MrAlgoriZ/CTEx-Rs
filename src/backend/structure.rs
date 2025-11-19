use crate::{
    data::requests::ccxt::binance::BinanceClient,
    engine::cycles::manager::{CounterCommand, SupervisorCommand},
};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct ApiState {
    pub supervisor_handle: mpsc::Sender<SupervisorCommand>,
    pub counter_handle: mpsc::Sender<CounterCommand>,
    pub client: Arc<BinanceClient>,
}

#[derive(Debug, Serialize)]
pub struct ApiStructure {
    pub root: String,
    pub health: String,

    pub cycles_list: String,
    pub cycle_add: String,
    pub cycle_stop: String,
    pub cycles_stop_all: String,

    pub accuracy_total: String,
    pub accuracy_token: String,
    pub accuracy_all_tokens: String,
}

impl Default for ApiStructure {
    fn default() -> Self {
        Self {
            root: "/".to_string(),
            health: "/health".to_string(),

            cycles_list: "/cycles".to_string(),
            cycle_add: "/cycles".to_string(),
            cycle_stop: "/cycles/{symbol}".to_string(),
            cycles_stop_all: "/cycles".to_string(),

            accuracy_total: "/accuracy/total".to_string(),
            accuracy_token: "/accuracy/{symbol}".to_string(),
            accuracy_all_tokens: "/accuracy".to_string(),
        }
    }
}
