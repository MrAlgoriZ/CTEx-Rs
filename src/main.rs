mod data;
mod engine;
mod models;
use crate::engine::{
    cycles::manager::{CycleManager, CycleType},
    utils::config::load_config::load_config,
};

use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let symbols = load_config("config/config.yaml").token;

    let mut cycle_types = HashMap::new();
    for symbol in symbols.clone().into_iter() {
        cycle_types.insert(symbol, CycleType::Trading);
    }

    let manager = CycleManager::new(symbols).with_cycle_types(cycle_types);
    manager.run_all().await;
}
