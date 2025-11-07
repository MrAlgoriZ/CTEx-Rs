mod data;
mod engine;
mod models;
use crate::engine::{
    cycles::manager::{CycleManager, CycleType},
    utils::config::load_config::{ensure_config_exists, load_config},
};

use std::collections::HashMap;

const CONFIG_PATH: &'static str = "config/config.yaml";

#[tokio::main]
async fn main() {
    ensure_config_exists(CONFIG_PATH);
    let config = load_config(CONFIG_PATH);
    let symbols = config.token;

    let mut cycle_types = HashMap::new();
    for symbol in symbols.clone().into_iter() {
        cycle_types.insert(
            symbol,
            CycleType::from_str(&config.cycle_type.to_lowercase()),
        );
    }

    let manager = CycleManager::new(symbols).with_cycle_types(cycle_types);
    manager.run_all().await;
}
