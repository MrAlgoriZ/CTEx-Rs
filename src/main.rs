mod backend;
mod data;
mod engine;
mod models;
use crate::{
    backend::app::Api,
    engine::{
        cycles::manager::{CycleManager, CycleType},
        utils::config::load_config::{ensure_config_exists, load_config},
    },
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

    let mut manager = CycleManager::new().await;

    manager.run_all(symbols, cycle_types).await.unwrap();

    let counter_handle = manager.counter_handle();
    let supervisor_handle = manager.supervisor_handle();

    let api = Api::new(supervisor_handle, counter_handle).await;
    let api_task = tokio::spawn(async move {
        api.run().await;
    });

    tokio::select! {
        _ = api_task => println!("API завершилось!"),
        _ = tokio::signal::ctrl_c() => {
            println!("Получен сигнал завершения!");
        }
    }
}
