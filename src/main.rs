mod backend;
mod data;
mod engine;
mod models;

use crate::backend::app::Api;
use crate::engine::cycles::manager::CycleManager;
use crate::engine::utils::config::load_config::{ensure_config_exists, load_config};
use crate::engine::utils::log::setup_logger;

use dotenvy::dotenv;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;

static CONFIG_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| ["config", "config.yaml"].iter().collect());

static MODEL_CONFIG_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| ["config", "model.yaml"].iter().collect());

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    println!("Logging initialization...");
    setup_logger()?;

    println!("Config initialization...");
    ensure_config_exists(vec![&*CONFIG_PATH, &*MODEL_CONFIG_PATH]);
    dotenv().ok();

    let config = load_config();
    let symbols = config.exchange.symbols;

    let mut cycle_types = HashMap::new();
    for symbol in symbols.clone().into_iter() {
        cycle_types.insert(symbol, config.runtime.cycle_type);
    }

    let mut manager = CycleManager::new().await;

    manager.run_all(symbols, cycle_types).await?;

    let counter_handle = manager.counter_handle();
    let supervisor_handle = manager.supervisor_handle();
    let prediction_handle = manager.prediction_handle();

    if config.backend.enabled {
        let api = Api::new(supervisor_handle, counter_handle, prediction_handle).await?;
        let api_task = tokio::spawn(async move {
            api.run().await;
        });

        tokio::select! {
            _ = api_task => println!("API has finished!"),
            _ = tokio::signal::ctrl_c() => {
                println!("Termination signal received!");
            }
        }
    } else {
        tokio::signal::ctrl_c().await.unwrap();
        println!("Termination signal received!");
    }

    Ok(())
}
