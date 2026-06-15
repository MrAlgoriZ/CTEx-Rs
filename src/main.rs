mod backend;
mod data;
mod engine;
mod models;

use crate::backend::app::Api;
use crate::engine::cycles::manager::CycleManager;
use crate::engine::utils::config::load_config::{config, ensure_config_exists, load_config};
use crate::engine::utils::log::setup_logger;

use anyhow::Result;
use dotenvy::dotenv;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;

type LazyPath = LazyLock<PathBuf>;

static MODEL_CONFIG_PATH: LazyPath = LazyLock::new(|| ["config", "model.yaml"].iter().collect());

static BACKEND_CONFIG_PATH: LazyPath =
    LazyLock::new(|| ["config", "backend.yaml"].iter().collect());

static BEHAVIOUR_CONFIG_PATH: LazyPath =
    LazyLock::new(|| ["config", "behaviour.yaml"].iter().collect());

static EXCHANGE_CONFIG_PATH: LazyPath =
    LazyLock::new(|| ["config", "exchange.yaml"].iter().collect());

static PRINTS_CONFIG_PATH: LazyPath = LazyLock::new(|| ["config", "prints.yaml"].iter().collect());

static RUNTIME_CONFIG_PATH: LazyPath =
    LazyLock::new(|| ["config", "runtime.yaml"].iter().collect());

#[tokio::main]
async fn main() -> Result<()> {
    setup_logger()?;

    ensure_config_exists(vec![
        &*MODEL_CONFIG_PATH,
        &*BACKEND_CONFIG_PATH,
        &*BEHAVIOUR_CONFIG_PATH,
        &*EXCHANGE_CONFIG_PATH,
        &*PRINTS_CONFIG_PATH,
        &*RUNTIME_CONFIG_PATH,
    ]);
    dotenv().ok();

    load_config()?;
    let symbols = config().exchange.symbols.clone();

    let mut cycle_types = HashMap::new();
    for symbol in symbols.clone().into_iter() {
        cycle_types.insert(symbol, config().runtime.cycle_type);
    }

    let mut manager = CycleManager::new().await;

    manager.run_all(symbols, cycle_types).await?;

    let counter_handle = manager.counter_handle();
    let supervisor_handle = manager.supervisor_handle();
    let prediction_handle = manager.prediction_handle();

    if config().backend.enabled {
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
