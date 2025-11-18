mod backend;
mod data;
mod engine;
mod models;
use crate::{
    backend::app::Api,
    engine::{
        cycles::manager::{CycleManager, CycleType},
        state::counters::Counters,
        utils::config::load_config::{ensure_config_exists, load_config},
    },
};

use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

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

    let counters = Arc::new(tokio::sync::Mutex::new(Counters::new(
        load_config(CONFIG_PATH).data.accuracy_capacity,
    )));

    let manager = Arc::new(RwLock::new(
        CycleManager::new(symbols)
            .await
            .with_cycle_types(cycle_types)
            .with_counters(counters.clone()),
    ));

    let manager_clone = manager.clone();
    let manager_task = tokio::spawn(async move {
        manager_clone.write().await.run_all().await;
    });

    let api = Api::new(manager.clone(), counters).await;
    let api_task = tokio::spawn(async move {
        api.run().await;
    });

    tokio::select! {
        _ = manager_task => println!("Manager завершился!"),
        _ = api_task => println!("API завершилось!"),
    }
}
