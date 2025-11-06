mod data;
mod engine;
mod models;
use crate::data::requests::database::db_req::select_all_candles;
use crate::engine::utils::config::load_env::load_env;
use crate::engine::{
    cycles::manager::{CycleManager, CycleType},
    utils::config::load_config::load_config,
};
use crate::models::model::RFInterface;

use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    let symbols = load_config("config/config.yaml").token;
    let model = Arc::new(Mutex::new(RFInterface::new()));
    let pool = PgPool::connect(&load_env()[0]).await.unwrap();

    train_model(&pool, &model).await;
    drop(pool);

    let mut cycle_types = HashMap::new();
    for symbol in symbols.clone().into_iter() {
        cycle_types.insert(symbol, CycleType::Trading);
    }

    let manager = CycleManager::new(symbols, Some(model)).with_cycle_types(cycle_types);
    manager.run_all().await
}

async fn train_model(pool: &PgPool, model: &Arc<Mutex<RFInterface>>) {
    let data = select_all_candles(pool).await.unwrap();
    let model_clone = model.clone();
    tokio::task::spawn_blocking(move || {
        let mut model_guard = model_clone.lock().unwrap();
        model_guard
            .train(data)
            .expect("The model faced a problem with learning");
    })
    .await
    .unwrap();
}
