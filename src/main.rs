mod data;
mod engine;
use data::process::data_collection::{collect_all, flat_all};
use data::requests::database::db_req::*;
use engine::utils::config::load_env::load_env;
use sqlx::PgPool;
use tokio;

#[tokio::main]
async fn main() {
    let env: [String; 2] = load_env().try_into().unwrap();
    println!("{:?}", env);
    let db = PgPool::connect(&env[0]).await.unwrap();

    let token = "ETHUSDT";
    let values = collect_all(token).await;

    insert_candle(&db, token, flat_all(values).features)
        .await
        .unwrap();
    let results = select_all_candles(&db).await;
    println!("{:#?}", results);
}
