mod data;
mod engine;
mod models;
use engine::cycles::trading::cycle::TradingCycle;

#[tokio::main]
async fn main() {
    let mut cycle = TradingCycle::new(String::from("BTCUSDT")).await;
    cycle.run().await;
}
