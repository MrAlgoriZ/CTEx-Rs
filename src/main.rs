mod data;
mod engine;
mod models;
use engine::cycles::loader::cycle::LoaderCycle;

#[tokio::main]
async fn main() {
    let mut cycle = LoaderCycle::new(String::from("BTCUSDT")).await;
    cycle.run().await;
}
