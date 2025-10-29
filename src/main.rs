mod data;
mod engine;
use data::process::data_collection::{collect_all, flat_all};

#[tokio::main]
async fn main() {
    let token: &str = "BTCUSDT";
    let values = flat_all(collect_all(token).await);

    println!("{}", values.token);
    println!("{:?}", values.features);
    println!("{}", values.features.len());
}
