// mod data;
// use data::requests::ccxt::binance::BinanceClient;
// mod engine;
// use engine::utils::processor::process_ohlcv;

// #[tokio::main]
// async fn main() {
//     let client = BinanceClient::new().await;
//     let symbol = "DOGEUSDT";
//     let ohlcv: Vec<data::data_interfaces::ICandle> = client.fetch_ohlcv(symbol, "1m", 2).await;
//     println!("Ohlcv: {:?}", ohlcv);
//     let static_ohlcv: Vec<data::data_interfaces::ICandle> = process_ohlcv(&ohlcv).await;
//     println!("Static ohlcv: {:?}", static_ohlcv);
// }

mod models;
use models::model::mt_main;

fn main() {
    let result = mt_main();
    println!("{:?}", result);
}