mod data;
use data::requests::ccxt::binance::BinanceClient;

#[tokio::main]
async fn main() {
    let client = BinanceClient::new().await;
    let symbol = "BTCUSDT";
    let ohlcv = client.fetch_ohlcv(symbol, "1m", 100).await;
    let ticker = client.fetch_ticker(symbol).await;
    println!("ohlcv: {:?}", ohlcv);
    println!("ticker {:?}", ticker);
}