mod data;
use data::requests::ccxt::binance::BinanceClient;

fn main() {
    let client = BinanceClient::new();
    let day_price = client.fetch_day_price("BTCUSDT");
    println!("Day Price: {:?}", day_price);
}
