mod data;
use data::requests::ccxt::binance::BinanceClient;
use data::requests::atr::*;

#[tokio::main]
async fn main() {
    let client = BinanceClient::new().await;

    let symbol = "BTCUSDT";

    // Пример параллельных вызовов
    let f1 = client.fetch_average_price(symbol);
    let f2 = client.fetch_ticker(symbol);
    let f3 = client.fetch_ohlcv(symbol, "1m", 100);
    let atr = get_all_atr(&client, symbol);


    let vol = get_volatility(&client, symbol);


    // Выполняем параллельно
    let (p, t, ohlcv, atr_values, vol_values) = tokio::join!(f1, f2, f3, atr, vol);

    println!("avg price: {:?}", p);
    println!("ticker: {:?}", t);
    println!("ohlcv.len: {:?}", ohlcv.map(|v| v.len()));
    println!("ATR values: {:?}", atr_values);
    println!("Volatility: {}", vol_values);
}