use binance::api::*;
use binance::market::*;

use crate::data::data_interfaces::*;

pub struct BinanceClient {
    market: Market,
}

impl BinanceClient {
    pub fn new() -> Self {
        BinanceClient {
            market: Binance::new(None, None),
        }
    }

    pub fn fetch_ohlcv(&self, token: &str, timeframe: &str, limit: u16) -> Vec<ICandle> {
        // -> limit, limit-1, limit-2, ..., 1
        // -> [[open, high, low, close, volume], [open, high, low, close, volume], ...]
        let mut ohlcv_list: Vec<ICandle> = Vec::new();

        match self.market.get_klines(token, timeframe, limit, None, None) {
            Ok(klines) => match klines {
                binance::model::KlineSummaries::AllKlineSummaries(klines) => {
                    for kline in klines.clone() {
                        let open: f64 = kline.open.parse().unwrap();
                        let high: f64 = kline.high.parse().unwrap();
                        let low: f64 = kline.low.parse().unwrap();
                        let close: f64 = kline.close.parse().unwrap();
                        let volume: f64 = kline.volume.parse().unwrap();

                        ohlcv_list.push(ICandle::new(open, high, low, close, volume));
                    }
                }
            },
            Err(e) => println!("Error: {}", e),
        }
        ohlcv_list
    }

    pub fn fetch_average_price(&self, token: &str) -> f64 {
        // Return the average price of a token, (mins=5)
        match self.market.get_average_price(token) {
            Ok(answer) => answer.price,
            Err(e) => {
                println!("Error: {:?}", e);
                0.0
            }
        }
    }

    pub fn fetch_ticker(&self, token: &str) -> ITicker {
        // -> [bid_price, ask_price]
        match self.market.get_book_ticker(token) {
            Ok(answer) => ITicker::new(answer.bid_price, answer.ask_price),
            Err(e) => {
                println!("Error: {:?}", e);
                ITicker::new(0.0, 0.0)
            }
        }
    }

    pub fn fetch_day_price(&self, token: &str) -> IDayPrice {
        // -> [open_price, high_price, low_price]
        match self.market.get_24h_price_stats(token) {
            Ok(answer) => IDayPrice::new(answer.open_price, answer.high_price, answer.low_price),
            Err(e) => {
                println!("Error: {}", e);
                IDayPrice::new(0.0, 0.0, 0.0)
            }
        }
    }
}
