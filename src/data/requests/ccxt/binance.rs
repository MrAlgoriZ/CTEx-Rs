use std::sync::Arc;
use tokio::task;

use binance::api::Binance;
use binance::market::Market;

use crate::data::data_interfaces::*;

const MINIMAL_VALUE: f64 = f64::MIN;

pub struct BinanceClient {
    market: Arc<Market>,
}

impl BinanceClient {
    pub async fn new() -> Self {
        let market = tokio::task::spawn_blocking(|| Binance::new(None, None))
            .await
            .expect("spawn_blocking failed");

        BinanceClient {
            market: Arc::new(market),
        }
    }

    async fn run_blocking<F, T>(&self, f: F, default: T) -> T
    where
        F: FnOnce(&Market) -> Result<T, String> + Send + 'static,
        T: Send + 'static,
    {
        let market = Arc::clone(&self.market);
        let handle = task::spawn_blocking(move || f(&market));

        match handle.await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => {
                eprintln!("BinanceClient error: {}", e);
                default
            }
            Err(join_err) => {
                eprintln!("JoinError: {}", join_err);
                default
            }
        }
    }

    pub async fn fetch_ohlcv(&self, token: &str, timeframe: &str, limit: usize) -> Vec<ICandle> {
        let token = token.to_string();
        let timeframe = timeframe.to_string();

        return self
            .run_blocking(
                move |market| {
                    let mut ohlcv_list: Vec<ICandle> = Vec::new();

                    match market.get_klines(&token, &timeframe, limit as u16, None, None) {
                        Ok(binance::model::KlineSummaries::AllKlineSummaries(klines)) => {
                            for kline in klines {
                                let open: f64 = kline.open.parse().unwrap_or(MINIMAL_VALUE);
                                let high: f64 = kline.high.parse().unwrap_or(MINIMAL_VALUE);
                                let low: f64 = kline.low.parse().unwrap_or(MINIMAL_VALUE);
                                let close: f64 = kline.close.parse().unwrap_or(MINIMAL_VALUE);
                                let volume: f64 = kline.volume.parse().unwrap_or(MINIMAL_VALUE);

                                ohlcv_list.push(ICandle::new(open, high, low, close, volume));
                            }
                            Ok(ohlcv_list)
                        }
                        Err(e) => Err(format!("Binance error: {:?}", e)),
                    }
                },
                Vec::new(),
            )
            .await;
    }

    pub async fn fetch_average_price(&self, token: &str) -> f64 {
        let token = token.to_string();

        return self
            .run_blocking(
                move |market| match market.get_average_price(&token) {
                    Ok(answer) => Ok(answer.price),
                    Err(e) => Err(format!("Error: {:?}", e)),
                },
                MINIMAL_VALUE,
            )
            .await;
    }

    pub async fn fetch_ticker(&self, token: &str) -> ITicker {
        let token = token.to_string();

        return self
            .run_blocking(
                move |market| match market.get_book_ticker(&token) {
                    Ok(answer) => {
                        let bid = answer.bid_price;
                        let ask = answer.ask_price;
                        Ok(ITicker::new(bid, ask))
                    }
                    Err(e) => Err(format!("Error: {:?}", e)),
                },
                ITicker::new(MINIMAL_VALUE, MINIMAL_VALUE),
            )
            .await;
    }

    pub async fn fetch_day_price(&self, token: &str) -> IDayPrice {
        let token = token.to_string();

        return self
            .run_blocking(
                move |market| match market.get_24h_price_stats(&token) {
                    Ok(answer) => {
                        let open = answer.open_price;
                        let high = answer.high_price;
                        let low = answer.low_price;
                        Ok(IDayPrice::new(open, high, low))
                    }
                    Err(e) => Err(format!("Error: {:?}", e)),
                },
                IDayPrice::new(MINIMAL_VALUE, MINIMAL_VALUE, MINIMAL_VALUE),
            )
            .await;
    }

    pub async fn test_token(&self, symbol: &str) -> Result<(), String> {
        let token = symbol.to_string();

        let result = self
            .run_blocking(
                move |market| match market.get_book_ticker(&token) {
                    Ok(_) => Ok(Ok(())),
                    Err(_) => Err(String::from("Token not found")),
                },
                Err(()),
            )
            .await;

        if result.is_ok() {
            return Ok(());
        }

        Err(String::from("Token not found"))
    }
}
