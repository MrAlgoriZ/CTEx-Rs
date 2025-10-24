use std::sync::Arc;
use tokio::task;

use binance::market::*;

use crate::data::data_interfaces::*;

pub struct BinanceClient {
    market: Arc<Market>,
}

impl BinanceClient {
    pub async fn new() -> Self {
        let market = tokio::task::spawn_blocking(|| {
            // создаём Market в блокирующем контексте
            binance::api::Binance::new(None, None)
        })
        .await
        .expect("spawn_blocking failed");

        BinanceClient {
            market: std::sync::Arc::new(market),
        }
    }

    pub async fn fetch_ohlcv(
        &self,
        token: &str,
        timeframe: &str,
        limit: u16,
    ) -> Result<Vec<ICandle>, String> {
        let market = Arc::clone(&self.market);
        let token = token.to_string();
        let timeframe = timeframe.to_string();

        let handle = task::spawn_blocking(move || {
            let mut ohlcv_list: Vec<ICandle> = Vec::new();

            match market.get_klines(&token, &timeframe, limit, None, None) {
                Ok(binance::model::KlineSummaries::AllKlineSummaries(klines)) => {
                    for kline in klines {
                        let open: f64 = kline.open.parse().unwrap_or(0.0);
                        let high: f64 = kline.high.parse().unwrap_or(0.0);
                        let low: f64 = kline.low.parse().unwrap_or(0.0);
                        let close: f64 = kline.close.parse().unwrap_or(0.0);
                        let volume: f64 = kline.volume.parse().unwrap_or(0.0);

                        ohlcv_list.push(ICandle::new(open, high, low, close, volume));
                    }
                    Ok(ohlcv_list)
                }
                Err(e) => Err(format!("Binance error: {:?}", e)),
            }
        });

        handle.await.map_err(|e| format!("JoinError: {}", e))?
    }

    pub async fn fetch_average_price(&self, token: &str) -> Result<f64, String> {
        let market = Arc::clone(&self.market);
        let token = token.to_string();

        let handle = task::spawn_blocking(move || {
            match market.get_average_price(&token) {
                Ok(answer) => Ok(answer.price),
                Err(e) => Err(format!("Error: {:?}", e)),
            }
        });

        handle.await.map_err(|e| format!("JoinError: {}", e))?
    }


    pub async fn fetch_ticker(&self, token: &str) -> Result<ITicker, String> {
        let market = Arc::clone(&self.market);
        let token = token.to_string();

        let handle = task::spawn_blocking(move || {
            match market.get_book_ticker(&token) {
                Ok(answer) => {
                    let bid = answer.bid_price;
                    let ask = answer.ask_price;
                    Ok(ITicker::new(bid, ask))
                }
                Err(e) => Err(format!("Error: {:?}", e)),
            }
        });

        handle.await.map_err(|e| format!("JoinError: {}", e))?
    }


    pub async fn fetch_day_price(&self, token: &str) -> Result<IDayPrice, String> {
        let market = Arc::clone(&self.market);
        let token = token.to_string();

        let handle = task::spawn_blocking(move || {
            match market.get_24h_price_stats(&token) {
                Ok(answer) => {
                    let open = answer.open_price;
                    let high = answer.high_price;
                    let low = answer.low_price;
                    Ok(IDayPrice::new(open, high, low))
                }
                Err(e) => Err(format!("Error: {:?}", e)),
            }
        });

        handle.await.map_err(|e| format!("JoinError: {}", e))?
    }
}
