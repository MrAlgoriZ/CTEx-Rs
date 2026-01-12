use binance::api::Binance;
use binance::market::Market;
use std::sync::Arc;
use tokio::task;
use tokio::time::{Duration, sleep};

use crate::data::data_interfaces::*;

// ВАЖНО! СОЗДАВАТЬ ТОЛЬКО С ИСПОЛЬЗОВАНИЕМ spawn_blocking(), Market может уронить асинхронный контекст
pub struct BinanceClient {
    market: Arc<Market>,
}

impl BinanceClient {
    const MAX_RETRIES: usize = 3;
    const RETRY_DELAY_SECS: u64 = 1;

    pub async fn new() -> Self {
        let market = tokio::task::spawn_blocking(|| Binance::new(None, None))
            .await
            .expect("spawn_blocking failed");

        BinanceClient {
            market: Arc::new(market),
        }
    }

    async fn run_blocking<F, T>(&self, f: F) -> Result<T, String>
    where
        F: Fn(&Market) -> Result<T, String> + Clone + Send + Sync + 'static,
        T: Send + 'static,
    {
        for attempt in 1..=Self::MAX_RETRIES {
            let market = Arc::clone(&self.market);
            let f_clone = f.clone();
            let handle = task::spawn_blocking(move || f_clone(&market));
            match handle.await {
                Ok(Ok(result)) => return Ok(result),
                Ok(Err(e)) => {
                    eprintln!("BinanceClient error on attempt {}/{}: {}", attempt, Self::MAX_RETRIES, e);
                    if attempt == Self::MAX_RETRIES {
                        return Err(format!("Failed after {} attempts: {}", Self::MAX_RETRIES, e));
                    }
                    sleep(Duration::from_secs(Self::RETRY_DELAY_SECS)).await;
                }
                Err(join_err) => {
                    eprintln!("JoinError on attempt {}/{}: {}", attempt, Self::MAX_RETRIES, join_err);
                    if attempt == Self::MAX_RETRIES {
                        return Err(format!("JoinError after {} attempts: {}", Self::MAX_RETRIES, join_err));
                    }
                    sleep(Duration::from_secs(Self::RETRY_DELAY_SECS)).await;
                }
            }
        }
        unreachable!()
    }

    pub async fn fetch_ohlcv(
        &self,
        token: &str,
        timeframe: &str,
        limit: usize,
    ) -> Result<Vec<ICandle>, String> {
        let token = token.to_string();
        let timeframe = timeframe.to_string();
        let limit = limit as u16;
        self.run_blocking(move |market| {
            match market.get_klines(&token, &timeframe, limit, None, None) {
                Ok(binance::model::KlineSummaries::AllKlineSummaries(klines)) => {
                    let mut ohlcv_list = Vec::with_capacity(klines.len());
                    for kline in klines {
                        let open = kline.open.parse().unwrap_or(f64::MIN);
                        let high = kline.high.parse().unwrap_or(f64::MIN);
                        let low = kline.low.parse().unwrap_or(f64::MIN);
                        let close = kline.close.parse().unwrap_or(f64::MIN);
                        let volume = kline.volume.parse().unwrap_or(f64::MIN);

                        ohlcv_list.push(ICandle::new(open, high, low, close, volume));
                    }
                    Ok(ohlcv_list)
                }
                Err(e) => Err(format!("{:?}", e)),
            }
        }).await
    }

    pub async fn fetch_average_price(&self, token: &str) -> Result<f64, String> {
        let token = token.to_string();
        self.run_blocking(move |market| {
            match market.get_average_price(&token) {
                Ok(answer) => Ok(answer.price),
                Err(e) => Err(format!("{:?}", e)),
            }
        }).await
    }

    pub async fn fetch_ticker(&self, token: &str) -> Result<ITicker, String> {
        let token = token.to_string();
        self.run_blocking(move |market| {
            match market.get_book_ticker(&token) {
                Ok(answer) => Ok(ITicker::new(answer.bid_price, answer.ask_price)),
                Err(e) => Err(format!("{:?}", e)),
            }
        }).await
    }

    pub async fn fetch_day_price(&self, token: &str) -> Result<IDayPrice, String> {
        let token = token.to_string();
        self.run_blocking(move |market| {
            match market.get_24h_price_stats(&token) {
                Ok(answer) => Ok(IDayPrice::new(
                    answer.open_price,
                    answer.high_price,
                    answer.low_price,
                )),
                Err(e) => Err(format!("{:?}", e)),
            }
        }).await
    }

    pub async fn test_token(&self, symbol: &str) -> Result<(), String> {
        let token = symbol.to_string();
        self.run_blocking(move |market| {
            match market.get_book_ticker(&token) {
                Ok(_) => Ok(()),
                Err(e) => Err(format!("Token not found: {:?}", e)),
            }
        }).await
    }
}
