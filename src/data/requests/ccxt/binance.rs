use binance::api::Binance;
use binance::market::Market;
use std::sync::Arc;

use crate::data::data_interfaces::*;

pub struct BinanceClient {
    market: Arc<Market>,
}

impl BinanceClient {
    const MAX_RETRIES: usize = 3;
    const RETRY_DELAY_SECS: u64 = 1;

    pub fn new() -> Self {
        let market = Binance::new(None, None);

        BinanceClient {
            market: Arc::new(market),
        }
    }

    pub async fn fetch_ohlcv(
        &self,
        token: &str,
        timeframe: &str,
        limit: usize,
    ) -> Result<Vec<ICandle>, String> {
        for attempt in 1..=Self::MAX_RETRIES {
            match self
                .market
                .get_klines(token, timeframe, limit as u16, None, None)
            {
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
                    return Ok(ohlcv_list);
                }
                Err(e) => {
                    eprintln!(
                        "fetch_ohlcv attempt {}/{} failed for {}: {:?}",
                        attempt,
                        Self::MAX_RETRIES,
                        token,
                        e
                    );
                    if attempt == Self::MAX_RETRIES {
                        return Err(format!(
                            "Failed to fetch OHLCV for {} after {} attempts",
                            token,
                            Self::MAX_RETRIES
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(Self::RETRY_DELAY_SECS))
                        .await;
                }
            }
        }
        unreachable!()
    }

    pub async fn fetch_average_price(&self, token: &str) -> Result<f64, String> {
        for attempt in 1..=Self::MAX_RETRIES {
            match self.market.get_average_price(token) {
                Ok(answer) => return Ok(answer.price),
                Err(e) => {
                    eprintln!(
                        "fetch_average_price attempt {}/{} failed for {}: {:?}",
                        attempt,
                        Self::MAX_RETRIES,
                        token,
                        e
                    );
                    if attempt == Self::MAX_RETRIES {
                        return Err(format!(
                            "Failed to fetch average price for {} after {} attempts",
                            token,
                            Self::MAX_RETRIES
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(Self::RETRY_DELAY_SECS))
                        .await;
                }
            }
        }
        unreachable!()
    }

    pub async fn fetch_ticker(&self, token: &str) -> Result<ITicker, String> {
        for attempt in 1..=Self::MAX_RETRIES {
            match self.market.get_book_ticker(token) {
                Ok(answer) => return Ok(ITicker::new(answer.bid_price, answer.ask_price)),
                Err(e) => {
                    eprintln!(
                        "fetch_ticker attempt {}/{} failed for {}: {:?}",
                        attempt,
                        Self::MAX_RETRIES,
                        token,
                        e
                    );
                    if attempt == Self::MAX_RETRIES {
                        return Err(format!(
                            "Failed to fetch ticker for {} after {} attempts",
                            token,
                            Self::MAX_RETRIES
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(Self::RETRY_DELAY_SECS))
                        .await;
                }
            }
        }
        unreachable!()
    }

    pub async fn fetch_day_price(&self, token: &str) -> Result<IDayPrice, String> {
        for attempt in 1..=Self::MAX_RETRIES {
            match self.market.get_24h_price_stats(token) {
                Ok(answer) => {
                    return Ok(IDayPrice::new(
                        answer.open_price,
                        answer.high_price,
                        answer.low_price,
                    ));
                }
                Err(e) => {
                    eprintln!(
                        "fetch_day_price attempt {}/{} failed for {}: {:?}",
                        attempt,
                        Self::MAX_RETRIES,
                        token,
                        e
                    );
                    if attempt == Self::MAX_RETRIES {
                        return Err(format!(
                            "Failed to fetch day price for {} after {} attempts",
                            token,
                            Self::MAX_RETRIES
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(Self::RETRY_DELAY_SECS))
                        .await;
                }
            }
        }
        unreachable!()
    }

    pub async fn test_token(&self, symbol: &str) -> Result<(), String> {
        let token = symbol.to_string();

        let result = match self.market.get_book_ticker(&token) {
            Ok(_) => Ok(()),
            Err(_) => Err(String::from("Token not found")),
        };

        if result.is_ok() {
            return Ok(());
        }

        Err(String::from("Token not found"))
    }
}
