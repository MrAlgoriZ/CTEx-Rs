use reqwest::Client;
use serde::Deserialize;

use crate::data::data_interfaces::*;
use crate::engine::utils::parse::parse_symbol;

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    message: Option<String>,
}

fn client() -> Client {
    Client::new()
}

const BASE_URL: &'static str = "http://127.0.0.1:3737";

// TODO Переписать клиент после изменений в логике
pub struct CCXTClient {
    pub exchange_name: String,
}

impl CCXTClient {
    pub fn new(exchange_name: &str) -> Self {
        CCXTClient {
            exchange_name: exchange_name.to_string(),
        }
    }

    pub async fn fetch_ohlcv(
        &self,
        symbol: &str,
        timeframe: &str,
        limit: usize,
    ) -> Result<Vec<Candle>, anyhow::Error> {
        let payload = serde_json::json!({
            "exchange_name": &self.exchange_name,
            "symbol": parse_symbol(symbol),
            "timeframe": timeframe,
            "limit": limit
        });

        let res = client()
            .post(format!("{}/exchange/fetch/ohlcv", BASE_URL))
            .json(&payload)
            .send()
            .await?;

        let body: ApiResponse<serde_json::Value> = res.json().await?;
        if !body.success {
            return Err(anyhow::anyhow!(body.message.unwrap_or("".to_string())));
        }
        let raw_ohlcv = match body.data {
            Some(candles) => candles,
            None => return Err(anyhow::anyhow!("Data is None!")),
        };

        use anyhow::{Context, anyhow};

        let candles = raw_ohlcv
            .as_array()
            .context("ohlcv is not an array")?
            .iter()
            .map(|item| {
                let arr = item.as_array().context("ohlcv item is not an array")?;

                if arr.len() < 6 {
                    return Err(anyhow!("ohlcv item has less than 6 elements"));
                }

                Ok(Candle {
                    open: arr[1].as_f64().context("open is not a number")?,
                    high: arr[2].as_f64().context("high is not a number")?,
                    low: arr[3].as_f64().context("low is not a number")?,
                    close: arr[4].as_f64().context("close is not a number")?,
                    volume: arr[5].as_f64().context("volume is not a number")?,
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;

        Ok(candles)
    }

    // ticker response

    /* ApiResponse {
        success: true,
        data: Some(
            Object {
                "ask": Number(90142.92),
                "askVolume": Number(0.22416),
                "average": Number(89038.46),
                "baseVolume": Number(14551.52977),
                "bid": Number(90142.91),
                "bidVolume": Number(2.1456),
                "change": Number(2208.89),
                "close": Number(90142.91),
                "datetime": String("2026-01-28T13:15:24.013Z"),
                "high": Number(90363.09),
                "indexPrice": Null,
                "info": Object {
                    "askPrice": String("90142.92000000"),
                    "askQty": String("0.22416000"),
                    "bidPrice": String("90142.91000000"),
                    "bidQty": String("2.14560000"),
                    "closeTime": String("1769606124013"),
                    "count": String("4188864"),
                    "firstId": String("5819311961"),
                    "highPrice": String("90363.09000000"),
                    "lastId": String("5823500824"),
                    "lastPrice": String("90142.91000000"),
                    "lastQty": String("0.26528000"),
                    "lowPrice": String("87304.33000000"),
                    "openPrice": String("87934.02000000"),
                    "openTime": String("1769519724013"),
                    "prevClosePrice": String("87934.02000000"),
                    "priceChange": String("2208.89000000"),
                    "priceChangePercent": String("2.512"),
                    "quoteVolume": String("1293271924.13623260"),
                    "symbol": String("BTCUSDT"),
                    "volume": String("14551.52977000"),
                    "weightedAvgPrice": String("88875.32407778"),
                },
                "last": Number(90142.91),
                "low": Number(87304.33),
                "markPrice": Null,
                "open": Number(87934.02),
                "percentage": Number(2.512),
                "previousClose": Number(87934.02),
                "quoteVolume": Number(1293271924.1362326),
                "symbol": String("BTC/USDT"),
                "timestamp": Number(1769606124013),
                "vwap": Number(88875.32407778),
            },
        ),
        message: None,
    } */

    pub async fn fetch_ticker(&self, symbol: &str) -> Result<Ticker, anyhow::Error> {
        let payload = serde_json::json!({
            "exchange_name": &self.exchange_name,
            "symbol": parse_symbol(symbol)
        });

        let res = client()
            .post(format!("{}/exchange/fetch/ticker", BASE_URL))
            .json(&payload)
            .send()
            .await?;
        let body: ApiResponse<serde_json::Value> = res.json().await?;
        if !body.success {
            return Err(anyhow::anyhow!(body.message.unwrap_or("".to_string())));
        }
        let bid = body
            .data
            .clone()
            .unwrap()
            .get("bid")
            .unwrap()
            .as_f64()
            .unwrap();
        let ask = body
            .data
            .clone()
            .unwrap()
            .get("ask")
            .unwrap()
            .as_f64()
            .unwrap();
        let average = body
            .data
            .clone()
            .unwrap()
            .get("average")
            .unwrap()
            .as_f64()
            .unwrap();
        let open = body
            .data
            .clone()
            .unwrap()
            .get("open")
            .unwrap()
            .as_f64()
            .unwrap();
        let high = body
            .data
            .clone()
            .unwrap()
            .get("high")
            .unwrap()
            .as_f64()
            .unwrap();
        let low = body.data.unwrap().get("low").unwrap().as_f64().unwrap();

        Ok(Ticker::new(bid, ask, open, high, low, average))
    }

    pub async fn test_symbol(&self, symbol: &str) -> Result<(), anyhow::Error> {
        let payload = serde_json::json!({
            "exchange_name": &self.exchange_name,
            "symbol": parse_symbol(symbol)
        });

        let res = client()
            .post(format!("{}/exchange/fetch/ticker", BASE_URL))
            .json(&payload)
            .send()
            .await?;
        let body: ApiResponse<serde_json::Value> = res.json().await?;

        if !body.success {
            return Err(anyhow::anyhow!(body.message.unwrap_or("".to_string())));
        }
        if !body.data.clone().is_none() {
            Ok(())
        } else {
            return Err(anyhow::anyhow!(body.message.unwrap_or("".to_string())));
        }
    }
}
