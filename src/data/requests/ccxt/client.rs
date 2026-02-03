use anyhow::Context;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::data::data_interfaces::*;
use crate::engine::cycles::manager::ServersCommand;

pub struct CCXTClient {
    pub exchange_name: String,
    server_tx: mpsc::Sender<ServersCommand>,
}

impl CCXTClient {
    pub fn new(exchange_name: &str, server_tx: mpsc::Sender<ServersCommand>) -> Self {
        CCXTClient {
            exchange_name: exchange_name.to_string(),
            server_tx,
        }
    }

    pub async fn fetch_ohlcv(
        &self,
        symbol: &str,
        timeframe: &str,
        limit: usize,
    ) -> Result<Vec<Candle>, anyhow::Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .server_tx
            .clone()
            .send(ServersCommand::GetPriority { respond_to: tx })
            .await;
        let server = rx.await?.context("server is not string!")?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .server_tx
            .send(ServersCommand::FetchOhlcv {
                symbol: symbol.to_string(),
                timeframe: timeframe.to_string(),
                limit,
                exchange_name: self.exchange_name.clone(),
                server: server.to_string(),
                respond_to: tx,
            })
            .await;
        let candles = rx.await?;
        candles
    }

    pub async fn fetch_ohlcv_with_timestamp(
        &self,
        symbol: &str,
        timeframe: &str,
        limit: usize,
    ) -> Result<Vec<CandleWithTimestamp>, anyhow::Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .server_tx
            .clone()
            .send(ServersCommand::GetPriority { respond_to: tx })
            .await;
        let server = rx.await?.context("server is not string!")?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .server_tx
            .send(ServersCommand::FetchOhlcvWithTimestamps {
                symbol: symbol.to_string(),
                timeframe: timeframe.to_string(),
                limit,
                exchange_name: self.exchange_name.clone(),
                server: server.to_string(),
                respond_to: tx,
            })
            .await;
        let candles = rx.await?;
        candles
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
        let (tx, rx) = oneshot::channel();
        let _ = self
            .server_tx
            .clone()
            .send(ServersCommand::GetPriority { respond_to: tx })
            .await;
        let server = rx.await?.context("server is not string!")?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .server_tx
            .send(ServersCommand::FetchTicker {
                symbol: symbol.to_string(),
                exchange_name: self.exchange_name.clone(),
                server: server.to_string(),
                respond_to: tx,
            })
            .await;
        let ticker = rx.await?;
        ticker
    }

    pub async fn test_symbol(&self, symbol: &str) -> Result<(), anyhow::Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .server_tx
            .clone()
            .send(ServersCommand::GetPriority { respond_to: tx })
            .await;
        let server = rx.await?.context("server is not string!")?;
        let (tx, rx) = oneshot::channel();
        let _ = self
            .server_tx
            .send(ServersCommand::TestSymbol {
                symbol: symbol.to_string(),
                exchange_name: self.exchange_name.clone(),
                server: server.to_string(),
                respond_to: tx,
            })
            .await;
        let tested = rx.await?;
        tested
    }
}
