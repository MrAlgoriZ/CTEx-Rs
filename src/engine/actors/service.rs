use super::{mpsc, oneshot};
use crate::data::data_interfaces::{Candle, CandleWithTimestamp, Ticker};
use crate::engine::utils::config::load_config::load_config;
use crate::engine::utils::parse::parse_symbol;

use anyhow::anyhow;
use log::{error, info};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    message: Option<String>,
}

pub struct ServiceState {
    pub active: bool,
    pub workload: u8,
}

pub enum ServiceCommand {
    #[allow(unused)]
    ListActive {
        respond_to: oneshot::Sender<Option<Vec<String>>>,
    },
    GetPriority {
        respond_to: oneshot::Sender<Option<String>>,
    },
    RemoveAllWorkload {
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    FetchOhlcv {
        symbol: String,
        timeframe: String,
        limit: usize,
        exchange_name: String,
        server: String,
        respond_to: oneshot::Sender<Result<Vec<Candle>, anyhow::Error>>,
    },
    FetchOhlcvWithTimestamps {
        symbol: String,
        timeframe: String,
        limit: usize,
        exchange_name: String,
        server: String,
        respond_to: oneshot::Sender<Result<Vec<CandleWithTimestamp>, anyhow::Error>>,
    },
    #[allow(unused)]
    FetchTicker {
        symbol: String,
        exchange_name: String,
        server: String,
        respond_to: oneshot::Sender<Result<Ticker, anyhow::Error>>,
    },
    TestSymbol {
        symbol: String,
        exchange_name: String,
        server: String,
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    UpdateActive {
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
}

async fn test_server(server: &str) -> bool {
    reqwest::Client::new()
        .get(format!("http://{}/", server))
        .send()
        .await
        .is_ok()
}

pub struct ServiceActor {
    servers: HashMap<String, ServiceState>,
    inbox: mpsc::Receiver<ServiceCommand>,
}

impl ServiceActor {
    pub async fn new() -> (Self, mpsc::Sender<ServiceCommand>) {
        let (tx, rx) = mpsc::channel(10);

        let servers_vec = load_config().exchange.servers;

        let mut servers = HashMap::new();

        for server in servers_vec {
            let active = test_server(&server).await;
            servers.insert(
                server,
                ServiceState {
                    active,
                    workload: 0,
                },
            );
        }

        if !servers.values().any(|s| s.active) {
            panic!("No active servers available!");
        }

        (Self { servers, inbox: rx }, tx)
    }

    pub async fn run(mut self) {
        info!("ServersActor has started!");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                ServiceCommand::RemoveAllWorkload { respond_to } => {
                    let result = self.remove_all_workload();
                    let _ = respond_to.send(result);
                }
                ServiceCommand::ListActive { respond_to } => {
                    let result = self.list_active();
                    let _ = respond_to.send(result);
                }
                ServiceCommand::UpdateActive { respond_to } => {
                    let result = self.update_active().await;
                    let _ = respond_to.send(result);
                }
                ServiceCommand::GetPriority { respond_to } => {
                    let result = self.get_priority();
                    let _ = respond_to.send(result);
                }
                ServiceCommand::FetchOhlcv {
                    symbol,
                    timeframe,
                    limit,
                    exchange_name,
                    server,
                    respond_to,
                } => {
                    let result = self
                        .fetch_ohlcv(&symbol, &timeframe, limit, &exchange_name, &server)
                        .await;
                    let _ = respond_to.send(result);
                }
                ServiceCommand::FetchOhlcvWithTimestamps {
                    symbol,
                    timeframe,
                    limit,
                    exchange_name,
                    server,
                    respond_to,
                } => {
                    let result = self
                        .fetch_ohlcv_with_timestamps(
                            &symbol,
                            &timeframe,
                            limit,
                            &exchange_name,
                            &server,
                        )
                        .await;
                    let _ = respond_to.send(result);
                }
                ServiceCommand::FetchTicker {
                    symbol,
                    exchange_name,
                    server,
                    respond_to,
                } => {
                    let result = self.fetch_ticker(&symbol, &exchange_name, &server).await;
                    let _ = respond_to.send(result);
                }
                ServiceCommand::TestSymbol {
                    symbol,
                    exchange_name,
                    server,
                    respond_to,
                } => {
                    let result = self.test_symbol(&symbol, &exchange_name, &server).await;
                    let _ = respond_to.send(result);
                }
            }
        }
    }

    fn add_workload(&mut self, server: String, num: u8) -> Result<(), anyhow::Error> {
        let state = self
            .servers
            .get_mut(&server)
            .ok_or_else(|| anyhow!("Server not found!"))?;

        if !state.active {
            return Err(anyhow!("Server is inactive!"));
        }

        state.workload = state.workload.saturating_add(num);
        Ok(())
    }

    fn remove_all_workload(&mut self) -> Result<(), anyhow::Error> {
        for state in self.servers.values_mut() {
            state.workload = 0;
        }
        Ok(())
    }

    fn list_active(&self) -> Option<Vec<String>> {
        let active: Vec<String> = self
            .servers
            .iter()
            .filter(|(_, s)| s.active)
            .map(|(k, _)| k.clone())
            .collect();

        if active.is_empty() {
            None
        } else {
            Some(active)
        }
    }

    async fn update_active(&mut self) -> Result<(), anyhow::Error> {
        for (server, state) in self.servers.iter_mut() {
            let is_active = test_server(server).await;
            state.active = is_active;
        }
        Ok(())
    }

    fn get_priority(&self) -> Option<String> {
        let mut active: Vec<(&String, &ServiceState)> =
            self.servers.iter().filter(|(_, s)| s.active).collect();

        if active.is_empty() {
            return None;
        }

        if active.len() == 1 {
            return Some(active[0].0.clone());
        }

        active.sort_by_key(|(_, s)| s.workload);

        Some(active[0].0.clone())
    }

    fn mark_server_inactive(&mut self, server: &str) {
        if let Some(state) = self.servers.get_mut(server) {
            state.active = false;
        }
    }

    async fn fetch_ohlcv(
        &mut self,
        symbol: &str,
        timeframe: &str,
        limit: usize,
        exchange_name: &str,
        server: &str,
    ) -> Result<Vec<Candle>, anyhow::Error> {
        let mut current_server = server.to_string();

        loop {
            let payload = serde_json::json!({
                "exchange_name": exchange_name,
                "symbol": parse_symbol(symbol),
                "timeframe": timeframe,
                "limit": limit
            });

            let res = match reqwest::Client::new()
                .post(format!("http://{}/exchange/fetch/ohlcv", current_server))
                .json(&payload)
                .send()
                .await
            {
                Ok(ohlcv) => ohlcv,
                Err(e) => {
                    error!("{}", e);
                    self.mark_server_inactive(&current_server);

                    current_server = self
                        .get_priority()
                        .ok_or_else(|| anyhow!("All servers are inactive!"))?;

                    continue;
                }
            };

            let body: ApiResponse<serde_json::Value> = res
                .json()
                .await
                .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))?;
            self.add_workload(current_server.clone(), limit as u8)?;
            if !body.success {
                return Err(anyhow!(body.message.unwrap_or("".to_string())));
            }
            let raw_ohlcv = match body.data {
                Some(candles) => candles,
                None => return Err(anyhow!("Data is None!")),
            };

            let candles = raw_ohlcv
                .as_array()
                .ok_or_else(|| anyhow!("ohlcv is not an array"))?
                .iter()
                .map(|item| {
                    let arr = item
                        .as_array()
                        .ok_or_else(|| anyhow!("ohlcv item is not an array"))?;

                    if arr.len() < 6 {
                        return Err(anyhow!("ohlcv item has less than 6 elements"));
                    }

                    Ok(Candle {
                        open: arr[1]
                            .as_f64()
                            .ok_or_else(|| anyhow!("open is not a number"))?,
                        high: arr[2]
                            .as_f64()
                            .ok_or_else(|| anyhow!("high is not a number"))?,
                        low: arr[3]
                            .as_f64()
                            .ok_or_else(|| anyhow!("low is not a number"))?,
                        close: arr[4]
                            .as_f64()
                            .ok_or_else(|| anyhow!("close is not a number"))?,
                        volume: arr[5]
                            .as_f64()
                            .ok_or_else(|| anyhow!("volume is not a number"))?,
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?;

            return Ok(candles);
        }
    }

    async fn fetch_ohlcv_with_timestamps(
        &mut self,
        symbol: &str,
        timeframe: &str,
        limit: usize,
        exchange_name: &str,
        server: &str,
    ) -> Result<Vec<CandleWithTimestamp>, anyhow::Error> {
        let mut current_server = server.to_string();

        loop {
            let payload = serde_json::json!({
                "exchange_name": exchange_name,
                "symbol": parse_symbol(symbol),
                "timeframe": timeframe,
                "limit": limit
            });

            let res = match reqwest::Client::new()
                .post(format!("http://{}/exchange/fetch/ohlcv", current_server))
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => response,
                Err(e) => {
                    error!("{}", e);
                    self.mark_server_inactive(&current_server);

                    current_server = self
                        .get_priority()
                        .ok_or_else(|| anyhow!("All servers are inactive!"))?;

                    continue;
                }
            };

            let body: ApiResponse<serde_json::Value> = res
                .json()
                .await
                .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))?;
            self.add_workload(current_server.clone(), limit as u8)?;
            if !body.success {
                return Err(anyhow!(body.message.unwrap_or("".to_string())));
            }
            let raw_ohlcv = match body.data {
                Some(candles) => candles,
                None => return Err(anyhow!("Data is None!")),
            };

            let candles = raw_ohlcv
                .as_array()
                .ok_or_else(|| anyhow!("ohlcv is not an array"))?
                .iter()
                .map(|item| {
                    let arr = item
                        .as_array()
                        .ok_or_else(|| anyhow!("ohlcv item is not an array"))?;

                    if arr.len() < 6 {
                        return Err(anyhow!("ohlcv item has less than 6 elements"));
                    }

                    Ok(CandleWithTimestamp {
                        timestamp: arr[0]
                            .as_u64()
                            .ok_or_else(|| anyhow!("timestamp is not a number"))?,
                        open: arr[1]
                            .as_f64()
                            .ok_or_else(|| anyhow!("open is not a number"))?,
                        high: arr[2]
                            .as_f64()
                            .ok_or_else(|| anyhow!("high is not a number"))?,
                        low: arr[3]
                            .as_f64()
                            .ok_or_else(|| anyhow!("low is not a number"))?,
                        close: arr[4]
                            .as_f64()
                            .ok_or_else(|| anyhow!("close is not a number"))?,
                        volume: arr[5]
                            .as_f64()
                            .ok_or_else(|| anyhow!("volume is not a number"))?,
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?;

            return Ok(candles);
        }
    }

    async fn fetch_ticker(
        &mut self,
        symbol: &str,
        exchange_name: &str,
        server: &str,
    ) -> Result<Ticker, anyhow::Error> {
        let mut current_server = server.to_string();

        loop {
            let payload = serde_json::json!({
                "exchange_name": exchange_name,
                "symbol": parse_symbol(symbol)
            });

            let res = match reqwest::Client::new()
                .post(format!("http://{}/exchange/fetch/ticker", current_server))
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => response,
                Err(e) => {
                    error!("{}", e);
                    self.mark_server_inactive(&current_server);

                    current_server = self
                        .get_priority()
                        .ok_or_else(|| anyhow!("All servers are inactive!"))?;

                    continue;
                }
            };

            let body: ApiResponse<serde_json::Value> = res
                .json()
                .await
                .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))?;
            self.add_workload(current_server.clone(), 1)?;
            if !body.success {
                return Err(anyhow!(body.message.unwrap_or("".to_string())));
            }
            let data = body
                .data
                .as_ref()
                .ok_or_else(|| anyhow!("Response data is None!"))?;
            let bid = data
                .get("bid")
                .ok_or_else(|| anyhow!("bid field is missing"))?
                .as_f64()
                .ok_or_else(|| anyhow!("bid is not a number"))?;
            let ask = data
                .get("ask")
                .ok_or_else(|| anyhow!("ask field is missing"))?
                .as_f64()
                .ok_or_else(|| anyhow!("ask is not a number"))?;

            return Ok(Ticker { bid, ask });
        }
    }

    async fn test_symbol(
        &mut self,
        symbol: &str,
        exchange_name: &str,
        server: &str,
    ) -> Result<(), anyhow::Error> {
        let payload = serde_json::json!({
            "exchange_name": exchange_name,
            "symbol": parse_symbol(symbol)
        });

        let res = reqwest::Client::new()
            .post(format!("http://{}/exchange/fetch/ticker", server))
            .json(&payload)
            .send()
            .await
            .map_err(|e| anyhow!("Failed to send request: {}", e))?;
        self.add_workload(server.to_string(), 1)?;
        let body: ApiResponse<serde_json::Value> = res
            .json()
            .await
            .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))?;

        if !body.success {
            return Err(anyhow!(body.message.unwrap_or("".to_string())));
        }
        if body.data.is_some() {
            Ok(())
        } else {
            Err(anyhow!(body.message.unwrap_or("".to_string())))
        }
    }
}
