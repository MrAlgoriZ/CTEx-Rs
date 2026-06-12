use anyhow::{Result, anyhow};
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use crate::data::data_interfaces::{Candle, DataMap};
use crate::data::process::data_collection::{OHLCV_FETCH_LEN, OHLCV_LEN, collect_targets};
use crate::data::process::volatility::get_volatility;
use crate::data::requests::ccxt::account::{Direction, DummyAccount};
use crate::data::requests::ccxt::client::CCXTClient;
use crate::data::requests::database::standart::SQLStandart;
use crate::engine::actors::chain::ChainCommand;
use crate::engine::actors::counter::CounterCommand;
use crate::engine::actors::model::ModelCommand;
use crate::engine::cycles::CyclePhase;
use crate::engine::cycles::manager::CycleError;
use crate::engine::cycles::traits::{
    Cycle, CycleGetters, CycleGettersForCycleWithModel, CycleWithModel,
};
use crate::engine::state::chain::Block;
use crate::engine::state::counters::SymbolCounters;
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::engine::utils::config::load_env::load_env;

pub struct SandboxCycle {
    pub symbol: String,
    last_candles: Option<DataMap>,
    last_predictions: Option<DataMap>,
    last_order_price: Option<f64>,
    print_symbol: String,
    config: Config,
    pool: PgPool,
    client: CCXTClient,
    account: DummyAccount,
    start_balance: Option<f64>,
}

impl CycleGetters for SandboxCycle {
    fn get_symbol(&self) -> &str {
        &self.symbol
    }

    fn get_print_symbol(&self) -> &str {
        &self.print_symbol
    }

    fn get_config(&self) -> &Config {
        &self.config
    }

    fn get_client(&self) -> &CCXTClient {
        &self.client
    }
}

impl CycleGettersForCycleWithModel for SandboxCycle {
    fn get_pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    fn change_last_predictions(&mut self, predictions: DataMap) {
        self.last_predictions = Some(predictions);
    }
}

impl Cycle for SandboxCycle {}
impl CycleWithModel for SandboxCycle {}

impl SandboxCycle {
    fn new(symbol: String, pool: PgPool, client: CCXTClient, account: DummyAccount) -> Self {
        SandboxCycle {
            print_symbol: format!("{}{}:", Fore::Blue.as_str(), symbol),
            symbol: symbol.clone(),
            last_candles: None,
            last_predictions: None,
            last_order_price: None,
            config: load_config(),
            pool,
            client,
            account,
            start_balance: None,
        }
    }

    pub async fn init(symbol: String, client: CCXTClient) -> Result<Self> {
        let pool = PgPool::connect(&load_env().database_url).await?;
        let account = DummyAccount::init("".to_string(), "".to_string());
        Ok(Self::new(symbol, pool, client, account))
    }

    pub async fn run(
        mut self,
        counter_tx: &mpsc::Sender<CounterCommand>,
        model_tx: &mpsc::Sender<ModelCommand>,
        chain_tx: Option<&mpsc::Sender<ChainCommand>>,
    ) -> Result<(), CycleError> {
        if self.get_client().test_symbol(&self.symbol).await.is_err() {
            return Err(CycleError::SymbolDoesNotExist);
        }
        let mut volatility: f64 = 0.0;

        let mut phase = CyclePhase::Warmup;
        let mut prediction: Option<f64> = None;

        loop {
            self.wait_for_next_interval().await?;
            self.update_volatility(&mut volatility).await?;
            if self.config.prints.cycle.volatility {
                self.print_volatility_status(volatility);
            }

            let (candles, ohlcv) = self
                .get_client()
                .collect_all(
                    &self.symbol,
                    &self.config.exchange.timeframes.main_timeframe,
                )
                .await?;

            if let CyclePhase::Active = phase {
                let targets = DataMap::new(
                    Some(self.get_symbol().to_string()),
                    collect_targets(ohlcv[..OHLCV_LEN].try_into().unwrap()),
                );

                let target = targets.get("position_size").unwrap();

                let ratio = if target != &0.0 {
                    (prediction.unwrap() - target).abs() / (target).abs()
                } else {
                    0.0
                };

                let success: bool =
                    ratio < (self.config.behaviour.success_threshold * 100.0 * volatility);

                if self.config.prints.cycle.target {
                    debug!(
                        "{} {}Pred: {:.5} | Target: {:.5} | Ratio {:.5}",
                        self.print_symbol,
                        Fore::White.as_str(),
                        prediction.unwrap(),
                        target,
                        ratio
                    );
                }

                self.update_counters(prediction.unwrap(), *target, volatility, counter_tx)
                    .await;

                if !success {
                    let last_candles = self.last_candles.clone().unwrap();
                    let last_predictions = self.last_predictions.clone().unwrap();
                    let (tx, rx) = oneshot::channel();
                    let _ = model_tx
                        .send(ModelCommand::GetAccuracy { respond_to: tx })
                        .await;
                    let accuracy = rx.await.map_err(|e| anyhow!(e))?;

                    let summary_data = {
                        if let Some(acc) = accuracy {
                            last_candles + acc + targets.clone()
                        } else {
                            last_candles + targets.clone()
                        }
                    };

                    self.handle_mistake(summary_data, last_predictions, counter_tx, Some(model_tx))
                        .await?;

                    if let Some(ctx) = chain_tx {
                        let (tx, rx) = oneshot::channel();
                        let _ = ctx
                            .send(ChainCommand::AddBlock {
                                symbol: self.get_symbol().to_string(),
                                block: Block::new(
                                    self.last_predictions.clone().unwrap().to_hashmap(),
                                    targets.clone().to_hashmap(),
                                    *ohlcv.last().unwrap(),
                                ),
                                respond_to: tx,
                            })
                            .await;
                        let _ = rx.await;
                    }
                }
            }

            let candles_to_pred = candles.clone();
            prediction = Some(self.predict(candles_to_pred, model_tx).await.unwrap());

            let direction = if prediction.unwrap() > 0.0 {
                Direction::Buy
            } else {
                Direction::Sell
            };
            let amount = prediction.unwrap() * 0.01;

            self.account
                .create_fake_order(
                    self.symbol.clone(),
                    amount,
                    direction,
                    self.client.fetch_ticker(&self.symbol).await?.bid,
                )
                .await?;

            phase = CyclePhase::Active;
            self.log_prediction(prediction.unwrap());
            self.last_candles = Some(candles);

            println!(
                "Balance (USDT): ${:.3}",
                self.account.get_balance_usdt(&self.client).await?.unwrap()
            );

            if self.config.prints.cycle.accuracy {
                self.print_accuracy(counter_tx).await;
            }
        }
    }

    pub async fn run_backtest(
        mut self,
        model_tx: &mpsc::Sender<ModelCommand>,
        chain_tx: Option<&mpsc::Sender<ChainCommand>>,
    ) -> Result<(), CycleError> {
        if self.get_client().test_symbol(&self.symbol).await.is_err() {
            return Err(CycleError::SymbolDoesNotExist);
        }

        println!(
            "{} {}Backtest has started!",
            self.print_symbol,
            Fore::Yellow.as_str()
        );

        let mut volatility: f64;
        let mut prediction: Option<f64> = None;

        let all_candles = self
            .get_client()
            .fetch_ohlcv_with_timestamp(
                &self.symbol,
                &self.config.exchange.timeframes.main_timeframe,
                1000,
            )
            .await?;

        let mut phase = CyclePhase::Warmup;

        let total = (all_candles.len() - 1 - OHLCV_FETCH_LEN) as u64;

        let mut threshold_counter: SymbolCounters<u8> = SymbolCounters::new(total as usize);

        let pb = ProgressBar::new(total);
        pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) ETA {eta_precise}"
            )
            .unwrap()
            .progress_chars("> "),
        );

        for i in OHLCV_FETCH_LEN..all_candles.len() - 1 {
            let window = &all_candles[i - OHLCV_FETCH_LEN..i];

            let to_volatility: Vec<Candle> = window[..10]
                .iter()
                .map(|candle| candle.to_candle())
                .collect();

            volatility = get_volatility(&to_volatility);

            let candles = DataMap::from_slice(
                Some(&self.symbol),
                &self.config.exchange.timeframes.main_timeframe,
                window,
            );

            match phase {
                CyclePhase::Active => {
                    let ohlcv = window[..OHLCV_LEN]
                        .iter()
                        .map(|candle| candle.to_candle())
                        .collect::<Vec<Candle>>();

                    let targets = DataMap::new(
                        Some(self.get_symbol().to_string()),
                        collect_targets(ohlcv.clone()[..OHLCV_LEN].try_into().unwrap()),
                    );

                    let target = targets.get("position_size").unwrap();

                    let ratio = if target != &0.0 {
                        (prediction.unwrap() - target).abs() / (target).abs()
                    } else {
                        0.0
                    };

                    let success: bool =
                        ratio < (self.config.behaviour.success_threshold * 100.0 * volatility);

                    let threshold_value: u8 = success.into();
                    threshold_counter.push(threshold_value);

                    if !success && self.config.runtime.with_training {
                        let summary_data = {
                            let last_candles = self.last_candles.clone().unwrap();

                            let (tx, rx) = oneshot::channel();
                            let _ = model_tx
                                .send(ModelCommand::GetAccuracy { respond_to: tx })
                                .await;
                            let accuracy = rx.await.map_err(|e| anyhow!(e))?;

                            if let Some(acc) = accuracy {
                                last_candles.clone() + acc.clone() + targets.clone()
                            } else {
                                last_candles.clone() + targets.clone()
                            }
                        };

                        if self.config.runtime.with_saves {
                            SQLStandart::Dummy
                                .insert_row(&self.pool, summary_data)
                                .await?;
                        }
                        let shifted_acc = threshold_counter.get_shifted_accuracy(3);
                        if shifted_acc.unwrap_or(0.0) == 0.0 {
                            let (tx, rx) = oneshot::channel();
                            let last_predictions = self.last_predictions.clone().unwrap();
                            let _ = model_tx
                                .send(ModelCommand::HandleMistakes {
                                    true_data: targets.clone(),
                                    predicted_data: last_predictions,
                                    respond_to: tx,
                                })
                                .await;
                            let _ = rx.await;
                        }

                        if let Some(ctx) = chain_tx {
                            let (tx, rx) = oneshot::channel();
                            let _ = ctx
                                .send(ChainCommand::AddBlock {
                                    symbol: self.get_symbol().to_string(),
                                    block: Block::new(
                                        self.last_predictions.clone().unwrap().to_hashmap(),
                                        targets.clone().to_hashmap(),
                                        *ohlcv.last().unwrap(),
                                    ),
                                    respond_to: tx,
                                })
                                .await;
                            let _ = rx.await;
                        }
                    }
                }
                CyclePhase::Warmup => {
                    self.start_balance = Some(
                        self.account
                            .get_fake_balance_usdt(window.last().unwrap().open)
                            .await?
                            .unwrap(),
                    );
                    println!(
                        "{} {}Start balance (USDT): ${:.3}\n",
                        self.print_symbol,
                        Fore::Green.as_str(),
                        self.start_balance.unwrap()
                    );
                }
            }

            let candles_to_pred = candles.clone();
            prediction = Some(self.predict(candles_to_pred, model_tx).await?);

            let direction = if prediction.unwrap() > 0.0 {
                Direction::Buy
            } else {
                Direction::Sell
            };
            let amount = prediction.unwrap() * 0.01;

            let order_price = window.last().unwrap().open;

            let _ = self
                .account
                .create_fake_order(self.symbol.clone(), amount, direction, order_price)
                .await;

            phase = CyclePhase::Active;
            self.last_candles = Some(candles);
            self.last_order_price = Some(order_price);
            pb.inc(1);
        }

        pb.finish_with_message(format!(
            "{} {}Backtest has finished!",
            self.print_symbol,
            Fore::Green.as_str()
        ));

        if let Some(ctx) = chain_tx {
            let (tx, rx) = oneshot::channel();
            ctx.send(ChainCommand::SavePlots {
                symbol: self.symbol.clone(),
                respond_to: tx,
            })
            .await
            .map_err(|e| CycleError::AnyhowError(anyhow!(e)))?;
            let _ = rx.await;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        let end_balance = self
            .account
            .get_fake_balance_usdt(self.last_order_price.unwrap())
            .await?
            .unwrap();

        let percent =
            ((end_balance - self.start_balance.unwrap()) / self.start_balance.unwrap()) * 100.0;

        let fore = if percent > 0.0 {
            Fore::Green.as_str()
        } else {
            Fore::Red.as_str()
        };

        println!(
            "\n\n{} {}End balance (USDT): ${:.3}",
            self.print_symbol, fore, end_balance
        );

        println!(
            "{} {}Model earned: {:.5}%",
            self.print_symbol, fore, percent
        );

        Ok(())
    }
}
