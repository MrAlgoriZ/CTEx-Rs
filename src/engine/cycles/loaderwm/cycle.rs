use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use crate::data::data_interfaces::{Candle, DataMap};
use crate::data::process::data_collection::{OHLCV_FETCH_LEN, OHLCV_LEN, collect_targets};
use crate::data::process::volatility::get_volatility;
use crate::data::requests::ccxt::client::CCXTClient;
use crate::data::requests::database::standart::SQLStandart;
use crate::engine::cycles::CyclePhase;
use crate::engine::cycles::manager::{CounterCommand, CycleError, ModelCommand};
use crate::engine::cycles::traits::{
    Cycle, CycleGetters, CycleGettersForCycleWithModel, CycleWithModel,
};
use crate::engine::state::counters::SymbolCounters;
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::engine::utils::config::load_env::load_env;

pub struct LoaderWMCycle {
    pub symbol: String,
    last_candles: Option<DataMap>,
    last_predictions: Option<DataMap>,
    print_symbol: String,
    client: CCXTClient,
    config: Config,
    pool: PgPool,
}

impl CycleGetters for LoaderWMCycle {
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

impl CycleGettersForCycleWithModel for LoaderWMCycle {
    fn get_pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
    fn change_last_predictions(&mut self, predictions: DataMap) {
        self.last_predictions = Some(predictions);
    }
}

impl Cycle for LoaderWMCycle {}
impl CycleWithModel for LoaderWMCycle {}

impl LoaderWMCycle {
    fn new(symbol: String, client: CCXTClient, pool: PgPool) -> Self {
        LoaderWMCycle {
            print_symbol: format!("{}{}:", Fore::BLUE.as_str(), symbol),
            symbol: symbol,
            last_candles: None,
            last_predictions: None,
            config: load_config("config/config.yaml"),
            client,
            pool,
        }
    }

    pub async fn init(symbol: String, client: CCXTClient) -> Result<Self, anyhow::Error> {
        let pool = PgPool::connect(&load_env().database_url).await?;
        Ok(Self::new(symbol, client, pool))
    }

    pub async fn run(
        mut self,
        counter_tx: &mpsc::Sender<CounterCommand>,
        model_tx: Option<&mpsc::Sender<ModelCommand>>,
    ) -> Result<(), CycleError> {
        if !self.client.test_symbol(&self.symbol).await.is_ok() {
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
                .client
                .collect_all(&self.symbol, &self.config.timeframes.main_timeframe)
                .await?;

            match phase {
                CyclePhase::Active => {
                    let targets = DataMap::new(
                        self.get_symbol().to_string(),
                        collect_targets(ohlcv[..OHLCV_LEN].try_into().unwrap()),
                    );

                    let target = targets.get("position_size").unwrap();
                    let ratio = if target != &0.0 {
                        (prediction.unwrap() - target).abs() / (target).abs()
                    } else {
                        0.0
                    };

                    if self.config.prints.cycle.target {
                        println!(
                            "{}{} {}Pred: {:.5} | Target: {:.5} | Ratio {:.5}",
                            self.print_time(),
                            self.print_symbol,
                            Fore::WHITE.as_str(),
                            prediction.unwrap(),
                            target,
                            ratio
                        );
                    }

                    self.update_counters(
                        prediction.unwrap(),
                        target.clone(),
                        volatility,
                        counter_tx,
                    )
                    .await;

                    let last_candles = self.last_candles.clone().unwrap();
                    let last_predictions = self.last_predictions.clone().unwrap();

                    let accuracy = if let Some(mtx) = model_tx {
                        let (tx, rx) = oneshot::channel();
                        let _ = mtx.send(ModelCommand::GetAccuracy { respond_to: tx }).await;
                        rx.await.map_err(|e| anyhow::anyhow!(e))?
                    } else {
                        todo!()
                    };
                    let summary_data = {
                        if let Some(acc) = accuracy {
                            last_candles + acc + targets
                        } else {
                            last_candles + targets
                        }
                    };

                    self.handle_mistake(
                        summary_data, // Внутри функции сохраняется всё, но модель сравнивает только targets
                        last_predictions,
                        counter_tx,
                        model_tx,
                    )
                    .await?;
                }
                _ => {}
            }

            let candles_to_pred = candles.clone();

            prediction = if let Some(mtx) = model_tx {
                Some(self.predict(candles_to_pred, &mtx).await.unwrap())
            } else {
                None
            };
            phase = CyclePhase::Active;
            if let Some(pred) = prediction {
                self.log_prediction(pred);
            }
            self.last_candles = Some(candles);

            if self.config.prints.cycle.accuracy {
                self.print_accuracy(counter_tx).await;
            }
        }
    }

    pub async fn run_backtest(
        mut self,
        model_tx: Option<&mpsc::Sender<ModelCommand>>,
    ) -> Result<(), CycleError> {
        if !self.client.test_symbol(&self.symbol).await.is_ok() {
            return Err(CycleError::SymbolDoesNotExist);
        }

        println!(
            "{}{} {}Бектест начался!\n",
            self.print_time(),
            self.print_symbol,
            Fore::YELLOW.as_str()
        );

        let mut volatility: f64;
        let mut prediction: Option<f64> = None;

        let all_candles = self
            .client
            .fetch_ohlcv_with_timestamp(&self.symbol, &self.config.timeframes.main_timeframe, 1000)
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

            let candles =
                DataMap::from_slice(&self.symbol, &self.config.timeframes.main_timeframe, window);

            match phase {
                CyclePhase::Active => {
                    let last_candles = self.last_candles.clone().unwrap();

                    let accuracy = if let Some(mtx) = model_tx {
                        let (tx, rx) = oneshot::channel();
                        let _ = mtx.send(ModelCommand::GetAccuracy { respond_to: tx }).await;
                        rx.await.map_err(|e| anyhow::anyhow!(e))?
                    } else {
                        Some(DataMap::generate_accuracy())
                    };
                    let ohlcv = window[..OHLCV_LEN]
                        .iter()
                        .map(|candle| candle.to_candle())
                        .collect::<Vec<Candle>>();

                    let targets = DataMap::new(
                        self.get_symbol().to_string(),
                        collect_targets(ohlcv[..OHLCV_LEN].try_into().unwrap()),
                    );

                    let target = targets.get("position_size").unwrap();
                    if let Some(pred) = prediction {
                        let ratio = if target != &0.0 {
                            (pred - target).abs() / (target).abs()
                        } else {
                            0.0
                        };
                        debug!("{}", ratio);
                        let success: bool =
                            ratio < (self.config.behaviour.success_threshold * 100.0 * volatility);

                        let threshold_value: u8 = success.into();
                        threshold_counter.push(threshold_value);
                    }
                    let summary_data = {
                        let mut base = last_candles.clone() + targets.clone();
                        if let Some(acc) = accuracy {
                            base = base + acc;
                        }
                        if let Some(preds) = self.last_predictions.clone() {
                            base = base
                                + DataMap::new(
                                    "".to_string(),
                                    preds
                                        .to_standart(&SQLStandart::ThirdLayer)
                                        .get_only_features(),
                                )
                        } else {
                            base = base + DataMap::generate_predictions(targets.clone())
                        }
                        base
                    };

                    if self.config.runtime.with_saves {
                        SQLStandart::Dummy
                            .insert_row(&self.pool, summary_data)
                            .await?;
                    }

                    if self.config.runtime.with_training {
                        if let Some(mtx) = model_tx {
                            let shifted_acc = threshold_counter.get_shifted_accuracy(3);
                            if shifted_acc.unwrap_or(0.0) == 0.0 {
                                let (tx, rx) = oneshot::channel();
                                let last_predictions = self.last_predictions.clone().unwrap();
                                let _ = mtx
                                    .send(ModelCommand::HandleMistakes {
                                        true_data: targets,
                                        predicted_data: last_predictions,
                                        respond_to: tx,
                                    })
                                    .await;
                                let _ = rx.await;
                            }
                        }
                    }
                }
                _ => {}
            }

            let candles_to_pred = candles.clone();

            prediction = if let Some(mtx) = model_tx {
                Some(self.predict(candles_to_pred, &mtx).await.unwrap())
            } else {
                None
            };
            // debug!("prediction: {:?}", prediction);

            phase = CyclePhase::Active;
            self.last_candles = Some(candles);
            pb.inc(1);
        }

        pb.finish_with_message(format!(
            "{}{} {}Бектест окончен!",
            self.print_time(),
            self.print_symbol,
            Fore::GREEN.as_str()
        ));

        tokio::time::sleep(Duration::from_secs(1)).await;

        println!(
            "\n{}{} {}Точность по threshold составляет: {:.3}%",
            self.print_time(),
            self.print_symbol,
            Fore::YELLOW.as_str(),
            threshold_counter.get_accuracy()
        );
        Ok(())
    }
}
