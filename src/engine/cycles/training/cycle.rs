use indicatif::{ProgressBar, ProgressStyle};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::data::data_interfaces::{Candle, DataMap};
use crate::data::process::data_collection::{CollectedData, OHLCV_FETCH_LEN};
use crate::data::process::target::{process_target, restore_price};
use crate::data::process::volatility::get_volatility;
use crate::data::requests::ccxt::client::CCXTClient;
use crate::data::requests::database::consts::SQLStandart;
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

pub struct TrainingCycle {
    pub symbol: String,
    last_grouped_candles: Option<Arc<CollectedData>>,
    last_candles_target: Option<f64>,
    print_symbol: String,
    client: CCXTClient,
    config: Config,
    pool: PgPool,
}

impl CycleGetters for TrainingCycle {
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

impl CycleGettersForCycleWithModel for TrainingCycle {
    fn get_pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
}

impl Cycle for TrainingCycle {}
impl CycleWithModel for TrainingCycle {}

impl TrainingCycle {
    fn new(symbol: String, client: CCXTClient, pool: PgPool) -> Self {
        TrainingCycle {
            print_symbol: format!("{}{}:", Fore::BLUE.as_str(), symbol),
            symbol: symbol,
            last_grouped_candles: None,
            last_candles_target: None,
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
        model_tx: &mpsc::Sender<ModelCommand>,
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

            let candles = Arc::new(
                self.client
                    .collect_all(&self.symbol, &self.config.timeframes.main_timeframe)
                    .await?,
            );
            let candles_target: f64 = self
                .client
                .fetch_ohlcv(&self.symbol, &self.config.timeframes.main_timeframe, 2)
                .await?[0]
                .close;

            match phase {
                CyclePhase::Active => {
                    let target: Option<f64> =
                        process_target(self.last_candles_target.unwrap(), candles_target);

                    let diff: f64 = (prediction.unwrap() - target.unwrap()).abs();
                    let success: bool =
                        diff < (self.config.behaviour.success_threshold * 100.0 * volatility);

                    if self.config.prints.cycle.target {
                        println!(
                            "{}{} {}Pred: {:.5} | Target: {:.5} | Diff {:.5}",
                            self.print_time(),
                            self.print_symbol,
                            Fore::WHITE.as_str(),
                            prediction.unwrap(),
                            target.unwrap(),
                            diff
                        );
                    }

                    self.update_counters(
                        prediction.unwrap(),
                        target.unwrap(),
                        volatility,
                        counter_tx,
                    )
                    .await;

                    if !success {
                        let last_grouped = self.last_grouped_candles.clone().unwrap();
                        let data = DataMap::from_collected(last_grouped, target, None);
                        self.handle_mistake(data, counter_tx, model_tx).await?;
                    }
                }
                _ => {}
            }

            let candles_to_flattened = candles.clone();
            let data_for_pred = DataMap::from_collected(candles_to_flattened, None, None);

            prediction = Some(self.predict(data_for_pred, &model_tx).await.unwrap());
            let restored_price: f64 = restore_price(candles_target, prediction.unwrap());

            phase = CyclePhase::Active;
            self.log_prediction(prediction.unwrap(), restored_price);
            self.last_grouped_candles = Some(candles);
            self.last_candles_target = Some(candles_target);

            if self.config.prints.cycle.accuracy {
                self.print_accuracy(counter_tx).await;
            }
        }
    }

    pub async fn run_backtest(
        mut self,
        model_tx: &mpsc::Sender<ModelCommand>,
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
        let mut direction_counter: SymbolCounters<u8> = SymbolCounters::new(total as usize);

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

            let candles = match CollectedData::from_slice(
                &self.symbol,
                &self.config.timeframes.main_timeframe,
                window,
            ) {
                Some(collected) => Arc::new(collected),
                None => {
                    return Err(CycleError::AnyhowError(anyhow::anyhow!(
                        "Collection the data has been failed!"
                    )));
                }
            };
            let current_target = all_candles[i - 2].close;

            match phase {
                CyclePhase::Active => {
                    let target = process_target(self.last_candles_target.unwrap(), current_target);

                    let diff = (prediction.unwrap() - target.unwrap()).abs();
                    let success: bool =
                        diff < (self.config.behaviour.success_threshold * 100.0 * volatility);

                    let threshold_value: u8 = success.into();
                    let direction_value: u8 = {
                        let target_direction = target.unwrap() > 0.0;
                        let prediction_direction = prediction.unwrap() > 0.0;
                        (target_direction == prediction_direction).into()
                    };

                    threshold_counter.push(threshold_value);
                    direction_counter.push(direction_value);

                    if !success && self.config.runtime.with_training {
                        let last_grouped = self.last_grouped_candles.clone().unwrap();
                        let data = DataMap::from_collected(last_grouped, target, None);

                        if data.has_target() && self.config.runtime.with_saves {
                            SQLStandart::SingleModel
                                .insert_row(&self.pool, data)
                                .await?;
                        }
                        let shifted_acc = threshold_counter.get_shifted_accuracy(3);
                        if shifted_acc.unwrap_or(0.0) == 0.0 {
                            self.train_model(model_tx).await?;
                        }
                    }
                }
                _ => {}
            }

            let candles_to_flattened = candles.clone();
            let data_for_pred = DataMap::from_collected(candles_to_flattened, None, None);

            prediction = Some(self.predict(data_for_pred, &model_tx).await?);

            phase = CyclePhase::Active;
            self.last_grouped_candles = Some(candles);
            self.last_candles_target = Some(current_target);
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

        println!(
            "{}{} {}Точность по направлению составляет: {:.3}%",
            self.print_time(),
            self.print_symbol,
            Fore::YELLOW.as_str(),
            direction_counter.get_accuracy()
        );

        Ok(())
    }
}
