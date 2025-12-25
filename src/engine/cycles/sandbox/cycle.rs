use chrono::{Local, Timelike};
use sqlx::PgPool;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::{sync::Mutex as TokioMutex, task::spawn_blocking, time::sleep};

use crate::data::data_interfaces::{FlattenedData, ICandle};
use crate::data::process::data_collection::{CollectedData, collect_all, flat_all};
use crate::data::process::target::{process_target, restore_price};
use crate::data::process::volatility::get_volatility;
use crate::data::requests::ccxt::binance::BinanceClient;
use crate::data::requests::database::db_req::{insert_candle, select_all_candles};
use crate::engine::cycles::manager::CounterCommand;
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::engine::utils::config::load_env::load_env;
use crate::models::model::RFInterface;

pub struct SandboxCycle {
    pub symbol: String,
    last_grouped_candles: Option<CollectedData>,
    last_candles_target: Option<f64>,
    print_symbol: String,
    client: BinanceClient,
    config: Config,
    pool: PgPool,
    trading_mode: Option<TradingMode>,
    risk_engine: Option<RiskEngine>,
    feedback_engine: Option<FeedBackEngine>,
}

impl SandboxCycle {
    pub async fn new(symbol: String) -> Self {
        SandboxCycle {
            print_symbol: format!("{}{}:", Fore::BLUE.as_str(), symbol),
            symbol: symbol,
            last_grouped_candles: None,
            last_candles_target: None,
            client: BinanceClient::new().await,
            config: load_config("config/config.yaml"),
            pool: PgPool::connect(&load_env()[0])
                .await
                .expect("Database connection failed"),
            trading_mode: None,
            risk_engine: None,
            feedback_engine: None,
        }
    }

    pub async fn run(
        &mut self,
        model: &Arc<TokioMutex<RFInterface>>,
        counter_tx: &mpsc::Sender<CounterCommand>,
    ) {
        if !self.client.test_token(&self.symbol).await.is_ok() {
            return;
        }
        if self.config.prints.cycle.volatility {
            let candles1d_to_vol: Vec<ICandle> =
                self.client.fetch_ohlcv(&self.symbol, "1d", 10).await;
            self.print_volatility_status(&candles1d_to_vol);
        }

        let mut target_indicate: Option<bool> = None;
        let mut prediction: Option<f64> = None;

        loop {
            self.wait_for_next_interval().await;
            let candles: CollectedData = collect_all(&self.symbol).await;
            let candles_target: f64 =
                self.client.fetch_ohlcv(&self.symbol, "15m", 2).await[0].close;

            if target_indicate == Some(true) {
                let target: Option<f64> =
                    process_target(self.last_candles_target.unwrap(), candles_target);

                let diff: f64 = (prediction.unwrap() - target.unwrap()).abs();
                let success: bool = diff < self.config.data.success_threshold;

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

                self.update_counters(prediction.unwrap(), target.unwrap(), counter_tx)
                    .await
                    .unwrap();

                if !success {
                    let last_grouped: CollectedData =
                        self.last_grouped_candles.as_ref().unwrap().clone();
                    self.handle_mistake(
                        spawn_blocking(move || flat_all(last_grouped, target))
                            .await
                            .unwrap(),
                        counter_tx,
                        model,
                        &self.pool,
                    )
                    .await
                    .unwrap();
                }
            }

            let candles_to_flattened: CollectedData = candles.clone();
            let flattened_for_pred: FlattenedData =
                spawn_blocking(move || flat_all(candles_to_flattened, None))
                    .await
                    .unwrap();

            prediction = Some(self.predict(flattened_for_pred, &model).await.unwrap());
            let restored_price: f64 = restore_price(candles_target, prediction.unwrap());

            target_indicate = Some(true);
            self.log_prediction(prediction.unwrap(), restored_price);
            self.last_grouped_candles = Some(candles);
            self.last_candles_target = Some(candles_target);

            if self.config.prints.cycle.accuracy {
                self.print_accuracy(counter_tx).await;
            }
        }
    }

    // --- Методы ---
    fn print_volatility_status(&self, candles: &[ICandle]) {
        let volatility: f64 = get_volatility(candles);
        println!(
            "{}{}Волатильность на токене {} составляет {:.5}",
            self.print_time(),
            Fore::YELLOW.as_str(),
            self.symbol,
            volatility
        );
    }

    async fn wait_for_next_interval(&self) {
        let now = Local::now();

        let current_seconds = now.minute() as f64 * 60.0
            + now.second() as f64
            + now.nanosecond() as f64 / 1_000_000_000.0;

        let seconds_to_wait = (900.0 - (current_seconds % 900.0)) % 900.0;

        if seconds_to_wait > 0.0 {
            let duration = Duration::from_secs_f64(seconds_to_wait);
            sleep(duration).await;
        }

        sleep(Duration::from_secs(2)).await;

        if self.config.prints.cycle.cycle_start {
            println!("{}{} Цикл запустился", self.print_time(), self.print_symbol);
        }
    }

    async fn predict(
        &self,
        flattened_candles: FlattenedData,
        model: &Arc<TokioMutex<RFInterface>>,
    ) -> Result<f64, String> {
        if !flattened_candles.is_there_a_target() {
            let model_clone: Arc<TokioMutex<RFInterface>> = model.clone();
            let features: Vec<f64> = flattened_candles.features;
            let token: String = flattened_candles.token;
            let pred: f64 = spawn_blocking(move || {
                let model_guard = model_clone.blocking_lock();
                model_guard.predict(features, Some(&token)).unwrap_or(0.0)
            })
            .await
            .unwrap();
            Ok(pred)
        } else {
            Err(String::from(
                "FlattenedData to prediction should not have the target",
            ))
        }
    }

    async fn update_counters(
        &self,
        prediction: f64,
        target: f64,
        counter_tx: &mpsc::Sender<CounterCommand>,
    ) -> Result<(), ()> {
        let diff: f64 = (prediction - target).abs();
        let success_threshold: f64 = self.config.data.success_threshold;

        let value = if diff < success_threshold { 1 } else { 0 };

        let _ = counter_tx
            .send(CounterCommand::Increment {
                symbol: self.symbol.to_uppercase().clone(),
                value,
            })
            .await;

        Ok(())
    }

    async fn handle_mistake(
        &self,
        flattened_candles: FlattenedData,
        counter_tx: &mpsc::Sender<CounterCommand>,
        model: &Arc<TokioMutex<RFInterface>>,
        pool: &PgPool,
    ) -> Result<(), ()> {
        if flattened_candles.is_there_a_target() {
            insert_candle(
                &self.pool,
                &self.symbol,
                &flattened_candles
                    .features
                    .try_into()
                    .expect("flattened candles len parse failed"),
            )
            .await
            .unwrap();

            let (tx, rx) = oneshot::channel();
            let _ = counter_tx
                .send(CounterCommand::GetShiftedAccuracy {
                    symbol: self.symbol.clone(),
                    window: 2,
                    respond_to: tx,
                })
                .await;

            if let Ok(shifted_acc) = rx.await {
                if shifted_acc.unwrap_or(0.0) == 0.0 {
                    self.train_model(pool, model).await;
                }
            }

            Ok(())
        } else {
            Err(())
        }
    }

    fn log_prediction(&self, prediction: f64, price: f64) {
        if self.config.prints.cycle.prediction {
            let str_prediction: String;
            if prediction > 0.0 {
                str_prediction = format!(
                    "{}Цена пойдет вверх на {:.5}%",
                    Fore::GREEN.as_str(),
                    prediction * 100.0
                );
            } else {
                str_prediction = format!(
                    "{}Цена пойдет вниз на {:.5}%",
                    Fore::RED.as_str(),
                    prediction.abs() * 100.0
                );
            }

            println!(
                "{}{} {}",
                self.print_time(),
                self.print_symbol,
                str_prediction
            );
        }

        if self.config.prints.cycle.price {
            println!(
                "{}{} Предполагаемая цена: {:.7}",
                self.print_time(),
                self.print_symbol,
                price
            );
        }
    }

    async fn print_accuracy(&self, counter_tx: &mpsc::Sender<CounterCommand>) {
        let (tx_local, rx_local) = oneshot::channel();
        let _ = counter_tx
            .send(CounterCommand::GetAccuracy {
                symbol: self.symbol.to_uppercase().clone(),
                respond_to: tx_local,
            })
            .await;

        let (tx_global, rx_global) = oneshot::channel();
        let _ = counter_tx
            .send(CounterCommand::GetTotalAccuracy {
                respond_to: tx_global,
            })
            .await;

        if let (Ok(Some(local_acc)), Ok(global_acc)) = (rx_local.await, rx_global.await) {
            println!(
                "{}{} {}L ACC {:.2}% | G ACC {:.2}%",
                self.print_time(),
                self.print_symbol,
                Fore::WHITE.as_str(),
                local_acc,
                global_acc
            );
        }
    }

    async fn train_model(&self, pool: &PgPool, model: &Arc<TokioMutex<RFInterface>>) {
        let data = select_all_candles(pool).await.unwrap();
        let model_clone = model.clone();
        spawn_blocking(move || {
            let mut model_guard = model_clone.blocking_lock();
            model_guard
                .train(data)
                .expect("The model faced a problem with learning");
        })
        .await
        .unwrap();
    }

    fn print_time(&self) -> String {
        format!(
            "{}[{}] ",
            Fore::WHITE.as_str(),
            Local::now().format("%H:%M:%S")
        )
    }
}

const THRESHOLD_RATIO: f64 = 1.25; // TODO Добавить изменение в конфиге

enum TradingMode {
    Agressive,
    Conservative,
}

struct FeedBackEngine {
    last_diffs: VecDeque<f64>,
    success_threshold: f64,
    trading_mode: Option<TradingMode>,
    trading_mode_value: f64,
    config: Config,
}

impl FeedBackEngine {
    fn new(success_threshold: f64, config: Config) -> Self {
        Self {
            last_diffs: VecDeque::with_capacity(5), // TODO Добавить изменение в конфиге
            success_threshold,
            trading_mode: None,
            trading_mode_value: 0.0,
            config,
        }
    }

    fn update_last_diffs(&mut self, diff: f64) {
        self.last_diffs.push_back(diff);
        if self.last_diffs.len() > self.last_diffs.capacity() {
            self.last_diffs.pop_front();
        }
    }

    fn update_success_threshold(&mut self) {
        let mut diffs: Vec<f64> = self.last_diffs.iter().cloned().collect();
        diffs.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let trimmed = if diffs.len() >= 3 {
            &diffs[1..diffs.len() - 1]
        } else {
            &diffs
        };

        let avg = trimmed.iter().sum::<f64>() / trimmed.len() as f64;
        let mut new_threshold = avg * THRESHOLD_RATIO;
        if new_threshold > self.config.data.success_threshold * THRESHOLD_RATIO {
            // TODO Добавить минимальные и максимальные значения success threshold (сейчас есть только default)
            new_threshold = self.config.data.success_threshold * THRESHOLD_RATIO;
        } else if new_threshold < 0.3 {
            // Магическое число (минимальное значение success threshold)
            new_threshold = 0.3;
        }
        self.success_threshold = new_threshold;
    }

    fn update_trading_mode(&mut self, volatility: f64, risk_threshold: f64) {
        let volatility_factor = volatility.clamp(0.5, 2.0);
        let risk_factor = (1.0 / risk_threshold).clamp(0.5, 2.0);

        self.trading_mode_value =
            (self.trading_mode_value * 0.8 + risk_factor * 0.2) / volatility_factor;

        let trading_mode_new_value = self.trading_mode_value.clamp(0.0, 1.0);

        if trading_mode_new_value <= 0.5 {
            self.trading_mode = Some(TradingMode::Conservative);
        } else {
            self.trading_mode = Some(TradingMode::Agressive);
        }

        self.trading_mode_value = trading_mode_new_value;
    }
}

struct RiskEngine {
    risk_threshold: f64,
    symbol: &'static str,
}

impl RiskEngine {
    fn new(risk_threshold: f64, symbol: &'static str) -> Self {
        Self {
            risk_threshold,
            symbol,
        }
    }

    async fn update_risk(&mut self, volatility: f64, counter_tx: &mpsc::Sender<CounterCommand>) {
        let (tx_local, rx_local) = oneshot::channel();
        let _ = counter_tx
            .send(CounterCommand::GetAccuracy {
                symbol: self.symbol.to_uppercase().clone(),
                respond_to: tx_local,
            })
            .await;

        let (tx_global, rx_global) = oneshot::channel();
        let _ = counter_tx
            .send(CounterCommand::GetTotalAccuracy {
                respond_to: tx_global,
            })
            .await;

        if let (Ok(Some(local_acc)), Ok(global_acc)) = (rx_local.await, rx_global.await) {
            let accuracy = (global_acc.max(0.1) * 0.4) + (local_acc.max(0.1) * 0.6);

            let risk_modifier = (1.0 / accuracy).clamp(0.5, 2.0);

            self.risk_threshold = (volatility * risk_modifier * 0.4) + self.risk_threshold * 0.6;
        }
    }
}
