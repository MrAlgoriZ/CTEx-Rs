use chrono::{Local, Timelike};
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::{sync::Mutex as tokio_mutex, task::spawn_blocking, time::sleep};

use crate::data::data_interfaces::{FlattenedData, ICandle};
use crate::data::process::data_collection::{CollectedData, collect_all, flat_all};
use crate::data::process::target::{process_target, restore_price};
use crate::data::process::volatility::get_volatility;
use crate::data::requests::ccxt::binance::BinanceClient;
use crate::data::requests::database::db_req::{insert_candle, select_all_candles};
use crate::engine::state::counters::*;
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::engine::utils::config::load_env::load_env;
use crate::models::model::RFInterface;

pub struct TrainingCycle {
    pub symbol: String,
    last_grouped_candles: Option<CollectedData>,
    last_candles_target: Option<f64>,
    print_symbol: String,
    client: BinanceClient,
    config: Config,
    pool: PgPool,
}

impl TrainingCycle {
    pub async fn new(symbol: String) -> Self {
        TrainingCycle {
            print_symbol: format!("{}{}:", Fore::BLUE.as_str(), symbol),
            symbol: symbol,
            last_grouped_candles: None,
            last_candles_target: None,
            client: BinanceClient::new().await,
            config: load_config("config/config.yaml"),
            pool: PgPool::connect(&load_env()[0])
                .await
                .expect("Database connection failed"),
        }
    }

    pub async fn run(
        &mut self,
        model: &Arc<Mutex<RFInterface>>,
        counters: Arc<tokio_mutex<Counters>>,
    ) {
        if self.config.prints.volatility {
            let candles1d_to_vol: Vec<ICandle> =
                self.client.fetch_ohlcv(&self.symbol, "1d", 10).await;
            self.print_volatility_status(&candles1d_to_vol);
        }

        let mut target_indicate: Option<bool> = None;
        let mut prediction: Option<f64> = None;
        let counters_to_loop: Arc<tokio_mutex<Counters>> = counters.clone();

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

                if self.config.prints.target {
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
                    counters_to_loop.clone(),
                )
                .await
                .unwrap();
                if !success {
                    let last_grouped: CollectedData =
                        self.last_grouped_candles.as_ref().unwrap().clone();
                    self.handle_mistake(
                        spawn_blocking(move || flat_all(last_grouped, target))
                            .await
                            .unwrap(),
                        counters_to_loop.clone(),
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

            if self.config.prints.accuracy {
                self.print_accuracy(counters_to_loop.clone()).await;
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

        if self.config.prints.cycle_start {
            println!("{}{} Цикл запустился", self.print_time(), self.print_symbol);
        }
    }

    async fn predict(
        &self,
        flattened_candles: FlattenedData,
        model: &Arc<Mutex<RFInterface>>,
    ) -> Result<f64, String> {
        if !flattened_candles.is_there_a_target() {
            let model_clone: Arc<Mutex<RFInterface>> = model.clone();
            let features: Vec<f64> = flattened_candles.features;
            let token: String = flattened_candles.token;
            let pred: f64 = spawn_blocking(move || {
                let model_guard = model_clone.lock().unwrap();
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
        counters: Arc<tokio_mutex<Counters>>,
    ) -> Result<(), ()> {
        let diff: f64 = (prediction - target).abs();
        let success_threshold: f64 = self.config.data.success_threshold;
        let mut mut_counters = counters.lock().await;

        if diff < success_threshold {
            mut_counters.total.data.push_back(1);
            mut_counters.get_mut(&self.symbol).data.push_back(1);
        }

        Ok(())
    }

    async fn handle_mistake(
        &self,
        flattened_candles: FlattenedData,
        counters: Arc<tokio_mutex<Counters>>,
        model: &Arc<Mutex<RFInterface>>,
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

            let mut mut_counters = counters.lock().await;
            mut_counters.total.data.push_back(0);
            mut_counters.get_mut(&self.symbol).data.push_back(0);
            let check: u16 = mut_counters.total.data.iter().map(|&v| v as u16).sum();
            if check != 0 && check % 10 == 0 {
                self.train_model(pool, model).await
            }
            Ok(())
        } else {
            Err(())
        }
    }

    fn log_prediction(&self, prediction: f64, price: f64) {
        if self.config.prints.prediction {
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

        if self.config.prints.price {
            println!(
                "{}{} Предполагаемая цена: {:.7}",
                self.print_time(),
                self.print_symbol,
                price
            );
        }
    }

    async fn print_accuracy(&self, counters: Arc<tokio_mutex<Counters>>) {
        let mut mut_counters = counters.lock().await;
        if !mut_counters.total.data.is_empty()
            && !mut_counters.get_mut(&self.symbol).data.is_empty()
        {
            let local_acc = mut_counters.get_mut(&self.symbol).get_accuracy();
            let global_acc = mut_counters.total.get_accuracy();

            println!(
                "{}{} {}L ACC {:.2}% | G ACC {:.2}%",
                self.print_time(),
                self.print_symbol,
                Fore::WHITE.as_str(),
                local_acc,
                global_acc
            );

            if mut_counters.total.data.len() >= 96 {
                let day_local_acc = mut_counters
                    .get_mut(&self.symbol)
                    .get_shifted_accuracy(96)
                    .unwrap_or(0.0);
                let day_global_acc = mut_counters.total.get_shifted_accuracy(96).unwrap_or(0.0);
                println!(
                    "\n{}{} {}DAY L ACC {:.2}% | DAY G ACC {:.2}%",
                    self.print_time(),
                    self.print_symbol,
                    Fore::WHITE.as_str(),
                    day_local_acc,
                    day_global_acc
                );
            }
        }
    }

    async fn train_model(&self, pool: &PgPool, model: &Arc<Mutex<RFInterface>>) {
        let data = select_all_candles(pool).await.unwrap();
        let model_clone = model.clone();
        spawn_blocking(move || {
            let mut model_guard = model_clone.lock().unwrap();
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
