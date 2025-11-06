use chrono::{Local, Timelike};
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::{task::spawn_blocking, time::sleep};

use crate::data::data_interfaces::{CandlesTarget, FlattenedData, ICandle};
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

pub struct TradingCycle {
    pub symbol: String,
    last_grouped_candles: Option<CollectedData>,
    last_candles_target: Option<CandlesTarget>,
    print_symbol: String,
    client: BinanceClient,
    config: Config,
    pool: PgPool,
}

impl TradingCycle {
    pub async fn new(symbol: String) -> Self {
        TradingCycle {
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

    pub async fn run(&mut self, model: &Arc<Mutex<RFInterface>>) {
        let candles1d_to_vol: Vec<ICandle> = self.client.fetch_ohlcv(&self.symbol, "1d", 10).await;
        self.print_volatility_status(&candles1d_to_vol);

        let mut counters = Counters::new();

        let mut target_indicate: Option<bool> = None;
        let mut prediction: Option<f64> = None;

        loop {
            self.wait_for_next_interval().await;
            self.reset_counters_if_needed(&mut counters);
            let candles = collect_all(&self.symbol).await;
            let candles_target: CandlesTarget = CandlesTarget::new(
                self.client
                    .fetch_ohlcv(&self.symbol, "15m", 2)
                    .await
                    .try_into()
                    .unwrap(),
                self.client.fetch_day_price(&self.symbol).await,
            );

            if target_indicate == Some(true) {
                let (target, is_significant) =
                    process_target(self.last_candles_target.as_ref().unwrap(), &candles_target);

                let diff = (prediction.unwrap() - target.unwrap()).abs();
                let success = diff < self.config.data.success_threshold;

                println!(
                    "{} {}Pred: {:.5} | Target: {:.5} | Diff {:.5}",
                    self.print_symbol,
                    Fore::WHITE.as_str(),
                    prediction.unwrap(),
                    target.unwrap(),
                    diff
                );

                self.update_counters(prediction.unwrap(), target.unwrap(), &mut counters)
                    .unwrap();
                self.update_diff(&mut counters, diff);
                if !success {
                    let last_grouped = self.last_grouped_candles.as_ref().unwrap().clone();
                    self.handle_mistake(
                        spawn_blocking(move || flat_all(last_grouped, target, is_significant))
                            .await
                            .unwrap(),
                        &mut counters,
                        model,
                        &self.pool,
                    )
                    .await
                    .unwrap();
                }
            }

            let candles_to_flattened = candles.clone();
            let flattened_for_pred =
                spawn_blocking(move || flat_all(candles_to_flattened, None, None))
                    .await
                    .unwrap();

            prediction = Some(self.predict(flattened_for_pred, &model).await.unwrap());
            let restored_price = restore_price(&candles_target, prediction.unwrap());

            target_indicate = Some(true);
            self.log_prediction(prediction.unwrap(), restored_price);
            self.last_grouped_candles = Some(candles);
            self.last_candles_target = Some(candles_target);

            self.print_accuracy(&mut counters);
        }
    }

    // --- Методы ---
    fn print_volatility_status(&self, candles: &[ICandle]) {
        let volatility = get_volatility(candles);
        println!(
            "{}Волатильность на токене {} составляет {:.5}",
            Fore::YELLOW.as_str(),
            self.symbol,
            volatility
        );
    }

    fn reset_counters_if_needed(&self, counters: &mut Counters) {
        let week = (60 / 15) * 24 * 7;
        // if counters.total.common >= 1440 {
        //     counters.total.reset();
        // }
        if counters.get(&self.symbol).common >= week {
            counters.get(&self.symbol).reset();
        }
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

        println!(
            "{} Цикл запустился в {}",
            self.print_symbol,
            Local::now().format("%H:%M:%S")
        );
    }

    async fn predict(
        &self,
        flattened_candles: FlattenedData,
        model: &Arc<Mutex<RFInterface>>,
    ) -> Result<f64, String> {
        if !flattened_candles.is_there_a_target() {
            let model_clone = model.clone();
            let features = flattened_candles.features;
            let token = flattened_candles.token;
            let pred = spawn_blocking(move || {
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

    fn update_counters(
        &self,
        prediction: f64,
        target: f64,
        counters: &mut Counters,
    ) -> Result<(), ()> {
        let diff = (prediction - target).abs();
        let success_threshold = self.config.data.success_threshold;

        counters.total.common += 1;
        counters.get(&self.symbol).common += 1;

        if diff < success_threshold {
            counters.total.success += 1;
            counters.get(&self.symbol).success += 1;
        }

        Ok(())
    }

    fn check_significant(&self, target: f64) -> bool {
        ((target * 10.0).round() / 10.0) != 0.0
    }

    async fn handle_mistake(
        &self,
        flattened_candles: FlattenedData,
        counters: &mut Counters,
        model: &Arc<Mutex<RFInterface>>,
        pool: &PgPool,
    ) -> Result<(), ()> {
        let features_len = flattened_candles.features.len();
        if flattened_candles.is_there_a_target() {
            if self.check_significant(flattened_candles.features[(features_len - 1) - 2]) {
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
                counters.total.saved += 1;
                counters.get(&self.symbol).saved += 1;
                let check = counters.total.common - counters.total.success;
                if check != 0 && check % 10 == 0 {
                    self.train_model(pool, model).await
                }
                Ok(())
            } else {
                println!(
                    "{} Данные не сохранены, потому что они - мусор",
                    self.print_symbol
                );
                Ok(())
            }
        } else {
            Err(())
        }
    }

    fn log_prediction(&self, prediction: f64, price: f64) {
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
        println!("{} Предполагаемая цена: {:.7}", self.print_symbol, price);
        println!("{} {}", self.print_symbol, str_prediction);
    }

    fn print_accuracy(&self, counters: &mut Counters) {
        if counters.total.common != 0 && counters.get(&self.symbol).common != 0 {
            let local_acc = (counters.get(&self.symbol).success as f64
                / counters.get(&self.symbol).common as f64)
                * 100.0;
            let global_acc = (counters.total.success as f64 / counters.total.common as f64) * 100.0;

            println!(
                "{} {}L ACC {:.2}% | G ACC {:.2}%",
                self.print_symbol,
                Fore::WHITE.as_str(),
                local_acc,
                global_acc
            );
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

    fn update_diff(&self, counters: &mut Counters, diff: f64) {
        counters.total.diff += diff;
        counters.get(&self.symbol).diff += diff;
    }
}
