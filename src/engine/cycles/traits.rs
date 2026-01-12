use chrono::{Local, Timelike};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;

use crate::data::data_interfaces::{FlattenedData, ICandle};
use crate::data::process::volatility::get_volatility;
use crate::data::requests::ccxt::binance::BinanceClient;
use crate::data::requests::database::db_req::{insert_candle, select_all_candles};
use crate::engine::cycles::manager::{CounterCommand, CounterType, ModelCommand};
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::Config;

pub trait CycleGetters {
    fn get_symbol(&self) -> &str;
    fn get_print_symbol(&self) -> &str;
    fn get_config(&self) -> &Config;
    fn get_client(&self) -> &BinanceClient;
}

pub trait CycleGettersForCycleWithModel {
    fn get_pool(&self) -> &sqlx::PgPool;
}

pub trait Cycle: CycleGetters {
    fn print_volatility_status(&self, volatility: f64) {
        println!(
            "{}{}Волатильность на токене {} составляет {:.3}",
            self.print_time(),
            Fore::YELLOW.as_str(),
            self.get_symbol(),
            volatility
        );
    }

    async fn update_volatility(&self, volatility_obj: &mut f64) -> Result<(), String> {
        let candles: Vec<ICandle> = self
            .get_client()
            .fetch_ohlcv(self.get_symbol(), "1d", 10)
            .await?;
        *volatility_obj = get_volatility(&candles);
        Ok(())
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

        if self.get_config().prints.cycle.cycle_start {
            println!(
                "{}{} Цикл запустился",
                self.print_time(),
                self.get_print_symbol()
            );
        }
    }

    fn print_time(&self) -> String {
        format!(
            "{}[{}] ",
            Fore::WHITE.as_str(),
            Local::now().format("%H:%M:%S")
        )
    }
}

pub trait CycleWithModel: Cycle + CycleGettersForCycleWithModel {
    async fn predict(
        &self,
        flattened_candles: FlattenedData,
        model_tx: &mpsc::Sender<ModelCommand>,
    ) -> Result<f64, String> {
        if !flattened_candles.is_there_a_target() {
            let (tx, rx) = oneshot::channel();

            model_tx
                .send(ModelCommand::Predict {
                    flattenned_candles: flattened_candles,
                    respond_to: tx,
                })
                .await
                .unwrap();

            let pred = rx.await.unwrap();

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
        volatility: f64,
        counter_tx: &mpsc::Sender<CounterCommand>,
    ) {
        let diff: f64 = (prediction - target).abs();
        let success_threshold: f64 =
            self.get_config().behaviour.success_threshold.default * 100.0 * volatility;
        println!("{}", success_threshold);
        let threshold_value: u8 = (diff < success_threshold).into();
        let direction_value: u8 = {
            let target_direction = target > 0.0;
            let prediction_direction = prediction > 0.0;
            (target_direction == prediction_direction).into()
        };

        let _ = counter_tx
            .send(CounterCommand::Increment {
                symbol: self.get_symbol().to_uppercase(),
                counter_type: CounterType::Threshold,
                value: threshold_value,
            })
            .await;

        let _ = counter_tx
            .send(CounterCommand::Increment {
                symbol: self.get_symbol().to_uppercase(),
                counter_type: CounterType::Direction,
                value: direction_value,
            })
            .await;
    }

    async fn handle_mistake(
        &self,
        flattened_candles: FlattenedData,
        counter_tx: &mpsc::Sender<CounterCommand>,
        model_tx: &mpsc::Sender<ModelCommand>,
    ) -> Result<(), String> {
        if flattened_candles.is_there_a_target() {
            insert_candle(
                &self.get_pool(),
                &self.get_symbol(),
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
                    symbol: self.get_symbol().to_string(),
                    window: 3,
                    counter_type: CounterType::Threshold,
                    respond_to: tx,
                })
                .await;

            if let Ok(shifted_acc) = rx.await {
                if shifted_acc.unwrap_or(0.0) == 0.0 {
                    self.train_model(model_tx).await?;
                }
            }

            Ok(())
        } else {
            Err("В поданных данных нет target!".to_string())
        }
    }

    fn log_prediction(&self, prediction: f64, price: f64) {
        if self.get_config().prints.cycle.prediction {
            let str_prediction: String;
            if prediction > 0.0 {
                str_prediction = format!(
                    "{}Цена пойдет вверх на {:.5}%",
                    Fore::GREEN.as_str(),
                    prediction
                );
            } else {
                str_prediction = format!(
                    "{}Цена пойдет вниз на {:.5}%",
                    Fore::RED.as_str(),
                    prediction.abs()
                );
            }

            println!(
                "{}{} {}",
                self.print_time(),
                self.get_print_symbol(),
                str_prediction
            );
        }

        if self.get_config().prints.cycle.price {
            println!(
                "{}{} Предполагаемая цена: {:.7}",
                self.print_time(),
                self.get_print_symbol(),
                price
            );
        }
    }

    async fn print_accuracy(&self, counter_tx: &mpsc::Sender<CounterCommand>) {
        let (tx_local, rx_local) = oneshot::channel();
        let _ = counter_tx
            .send(CounterCommand::GetAccuracy {
                symbol: self.get_symbol().to_uppercase(),
                respond_to: tx_local,
                counter_type: CounterType::Threshold,
            })
            .await;

        let (tx_global, rx_global) = oneshot::channel();
        let _ = counter_tx
            .send(CounterCommand::GetTotalAccuracy {
                respond_to: tx_global,
                counter_type: CounterType::Threshold,
            })
            .await;

        if let (Ok(Some(local_acc)), Ok(global_acc)) = (rx_local.await, rx_global.await) {
            println!(
                "{}{} {}L ACC {:.2}% | G ACC {:.2}%",
                self.print_time(),
                self.get_print_symbol(),
                Fore::WHITE.as_str(),
                local_acc,
                global_acc
            );
        }
    }

    async fn train_model(&self, model_tx: &mpsc::Sender<ModelCommand>) -> Result<(), String> {
        let data = select_all_candles(self.get_pool()).await.unwrap();
        let (tx, rx) = oneshot::channel();

        model_tx
            .send(ModelCommand::Train {
                data,
                respond_to: tx,
            })
            .await
            .unwrap();

        rx.await.unwrap()?;
        Ok(())
    }
}

// TODO: pub trait CycleWithAccount
