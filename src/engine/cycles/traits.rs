use chrono::Utc;
// use log::debug;
use anyhow::anyhow;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;

use crate::data::data_interfaces::{Candle, DataMap, Timeframe};
use crate::data::process::volatility::get_volatility;
use crate::data::requests::ccxt::client::CCXTClient;
use crate::data::requests::database::standart::SQLStandart;
use crate::engine::cycles::manager::{CounterCommand, ModelCommand};
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::Config;

pub trait CycleGetters {
    fn get_symbol(&self) -> &str;
    fn get_print_symbol(&self) -> &str;
    fn get_config(&self) -> &Config;
    fn get_client(&self) -> &CCXTClient;
}

pub trait CycleGettersForCycleWithModel {
    fn get_pool(&self) -> &sqlx::PgPool;
    fn change_last_predictions(&mut self, predictions: DataMap);
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

    async fn update_volatility(&self, volatility_obj: &mut f64) -> Result<(), anyhow::Error> {
        let candles: Vec<Candle> = self
            .get_client()
            .fetch_ohlcv(
                self.get_symbol(),
                &self.get_config().exchange.timeframes.main_timeframe,
                10,
            )
            .await?;
        *volatility_obj = get_volatility(&candles);
        Ok(())
    }

    async fn wait_for_next_interval(&self) -> Result<(), anyhow::Error> {
        let timeframe = Timeframe::from_str(&self.get_config().exchange.timeframes.main_timeframe)
            .expect("Invalid timeframe in config!");

        let now = Utc::now();

        match timeframe.seconds() {
            Some(interval) => {
                let now_ts = now.timestamp();
                let next_ts = (((now_ts as f64) / interval) + 1.0) * interval;
                let wait_secs = ((next_ts.round() as i64) - now_ts).max(0) as u64;

                if wait_secs > 0 {
                    sleep(Duration::from_secs(wait_secs)).await;
                }
            }

            None => {
                return Err(anyhow!("invalid timeframe in config"));
            }
        }

        sleep(Duration::from_secs(2)).await;

        if self.get_config().prints.cycle.cycle_start {
            println!(
                "{}{} Цикл запустился",
                self.print_time(),
                self.get_print_symbol()
            );
        }

        Ok(())
    }

    fn print_time(&self) -> String {
        format!(
            "{}[{}] ",
            Fore::WHITE.as_str(),
            Utc::now().format("%H:%M:%S")
        )
    }
}

pub trait CycleWithModel: Cycle + CycleGettersForCycleWithModel {
    async fn predict(
        &mut self,
        data: DataMap,
        model_tx: &mpsc::Sender<ModelCommand>,
    ) -> Result<f64, anyhow::Error> {
        if !data.has_target() {
            let (tx, rx) = oneshot::channel();

            model_tx
                .send(ModelCommand::Predict {
                    data,
                    respond_to: tx,
                })
                .await?;

            let pred = rx.await?;

            // debug!("pred: {:#?}", &pred);

            self.change_last_predictions(pred.clone());
            pred.get("position_size")
                .ok_or(anyhow!("Model must predict position size!"))
                .map(|v| *v)
        } else {
            Err(anyhow!(
                "FlattenedData to prediction should not have the target"
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
        let ratio = if target != 0.0 {
            (prediction - target).abs() / (target).abs()
        } else {
            100_000.0
        };

        let success_threshold: f64 =
            self.get_config().behaviour.success_threshold * 100.0 * volatility;

        let threshold_value: u8 = (ratio < success_threshold).into();

        let _ = counter_tx
            .send(CounterCommand::Increment {
                symbol: self.get_symbol().to_uppercase(),
                value: threshold_value,
            })
            .await;
    }

    async fn handle_mistake(
        &self,
        true_data: DataMap,
        predicted_data: DataMap,
        counter_tx: &mpsc::Sender<CounterCommand>,
        model_tx: Option<&mpsc::Sender<ModelCommand>>,
    ) -> Result<(), anyhow::Error> {
        if true_data.has_target() {
            SQLStandart::Dummy
                .insert_row(&self.get_pool(), true_data.clone())
                .await?;

            let (tx, rx) = oneshot::channel();
            let _ = counter_tx
                .send(CounterCommand::GetShiftedAccuracy {
                    symbol: self.get_symbol().to_string(),
                    window: 3,
                    respond_to: tx,
                })
                .await;

            if let Some(mtx) = model_tx {
                if let Ok(shifted_acc) = rx.await {
                    if shifted_acc.unwrap_or(0.0) == 0.0 {
                        let targets =
                            DataMap::new(true_data.symbol.clone(), true_data.get_only_targets());
                        let (tx, rx) = oneshot::channel();
                        let _ = mtx
                            .send(ModelCommand::HandleMistakes {
                                true_data: targets,
                                predicted_data,
                                respond_to: tx,
                            })
                            .await;
                        let _ = rx.await;
                    }
                }
            }
            Ok(())
        } else {
            Err(anyhow!("В поданных данных нет target!"))
        }
    }

    fn log_prediction(&self, prediction: f64) {
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
    }

    async fn print_accuracy(&self, counter_tx: &mpsc::Sender<CounterCommand>) {
        let (tx_local, rx_local) = oneshot::channel();
        let _ = counter_tx
            .send(CounterCommand::GetAccuracy {
                symbol: self.get_symbol().to_uppercase(),
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
                self.get_print_symbol(),
                Fore::WHITE.as_str(),
                local_acc,
                global_acc
            );
        }
    }
}
