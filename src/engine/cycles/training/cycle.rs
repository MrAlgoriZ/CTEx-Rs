use sqlx::PgPool;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;

use crate::data::data_interfaces::{FlattenedData, ICandle};
use crate::data::process::data_collection::{CollectedData, collect_all, flat_all};
use crate::data::process::target::{process_target, restore_price};
use crate::data::requests::ccxt::binance::BinanceClient;
use crate::engine::cycles::cycle_traits::{
    Cycle, CycleGetters, CycleGettersForCycleWithModel, CycleWithModel,
};
use crate::engine::cycles::manager::CounterCommand;
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

impl CycleGetters for TrainingCycle {
    fn get_symbol(&self) -> &String {
        &self.symbol
    }

    fn get_print_symbol(&self) -> &String {
        &self.print_symbol
    }

    fn get_config(&self) -> Config {
        self.config.clone()
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
    pub async fn new(symbol: String) -> Self {
        TrainingCycle {
            print_symbol: format!("{}{}:", Fore::BLUE.as_str(), symbol),
            symbol: symbol,
            last_grouped_candles: None,
            last_candles_target: None,
            client: BinanceClient::new().await,
            config: load_config("config/config.yaml"),
            pool: PgPool::connect(&load_env().database_url)
                .await
                .expect("Database connection failed"),
        }
    }

    pub async fn run(
        &mut self,
        model: &Arc<StdMutex<RFInterface>>,
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
                let success: bool = diff < self.config.behaviour.success_threshold.default;

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
}
