use sqlx::PgPool;
use tokio::task::spawn_blocking;

use crate::data::data_interfaces::ICandle;
use crate::data::process::data_collection::{CollectedData, collect_all, flat_all};
use crate::data::process::target::process_target;
use crate::data::requests::ccxt::binance::BinanceClient;
use crate::data::requests::database::db_req::insert_candle;
use crate::engine::cycles::cycle_traits::{Cycle, CycleGetters};
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::engine::utils::config::load_env::load_env;

pub struct LoaderCycle {
    pub symbol: String,
    last_grouped_candles: Option<CollectedData>,
    last_candles_target: Option<f64>,
    config: Config,
    print_symbol: String,
    client: BinanceClient,
    pool: PgPool,
}

impl CycleGetters for LoaderCycle {
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

impl Cycle for LoaderCycle {}

impl LoaderCycle {
    pub async fn new(symbol: String) -> Self {
        LoaderCycle {
            print_symbol: format!("{}{}:", Fore::BLUE.as_str(), symbol),
            symbol: symbol,
            last_grouped_candles: None,
            last_candles_target: None,
            config: load_config("config/config.yaml"),
            client: BinanceClient::new().await,
            pool: PgPool::connect(&load_env().database_url)
                .await
                .expect("Database connection failed"),
        }
    }

    pub async fn run(&mut self) {
        if !self.client.test_token(&self.symbol).await.is_ok() {
            return;
        }
        if self.config.prints.cycle.volatility {
            let candles1d_to_vol: Vec<ICandle> =
                self.client.fetch_ohlcv(&self.symbol, "1d", 10).await;
            self.print_volatility_status(&candles1d_to_vol);
        }

        let mut target_indicate: Option<bool> = None;

        loop {
            self.wait_for_next_interval().await;
            let candles: CollectedData = collect_all(&self.symbol).await;
            let candles_target: f64 =
                self.client.fetch_ohlcv(&self.symbol, "15m", 2).await[0].close;

            if target_indicate == Some(true) {
                let target: Option<f64> =
                    process_target(self.last_candles_target.unwrap(), candles_target);

                if self.config.prints.cycle.target {
                    println!(
                        "{}{} {}Target: {:.5}",
                        self.print_time(),
                        self.print_symbol,
                        Fore::WHITE.as_str(),
                        target.unwrap(),
                    );
                }

                let last_grouped: CollectedData =
                    self.last_grouped_candles.as_ref().unwrap().clone();

                self.save_data(
                    spawn_blocking(move || flat_all(last_grouped, target))
                        .await
                        .unwrap(),
                    &self.pool,
                )
                .await
                .unwrap();
            }

            target_indicate = Some(true);
            self.last_grouped_candles = Some(candles);
            self.last_candles_target = Some(candles_target);
        }
    }

    // --- Методы ---
    async fn save_data(
        &self,
        flattened_candles: crate::data::data_interfaces::FlattenedData,
        pool: &PgPool,
    ) -> Result<(), ()> {
        if flattened_candles.is_there_a_target() {
            insert_candle(
                pool,
                &self.symbol,
                &flattened_candles
                    .features
                    .try_into()
                    .expect("flattened candles len parse failed"),
            )
            .await
            .unwrap();
            Ok(())
        } else {
            Err(())
        }
    }
}
