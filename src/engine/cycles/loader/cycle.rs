use chrono::{Local, Timelike};
use sqlx::PgPool;
use std::time::Duration;
use tokio::{task::spawn_blocking, time::sleep};

use crate::data::data_interfaces::{CandlesTarget, ICandle};
use crate::data::process::data_collection::{CollectedData, collect_all, flat_all};
use crate::data::process::target::process_target;
use crate::data::process::volatility::get_volatility;
use crate::data::requests::ccxt::binance::BinanceClient;
use crate::data::requests::database::db_req::insert_candle;
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::engine::utils::config::load_env::load_env;

pub struct LoaderCycle {
    pub symbol: String,
    last_grouped_candles: Option<CollectedData>,
    last_candles_target: Option<CandlesTarget>,
    config: Config,
    print_symbol: String,
    client: BinanceClient,
    pool: PgPool,
}

impl LoaderCycle {
    pub async fn new(symbol: String) -> Self {
        LoaderCycle {
            print_symbol: format!("{}{}:", Fore::BLUE.as_str(), symbol),
            symbol: symbol,
            last_grouped_candles: None,
            last_candles_target: None,
            config: load_config("config/config.yaml"),
            client: BinanceClient::new().await,
            pool: PgPool::connect(&load_env()[0])
                .await
                .expect("Database connection failed"),
        }
    }

    pub async fn run(&mut self) {
        if self.config.prints.volatility {
            let candles1d_to_vol: Vec<ICandle> =
                self.client.fetch_ohlcv(&self.symbol, "1d", 10).await;
            self.print_volatility_status(&candles1d_to_vol);
        }
        let mut target_indicate: Option<bool> = None;

        loop {
            self.wait_for_next_interval().await;
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
                let target =
                    process_target(self.last_candles_target.as_ref().unwrap(), &candles_target);

                if self.config.prints.target {
                    println!(
                        "{}[{}] {} {}Target: {:.5}",
                        Fore::WHITE.as_str(),
                        Local::now().format("%H:%M:%S"),
                        self.print_symbol,
                        Fore::WHITE.as_str(),
                        target.unwrap(),
                    );
                }

                let last_grouped = self.last_grouped_candles.as_ref().unwrap().clone();
                let flatten = spawn_blocking(move || flat_all(last_grouped, target))
                    .await
                    .unwrap();
                insert_candle(
                    &self.pool,
                    &self.symbol,
                    &flatten
                        .features
                        .try_into()
                        .expect("flattened candles len parse failed"),
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
    fn print_volatility_status(&self, candles: &[ICandle]) {
        let volatility = get_volatility(candles);
        println!(
            "{}[{}] {}Волатильность на токене {} составляет {:.5}",
            Fore::WHITE.as_str(),
            Local::now().format("%H:%M:%S"),
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
            println!(
                "{}[{}] {} Цикл запустился",
                Fore::WHITE.as_str(),
                Local::now().format("%H:%M:%S"),
                self.print_symbol,
            );
        }
    }
}
