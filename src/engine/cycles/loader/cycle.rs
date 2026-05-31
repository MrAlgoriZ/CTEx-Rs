use anyhow::anyhow;
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::PgPool;

use crate::data::data_interfaces::{Candle, CandleWithTimestamp, DataMap};
use crate::data::process::data_collection::{OHLCV_FETCH_LEN, OHLCV_LEN, collect_targets};
use crate::data::process::volatility::get_volatility;
use crate::data::requests::ccxt::client::CCXTClient;
use crate::data::requests::database::standart::SQLStandart;
use crate::engine::cycles::CyclePhase;
use crate::engine::cycles::manager::CycleError;
use crate::engine::cycles::traits::{Cycle, CycleGetters};
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::Config;
use crate::engine::utils::config::load_config::load_config;
use crate::engine::utils::config::load_env::load_env;

pub struct LoaderCycle {
    pub symbol: String,
    last_candles: Option<DataMap>,
    last_close: Option<f64>,
    config: Config,
    print_symbol: String,
    client: CCXTClient,
    pool: PgPool,
}

impl CycleGetters for LoaderCycle {
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

impl Cycle for LoaderCycle {}

impl LoaderCycle {
    fn new(symbol: String, client: CCXTClient, pool: PgPool) -> Self {
        LoaderCycle {
            print_symbol: format!("{}{}:", Fore::BLUE.as_str(), symbol),
            symbol: symbol,
            last_candles: None,
            last_close: None,
            config: load_config(),
            client,
            pool,
        }
    }

    pub async fn init(symbol: String, client: CCXTClient) -> Result<Self, anyhow::Error> {
        let pool = PgPool::connect(&load_env().database_url).await?;
        Ok(Self::new(symbol, client, pool))
    }

    pub async fn run(mut self) -> Result<(), CycleError> {
        if !self.client.test_symbol(&self.symbol).await.is_ok() {
            return Err(CycleError::SymbolDoesNotExist);
        }

        let volatility: f64 = {
            let candles: Vec<Candle> = self
                .client
                .fetch_ohlcv(
                    &self.symbol,
                    &self.config.exchange.timeframes.main_timeframe,
                    10,
                )
                .await?;
            get_volatility(&candles)
        };

        if self.config.prints.cycle.volatility {
            self.print_volatility_status(volatility);
        }

        let mut phase: CyclePhase = CyclePhase::Warmup;

        loop {
            self.wait_for_next_interval().await?;
            let (candles, ohlcv) = self
                .client
                .collect_all(
                    &self.symbol,
                    &self.config.exchange.timeframes.main_timeframe,
                )
                .await?;
            let close = if self.config.prints.cycle.target {
                Some(
                    self.client
                        .fetch_ohlcv(
                            &self.symbol,
                            &self.config.exchange.timeframes.main_timeframe,
                            2,
                        )
                        .await?[0]
                        .close,
                )
            } else {
                None
            };

            match phase {
                CyclePhase::Active => {
                    let last_candles = self.last_candles.clone().unwrap();
                    let targets = DataMap::new(
                        self.get_symbol().to_string(),
                        collect_targets(ohlcv[..OHLCV_LEN].try_into().unwrap()),
                    );

                    if self.config.prints.cycle.target {
                        let target = targets.get("position_size").unwrap();

                        println!(
                            "{}{} {}Position size: {:.5}",
                            self.print_time(),
                            self.print_symbol,
                            Fore::WHITE.as_str(),
                            target,
                        );
                    }

                    self.save_data(last_candles + targets, &self.pool)
                        .await
                        .unwrap();
                }
                _ => {}
            }

            phase = CyclePhase::Active;
            self.last_candles = Some(candles);
            self.last_close = close;
        }
    }

    pub async fn run_backtest(mut self) -> Result<(), CycleError> {
        if !self.client.test_symbol(&self.symbol).await.is_ok() {
            return Err(CycleError::SymbolDoesNotExist);
        }

        println!(
            "{}{} {}Бектест начался!\n",
            self.print_time(),
            self.print_symbol,
            Fore::YELLOW.as_str()
        );

        let all_candles: Vec<CandleWithTimestamp> = self
            .client
            .fetch_ohlcv_with_timestamp(
                &self.symbol,
                &self.config.exchange.timeframes.main_timeframe,
                1000,
            )
            .await?;

        let total = (all_candles.len() - 1 - OHLCV_FETCH_LEN) as u64;

        let pb = ProgressBar::new(total);
        pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) ETA {eta_precise}"
            )
            .unwrap()
            .progress_chars("> "),
        );

        let mut phase = CyclePhase::Warmup;

        for i in OHLCV_FETCH_LEN..all_candles.len() - 1 {
            let window = &all_candles[i - OHLCV_FETCH_LEN..i];
            let current_close = all_candles[i - 2].close;

            let candles = DataMap::from_slice(
                &self.symbol,
                &self.config.exchange.timeframes.main_timeframe,
                window,
            );

            match phase {
                CyclePhase::Active => {
                    let ohlcv = window[..OHLCV_LEN]
                        .iter()
                        .map(|candle| candle.to_candle())
                        .collect::<Vec<Candle>>();
                    let last_candles = self.last_candles.clone().unwrap();
                    let targets = DataMap::new(
                        self.get_symbol().to_string(),
                        collect_targets(ohlcv.try_into().unwrap()),
                    );

                    self.save_data(last_candles + targets, &self.pool).await?;
                }
                _ => {}
            }

            phase = CyclePhase::Active;
            self.last_candles = Some(candles);
            self.last_close = Some(current_close);
            pb.inc(1);
        }

        pb.finish_with_message(format!(
            "{}{} {}Бектест окончен!",
            self.print_time(),
            self.print_symbol,
            Fore::GREEN.as_str()
        ));
        println!("");

        Ok(())
    }

    // --- Методы ---
    async fn save_data(&self, data: DataMap, pool: &PgPool) -> Result<(), anyhow::Error> {
        if data.has_target() {
            SQLStandart::Dummy.insert_row(pool, data).await?;
            Ok(())
        } else {
            Err(anyhow!("DataMap must has the target!"))
        }
    }
}
