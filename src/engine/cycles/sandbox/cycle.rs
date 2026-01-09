use chrono::{Local, Timelike};
use sqlx::PgPool;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::{sync::Mutex, task::spawn_blocking, time::sleep};

use crate::data::data_interfaces::FlattenedData;
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
    risk_engine: RiskEngine,
    feedback_engine: FeedBackEngine,
    account: Arc<Mutex<DummyAccount>>,
}

impl SandboxCycle {
    pub async fn new(symbol: String, account: Arc<Mutex<DummyAccount>>) -> Self {
        SandboxCycle {
            print_symbol: format!("{}{}:", Fore::BLUE.as_str(), symbol),
            symbol: symbol.clone(),
            last_grouped_candles: None,
            last_candles_target: None,
            client: BinanceClient::new().await,
            config: load_config("config/config.yaml"),
            pool: PgPool::connect(&load_env()[0])
                .await
                .expect("Database connection failed"),
            risk_engine: RiskEngine::new(symbol, load_config("config/config.yaml")),
            feedback_engine: FeedBackEngine::new(load_config("config/config.yaml")),
            account,
        }
    }

    pub async fn run(
        &mut self,
        model: &Arc<Mutex<RFInterface>>,
        counter_tx: &mpsc::Sender<CounterCommand>,
    ) {
        if !self.client.test_token(&self.symbol).await.is_ok() {
            return;
        }

        let mut target_indicate: Option<bool> = None;
        let mut prediction: Option<f64> = None;

        loop {
            let candles1d_to_vol = self.client.fetch_ohlcv(&self.symbol, "1d", 10).await;
            let volatility: f64 = get_volatility(&candles1d_to_vol);

            if self.config.prints.cycle.volatility && target_indicate == None {
                self.print_volatility_status(volatility);
            }

            self.wait_for_next_interval().await;
            let candles: CollectedData = collect_all(&self.symbol).await;
            let candles_target: f64 =
                self.client.fetch_ohlcv(&self.symbol, "15m", 2).await[0].close;

            if target_indicate == Some(true) {
                let target: Option<f64> =
                    process_target(self.last_candles_target.unwrap(), candles_target);

                let diff: f64 = (prediction.unwrap() - target.unwrap()).abs();
                let success: bool = diff < self.feedback_engine.success_threshold;

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

                self.feedback_engine.update_last_diffs(diff);
                self.feedback_engine.update_success_threshold();

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

                self.risk_engine.update_risk(volatility, counter_tx).await;

                self.feedback_engine
                    .update_trading_mode(volatility, self.risk_engine.risk_threshold);
                println!(
                    "Риски {:.3}, trading_mode: {:?}, tdv: {}",
                    self.risk_engine.risk_threshold,
                    self.feedback_engine.trading_mode.clone().unwrap(),
                    self.feedback_engine.trading_mode_value
                );
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

            let choice = self
                .generate_choice(prediction.unwrap(), volatility, counter_tx)
                .await;
            match choice {
                TradingChoice::Buy(amount) => {
                    self.account
                        .lock()
                        .await
                        .buy(
                            &self.symbol,
                            amount * self.account.lock().await.get_balance(),
                            &self.client,
                        )
                        .await
                }
                TradingChoice::Sell(amount) => {
                    self.account
                        .lock()
                        .await
                        .sell(
                            &self.symbol,
                            amount * self.account.lock().await.get_token_balance(&self.symbol),
                            &self.client,
                        )
                        .await
                }
                TradingChoice::DoNothing => {}
            }

            self.print_account_balance().await;
        }
    }

    // --- Методы ---
    fn print_volatility_status(&self, volatility: f64) {
        println!(
            "{}{}Волатильность на токене {} составляет {:.3}",
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
        model: &Arc<Mutex<RFInterface>>,
    ) -> Result<f64, String> {
        if !flattened_candles.is_there_a_target() {
            let model_clone: Arc<Mutex<RFInterface>> = model.clone();
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
        let success_threshold: f64 = self.config.behaviour.success_threshold.default;

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

    async fn train_model(&self, pool: &PgPool, model: &Arc<Mutex<RFInterface>>) {
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

    async fn generate_choice(
        &self,
        prediction: f64,
        volatility: f64,
        counter_tx: &mpsc::Sender<CounterCommand>,
    ) -> TradingChoice {
        let mut diffs: Vec<f64> = self.feedback_engine.last_diffs.iter().cloned().collect();
        diffs.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let trimmed = if diffs.len() >= 3 {
            &diffs[1..diffs.len() - 1]
        } else {
            &diffs
        };

        let avg_diffs = trimmed.iter().sum::<f64>() / trimmed.len() as f64;
        let real_accuracy = 1.0 - avg_diffs;
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

        let accuracy =
            if let (Ok(Some(local_acc)), Ok(global_acc)) = (rx_local.await, rx_global.await) {
                (local_acc * 0.6 + global_acc * 0.4) / 100.0
            } else {
                0.0
            };

        let accuracy_diff = accuracy - real_accuracy;

        if accuracy_diff <= 5.0 {
            let choice: f64 = (prediction
                * self.feedback_engine.trading_mode_value.abs()
                * 100.0
                * volatility
                * 100.0
                * real_accuracy
                * 100.0)
                / self.risk_engine.risk_threshold;
            if choice > 0.1 {
                return TradingChoice::Buy(choice);
            } else if choice < -0.1 {
                return TradingChoice::Sell(choice.abs());
            } else {
                return TradingChoice::DoNothing;
            }
        }
        TradingChoice::DoNothing
    }

    async fn print_account_balance(&self) {
        println!(
            "{}DummyAccount total balance = {} USDT",
            self.print_time(),
            self.account
                .lock()
                .await
                .get_total_value(&self.client)
                .await
        )
    }
}

enum TradingChoice {
    Buy(f64),  // Percent
    Sell(f64), // Also percent
    DoNothing,
}

#[derive(Debug, Clone)]
enum TradingMode {
    Agressive,
    Conservative,
    Neutral,
}

struct FeedBackEngine {
    last_diffs: VecDeque<f64>,
    success_threshold: f64,
    trading_mode: Option<TradingMode>,
    trading_mode_value: f64,
    config: Config,
}

impl FeedBackEngine {
    fn new(config: Config) -> Self {
        Self {
            last_diffs: VecDeque::with_capacity(config.behaviour.feedback_engine_capacity),
            success_threshold: config.behaviour.success_threshold.default,
            trading_mode: None,
            trading_mode_value: config.behaviour.trading_mode_value.default,
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
        let new_threshold = avg * self.config.behaviour.success_threshold.ratio;
        self.success_threshold = new_threshold.clamp(
            self.config.behaviour.success_threshold.minimum,
            self.config.behaviour.success_threshold.maximum,
        );
    }

    fn update_trading_mode(&mut self, volatility: f64, risk_threshold: f64) {
        let volatility_norm = volatility.clamp(0.02, 0.1) / 0.1;
        let risk_norm = (1.0 / risk_threshold).clamp(0.5, 2.0) / 2.0;

        let pressure = risk_norm - volatility_norm;

        self.trading_mode_value = (self.trading_mode_value * 0.85 + pressure * 0.15).clamp(
            self.config.behaviour.trading_mode_value.minimum,
            self.config.behaviour.trading_mode_value.maximum,
        );

        self.trading_mode = if self.trading_mode_value > 0.2 {
            Some(TradingMode::Agressive)
        } else if self.trading_mode_value < -0.2 {
            Some(TradingMode::Conservative)
        } else {
            Some(TradingMode::Neutral)
        };
    }
}

struct RiskEngine {
    risk_threshold: f64,
    symbol: String,
    config: Config,
}

impl RiskEngine {
    fn new(symbol: String, config: Config) -> Self {
        Self {
            risk_threshold: config.behaviour.risk_threshold.default,
            symbol,
            config,
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

            self.risk_threshold =
                ((volatility * 100.0 * risk_modifier * 0.4) + self.risk_threshold * 0.6).clamp(
                    self.config.behaviour.risk_threshold.minimum,
                    self.config.behaviour.risk_threshold.maximum,
                );
        }
    }
}

// Dummy crypto balance
#[derive(Debug, Clone)]
pub struct DummyAccount {
    balance: f64,                 // Main balance (Tether USDT)
    tokens: HashMap<String, f64>, // Token name & Balance
}

impl DummyAccount {
    // pub fn new() -> Self {
    //     DummyAccount {
    //         balance: 0.0,
    //         tokens: HashMap::new(),
    //     }
    // }

    pub fn with_balance(balance: f64) -> Self {
        DummyAccount {
            balance,
            tokens: HashMap::new(),
        }
    }

    pub async fn buy(&mut self, token: &str, amount: f64, client: &BinanceClient) {
        if amount <= 0.0 {
            return;
        }

        if self.balance < amount {
            return;
        }

        let ask = client.fetch_ticker(token).await.ask;

        if ask <= 0.0 {
            return;
        }

        let token_amount = amount / ask;

        self.balance -= amount;
        self.add_token_balance(token, token_amount);
    }

    pub async fn sell(&mut self, token: &str, amount: f64, client: &BinanceClient) {
        if amount <= 0.0 {
            return;
        }

        let current_balance = self.tokens.get(token).copied().unwrap_or(0.0);

        if current_balance < amount {
            return;
        }

        let bid = client.fetch_ticker(token).await.bid;

        if bid <= 0.0 {
            return;
        }

        let usdt_amount = amount * bid;

        self.remove_token_balance(token, amount);
        self.balance += usdt_amount;
    }

    pub fn add_token_balance(&mut self, token: &str, amount: f64) {
        if amount <= 0.0 {
            return;
        }

        *self.tokens.entry(token.to_string()).or_insert(0.0) += amount;
    }

    pub fn remove_token_balance(&mut self, token: &str, amount: f64) {
        if amount <= 0.0 {
            return;
        }

        if let Some(balance) = self.tokens.get_mut(token) {
            *balance -= amount;

            if *balance <= 0.0 {
                self.tokens.remove(token);
            }
        }
    }

    pub fn get_balance(&self) -> f64 {
        self.balance
    }

    pub fn get_token_balance(&self, token: &str) -> f64 {
        self.tokens.get(token).copied().unwrap_or(0.0)
    }

    // pub fn get_tokens(&self) -> &HashMap<String, f64> {
    //     &self.tokens
    // }

    // pub fn deposit(&mut self, amount: f64) -> Result<(), String> {
    //     if amount <= 0.0 {
    //         return Err("Deposit amount must be positive".to_string());
    //     }
    //     self.balance += amount;
    //     Ok(())
    // }

    // pub fn withdraw(&mut self, amount: f64) -> Result<(), String> {
    //     if amount <= 0.0 {
    //         return Err("Withdrawal amount must be positive".to_string());
    //     }
    //     if self.balance < amount {
    //         return Err(format!(
    //             "Insufficient balance. Available: {}, Required: {}",
    //             self.balance, amount
    //         ));
    //     }
    //     self.balance -= amount;
    //     Ok(())
    // }

    pub async fn get_total_value(&self, client: &BinanceClient) -> f64 {
        let mut total = self.balance;

        for (token, amount) in &self.tokens {
            let bid = client.fetch_ticker(token).await.bid;
            total += amount * bid;
        }

        total
    }

    // pub async fn liquidate_all(&mut self, client: &BinanceClient) {
    //     let tokens: Vec<String> = self.tokens.keys().cloned().collect();

    //     for token in tokens {
    //         let amount = self.get_token_balance(&token);
    //         if amount > 0.0 {
    //             self.sell(&token, amount, client).await;
    //         }
    //     }
    // }
}
