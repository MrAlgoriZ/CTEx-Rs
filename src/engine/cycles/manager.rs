use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as tokio_mutex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};

use crate::CONFIG_PATH;
use crate::data::requests::ccxt::binance::BinanceClient;
use crate::engine::cycles::loader::cycle::LoaderCycle;
use crate::engine::cycles::training::cycle::TrainingCycle;
use crate::engine::state::counters::Counters;
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::load_config::load_config;
use crate::engine::utils::config::load_env::load_env;
use crate::models::model::{RFInterface, train_model};

#[derive(Clone, Copy, Debug)]
pub enum CycleType {
    Loader,
    Training,
}

impl CycleType {
    pub fn from_str(cycle_type: &str) -> Self {
        match cycle_type {
            "training" => CycleType::Training,
            "loader" => CycleType::Loader,
            _ => panic!("Cycle type must be 'trading' or 'loader'"),
        }
    }
}

pub struct CycleManager {
    symbols: Vec<String>,
    cycle_type: HashMap<String, CycleType>,
    tasks: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
    should_stop: Arc<RwLock<bool>>,
    counters: Arc<tokio_mutex<Counters>>,
    model: Arc<RwLock<Option<Arc<Mutex<RFInterface>>>>>,
    client: Arc<BinanceClient>,
}

impl CycleManager {
    pub async fn new(symbols: Vec<String>) -> Self {
        Self {
            symbols: symbols.clone(),
            cycle_type: HashMap::new(),
            tasks: Arc::new(RwLock::new(HashMap::new())),
            should_stop: Arc::new(RwLock::new(false)),
            counters: Arc::new(tokio_mutex::new(Counters::new(
                load_config(CONFIG_PATH).data.accuracy_capacity,
            ))),
            model: Arc::new(RwLock::new(None)),
            client: Arc::new(BinanceClient::new().await),
        }
    }

    pub fn with_cycle_types(mut self, cycle_types: HashMap<String, CycleType>) -> Self {
        self.cycle_type = cycle_types;
        self
    }

    pub fn with_counters(mut self, counters: Arc<tokio_mutex<Counters>>) -> Self {
        self.counters = counters;
        self
    }

    pub async fn run_all(&self) {
        let needs_model = self.symbols.iter().any(|symbol| {
            matches!(
                self.cycle_type.get(symbol).unwrap_or(&CycleType::Loader),
                CycleType::Training
            )
        });

        if needs_model {
            let mut model_guard = self.model.write().await;
            if model_guard.is_none() {
                let pool = PgPool::connect(&load_env()[0]).await.unwrap();
                let model = Arc::new(Mutex::new(RFInterface::new()));
                train_model(&pool, &model).await;
                drop(pool);
                *model_guard = Some(model);
            }
        }

        for symbol in &self.symbols {
            let cycle_type = self.cycle_type.get(symbol).unwrap_or(&CycleType::Loader);
            let handle = self
                .spawn_cycle_with_restart(symbol.clone(), cycle_type, self.counters.clone())
                .await;
            {
                let mut tasks_guard = self.tasks.write().await;
                tasks_guard.insert(symbol.clone(), handle);
            }
        }

        println!(
            "{}{}",
            Fore::CYAN.as_str(),
            format!(
                "Запущено {} циклов: {}",
                self.symbols.len(),
                self.symbols.join(", ")
            )
        );

        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    }

    // pub async fn add_cycle(&mut self, symbol: String, cycle_type: CycleType) -> Result<(), String> {
    //     {
    //         let tasks_guard = self.tasks.read().await;
    //         if tasks_guard.contains_key(&symbol) {
    //             return Err(format!("Цикл для {} уже запущен", symbol));
    //         }
    //     }

    //     if matches!(cycle_type, CycleType::Training) {
    //         let mut model_guard = self.model.write().await;
    //         if model_guard.is_none() {
    //             let pool = PgPool::connect(&load_env()[0])
    //                 .await
    //                 .map_err(|e| e.to_string())?;
    //             let model = Arc::new(Mutex::new(RFInterface::new()));
    //             train_model(&pool, &model).await;
    //             drop(pool);
    //             *model_guard = Some(model);
    //         }
    //     }

    //     if !self.symbols.contains(&symbol) {
    //         self.symbols.push(symbol.clone());
    //     }
    //     self.cycle_type.insert(symbol.clone(), cycle_type);

    //     let handle = self
    //         .spawn_cycle_with_restart(symbol.clone(), &cycle_type, self.counters.clone())
    //         .await;

    //     let mut tasks_guard = self.tasks.write().await;
    //     tasks_guard.insert(symbol.clone(), handle);

    //     println!(
    //         "{}{}",
    //         Fore::GREEN.as_str(),
    //         format!("Добавлен цикл {:?} для {}", cycle_type, symbol)
    //     );

    //     Ok(())
    // }

    // pub async fn stop_cycle(&mut self, symbol: &str) -> Result<(), String> {
    //     let mut tasks_guard = self.tasks.write().await;

    //     if let Some(handle) = tasks_guard.remove(symbol) {
    //         handle.abort();

    //         self.symbols.retain(|s| s != symbol);
    //         self.cycle_type.remove(symbol);

    //         println!(
    //             "{}{}",
    //             Fore::YELLOW.as_str(),
    //             format!("Остановлен цикл для {}", symbol)
    //         );

    //         Ok(())
    //     } else {
    //         Err(format!("Цикл для {} не найден", symbol))
    //     }
    // }

    // pub async fn stop_all(&mut self) {
    //     *self.should_stop.write().await = true;

    //     let mut tasks_guard = self.tasks.write().await;

    //     for (symbol, handle) in tasks_guard.drain() {
    //         handle.abort();
    //         println!(
    //             "{}{}",
    //             Fore::YELLOW.as_str(),
    //             format!("Остановлен цикл для {}", symbol)
    //         );
    //     }

    //     self.symbols.clear();
    //     self.cycle_type.clear();

    //     println!("{}{}", Fore::RED.as_str(), "Все циклы остановлены");
    // }

    async fn spawn_cycle_with_restart(
        &self,
        symbol: String,
        cycle_type: &CycleType,
        counters: Arc<tokio_mutex<Counters>>,
    ) -> JoinHandle<()> {
        let should_stop = Arc::clone(&self.should_stop);
        let symbol_clone = symbol.clone();
        let cycle_type_clone = *cycle_type;
        let model = Arc::clone(&self.model);
        let counters_clone = counters.clone();
        let client = Arc::clone(&self.client);

        tokio::spawn(async move {
            loop {
                if *should_stop.read().await {
                    break;
                }

                match Self::run_cycle_once(
                    &symbol_clone,
                    &cycle_type_clone,
                    &model,
                    counters_clone.clone(),
                    Arc::clone(&client),
                )
                .await
                {
                    Ok(_) => {
                        break;
                    }
                    Err(e) => {
                        eprintln!(
                            "{}{}",
                            Fore::RED.as_str(),
                            format!(
                                "Цикл {} упал с ошибкой: {}, перезапуск через 5 секунд",
                                symbol_clone, e
                            )
                        );
                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        })
    }

    async fn run_cycle_once(
        symbol: &str,
        cycle_type: &CycleType,
        model: &Arc<RwLock<Option<Arc<Mutex<RFInterface>>>>>,
        counters: Arc<tokio_mutex<Counters>>,
        client: Arc<BinanceClient>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match cycle_type {
            CycleType::Loader => {
                let mut cycle = LoaderCycle::new(symbol.to_string(), client).await;
                println!("Запуск LoaderCycle для {}", symbol);
                sleep(Duration::from_secs(10)).await;
                cycle.run().await;
            }
            CycleType::Training => {
                let mut cycle = TrainingCycle::new(symbol.to_string(), client).await;

                let model_guard = model.read().await;
                let model_ref = model_guard
                    .as_ref()
                    .expect("Model should be initialized for Training cycle");

                println!("Запуск TradingCycle для {}", symbol);
                sleep(Duration::from_secs(10)).await;
                cycle.run(model_ref, counters).await;
            }
        }
        Ok(())
    }

    pub async fn active_cycles(&self) -> Vec<String> {
        self.tasks.read().await.keys().cloned().collect()
    }
}
