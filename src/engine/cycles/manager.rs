use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as tokio_mutex;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};

use crate::engine::cycles::loader::cycle::LoaderCycle;
use crate::engine::cycles::training::cycle::TrainingCycle;
use crate::engine::state::counters::Counters;
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::load_env::load_env;
use crate::models::model::{RFInterface, train_model};

#[derive(Clone, Copy)]
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
    stop_notify: Arc<Notify>,
    should_stop: Arc<RwLock<bool>>,
}

impl CycleManager {
    pub fn new(symbols: Vec<String>) -> Self {
        Self {
            symbols: symbols.clone(),
            cycle_type: HashMap::new(),
            tasks: Arc::new(RwLock::new(HashMap::new())),
            stop_notify: Arc::new(Notify::new()),
            should_stop: Arc::new(RwLock::new(false)),
        }
    }

    pub fn with_cycle_types(mut self, cycle_types: HashMap<String, CycleType>) -> Self {
        self.cycle_type = cycle_types;
        self
    }

    pub async fn run_all(&self) {
        let mut tasks_guard = self.tasks.write().await;

        let needs_model = self.symbols.iter().any(|symbol| {
            matches!(
                self.cycle_type.get(symbol).unwrap_or(&CycleType::Loader),
                CycleType::Training
            )
        });

        let model = if needs_model {
            let pool = PgPool::connect(&load_env()[0]).await.unwrap();
            let model = Arc::new(Mutex::new(RFInterface::new()));
            train_model(&pool, &model).await;
            drop(pool);
            Some(model)
        } else {
            None
        };

        let counters = Arc::new(tokio_mutex::new(Counters::new()));

        for symbol in &self.symbols {
            let cycle_type = self.cycle_type.get(symbol).unwrap_or(&CycleType::Loader);
            let handle = self
                .spawn_cycle_with_restart(symbol.clone(), cycle_type, &model, counters.clone())
                .await;
            tasks_guard.insert(symbol.clone(), handle);
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

        drop(tasks_guard);

        self.stop_notify.notified().await;
    }

    async fn spawn_cycle_with_restart(
        &self,
        symbol: String,
        cycle_type: &CycleType,
        model: &Option<Arc<Mutex<RFInterface>>>,
        counters: Arc<tokio_mutex<Counters>>,
    ) -> JoinHandle<()> {
        let should_stop = Arc::clone(&self.should_stop);
        let symbol_clone = symbol.clone();
        let cycle_type_clone = *cycle_type;
        let model_clone = model.clone();
        let counters_clone = counters.clone();

        tokio::spawn(async move {
            loop {
                if *should_stop.read().await {
                    break;
                }

                match Self::run_cycle_once(
                    &symbol_clone,
                    &cycle_type_clone,
                    &model_clone,
                    counters_clone.clone(),
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
        model: &Option<Arc<Mutex<RFInterface>>>,
        counters: Arc<tokio_mutex<Counters>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match cycle_type {
            CycleType::Loader => {
                let mut cycle = LoaderCycle::new(symbol.to_string()).await;
                println!("Запуск LoaderCycle для {}", symbol);
                sleep(Duration::from_secs(10)).await;
                cycle.run().await;
            }
            CycleType::Training => {
                let mut cycle = TrainingCycle::new(symbol.to_string()).await;

                // Модель должна существовать для Training цикла
                let model = model
                    .as_ref()
                    .expect("Model should be initialized for Training cycle");

                println!("Запуск TradingCycle для {}", symbol);
                sleep(Duration::from_secs(10)).await;
                cycle.run(model, counters).await;
            }
        }
        Ok(())
    }
}
