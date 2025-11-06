use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};

use crate::engine::cycles::loader::cycle::LoaderCycle;
use crate::engine::cycles::trading::cycle::TradingCycle;
use crate::engine::utils::colors::Fore;
use crate::models::model::RFInterface;

#[derive(Clone, Copy)]
pub enum CycleType {
    Loader,
    Trading,
}

pub struct CycleManager {
    symbols: Vec<String>,
    cycle_type: HashMap<String, CycleType>,
    tasks: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
    stop_notify: Arc<Notify>,
    should_stop: Arc<RwLock<bool>>,
    model: Option<Arc<Mutex<RFInterface>>>,
}

impl CycleManager {
    pub fn new(symbols: Vec<String>, model: Option<Arc<Mutex<RFInterface>>>) -> Self {
        Self {
            symbols: symbols.clone(),
            cycle_type: HashMap::new(),
            tasks: Arc::new(RwLock::new(HashMap::new())),
            stop_notify: Arc::new(Notify::new()),
            should_stop: Arc::new(RwLock::new(false)),
            model,
        }
    }

    pub fn with_cycle_types(mut self, cycle_types: HashMap<String, CycleType>) -> Self {
        self.cycle_type = cycle_types;
        self
    }

    pub fn add_symbol(&mut self, symbol: String, cycle_type: CycleType) {
        if !self.symbols.contains(&symbol) {
            self.symbols.push(symbol.clone());
        }
        self.cycle_type.insert(symbol, cycle_type);
    }

    pub async fn run_all(&self) {
        let mut tasks_guard = self.tasks.write().await;

        for symbol in &self.symbols {
            let cycle_type = self.cycle_type.get(symbol).unwrap_or(&CycleType::Loader);
            let handle = self
                .spawn_cycle_with_restart(symbol.clone(), cycle_type)
                .await;
            tasks_guard.insert(symbol.clone(), handle);
        }

        println!(
            "{}{}",
            Fore::CYAN.as_str(),
            format!(
                "Запущено {} загрузочных циклов: {}",
                self.symbols.len(),
                self.symbols.join(", ")
            )
        );

        drop(tasks_guard);

        self.stop_notify.notified().await;
    }

    pub async fn run_cycle(&self, symbol: &str) -> Result<(), String> {
        if !self.symbols.contains(&symbol.to_string()) {
            return Err(format!("Символ {} не найден в списке", symbol));
        }

        let mut tasks_guard = self.tasks.write().await;

        if tasks_guard.contains_key(symbol) {
            return Err(format!("Цикл для {} уже запущен", symbol));
        }

        let cycle_type = self.cycle_type.get(symbol).unwrap_or(&CycleType::Loader);
        let handle = self
            .spawn_cycle_with_restart(symbol.to_string(), cycle_type)
            .await;
        tasks_guard.insert(symbol.to_string(), handle);

        println!(
            "{}{}",
            Fore::CYAN.as_str(),
            format!("Запущен цикл для {}", symbol)
        );
        Ok(())
    }

    pub async fn run_symbols(&self, symbols: Vec<&str>) {
        let mut tasks_guard = self.tasks.write().await;

        for symbol in symbols {
            if !self.symbols.contains(&symbol.to_string()) {
                eprintln!(
                    "{}{}",
                    Fore::YELLOW.as_str(),
                    format!("Символ {} не найден, пропускаем", symbol)
                );
                continue;
            }

            let cycle_type = self.cycle_type.get(symbol).unwrap_or(&CycleType::Loader);
            let handle = self
                .spawn_cycle_with_restart(symbol.to_string(), cycle_type)
                .await;
            tasks_guard.insert(symbol.to_string(), handle);
        }

        println!(
            "{}{}",
            Fore::CYAN.as_str(),
            format!("Запущено {} циклов", tasks_guard.len())
        );
        drop(tasks_guard);

        self.stop_notify.notified().await;
    }

    pub async fn stop_cycle(&self, symbol: &str) -> Result<(), String> {
        let mut tasks_guard = self.tasks.write().await;

        if let Some(handle) = tasks_guard.remove(symbol) {
            handle.abort();
            println!(
                "{}{}",
                Fore::YELLOW.as_str(),
                format!("Цикл {} остановлен", symbol)
            );
            Ok(())
        } else {
            Err(format!("Цикл для {} не запущен", symbol))
        }
    }

    pub async fn stop_all(&self) {
        println!("{}Остановка всех загрузочных циклов", Fore::YELLOW.as_str());

        *self.should_stop.write().await = true;

        let mut tasks_guard = self.tasks.write().await;

        for (symbol, handle) in tasks_guard.drain() {
            handle.abort();
            println!(
                "{}{}",
                Fore::YELLOW.as_str(),
                format!("Остановлен цикл: {}", symbol)
            );
        }

        self.stop_notify.notify_waiters();
        println!("{}Все циклы остановлены", Fore::YELLOW.as_str());
    }

    async fn spawn_cycle_with_restart(
        &self,
        symbol: String,
        cycle_type: &CycleType,
    ) -> JoinHandle<()> {
        let should_stop = Arc::clone(&self.should_stop);
        let symbol_clone = symbol.clone();
        let cycle_type_clone = *cycle_type;
        let model = self.model.clone();

        tokio::spawn(async move {
            loop {
                if *should_stop.read().await {
                    break;
                }

                match Self::run_cycle_once(&symbol_clone, &cycle_type_clone, model.clone()).await {
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
        model: Option<Arc<Mutex<RFInterface>>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match cycle_type {
            CycleType::Loader => {
                let mut cycle = LoaderCycle::new(symbol.to_string()).await;
                println!("Запуск LoaderCycle для {}", symbol);
                sleep(Duration::from_secs(10)).await;
                cycle.run().await;
            }
            CycleType::Trading => {
                let mut cycle = TradingCycle::new(symbol.to_string()).await;
                println!("Запуск TradingCycle для {}", symbol);
                sleep(Duration::from_secs(10)).await;
                cycle.run(&model.unwrap()).await;
            }
        }
        Ok(())
    }

    pub async fn active_cycles(&self) -> Vec<String> {
        self.tasks.read().await.keys().cloned().collect()
    }
}
