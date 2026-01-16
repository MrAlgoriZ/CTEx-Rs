use chrono::Local;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::{Duration, sleep};

use crate::CONFIG_PATH;
use crate::data::data_interfaces::FlattenedData;
use crate::data::requests::ccxt::binance::BinanceClient;
use crate::engine::cycles::loader::cycle::LoaderCycle;
use crate::engine::cycles::sandbox::cycle::{DummyAccount, SandboxCycle};
use crate::engine::cycles::training::cycle::TrainingCycle;
use crate::engine::state::counters::Counters;
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::load_config::load_config;
use crate::engine::utils::config::load_env::load_env;
use crate::models::model::{RFInterface, train_model};

#[derive(Debug)]
pub enum SupervisorCommand {
    StartCycle {
        symbol: String,
        cycle_type: CycleType,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    StopCycle {
        symbol: String,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
    StopAll {
        respond_to: oneshot::Sender<()>,
    },
    ListActive {
        respond_to: oneshot::Sender<Vec<String>>,
    },
    SetModel {
        model_tx: mpsc::Sender<ModelCommand>,
    },
}

struct CycleSupervisor {
    workers: HashMap<String, WorkerHandle>,
    model_tx: Option<mpsc::Sender<ModelCommand>>,
    counter_tx: mpsc::Sender<CounterCommand>,
    inbox: mpsc::Receiver<SupervisorCommand>,
}

impl CycleSupervisor {
    fn new(counter_tx: mpsc::Sender<CounterCommand>) -> (Self, mpsc::Sender<SupervisorCommand>) {
        let (tx, rx) = mpsc::channel(50);

        (
            Self {
                workers: HashMap::new(),
                model_tx: None,
                counter_tx,
                inbox: rx,
            },
            tx,
        )
    }

    async fn run(mut self) {
        log_info("Supervisor запущен");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                SupervisorCommand::StartCycle {
                    symbol,
                    cycle_type,
                    respond_to,
                } => {
                    let result = self.start_worker(symbol, cycle_type).await;
                    let _ = respond_to.send(result);
                }

                SupervisorCommand::StopCycle { symbol, respond_to } => {
                    let result = self.stop_worker(&symbol).await;
                    let _ = respond_to.send(result);
                }

                SupervisorCommand::StopAll { respond_to } => {
                    self.stop_all_workers().await;
                    let _ = respond_to.send(());
                }

                SupervisorCommand::ListActive { respond_to } => {
                    let active: Vec<String> = self.workers.keys().cloned().collect();
                    let _ = respond_to.send(active);
                }

                SupervisorCommand::SetModel { model_tx } => {
                    self.model_tx = Some(model_tx);
                    log_info("Model установлена в Supervisor");
                }
            }
        }

        log_warning("Supervisor остановлен");
    }

    async fn start_worker(&mut self, symbol: String, cycle_type: CycleType) -> Result<(), String> {
        if self.workers.contains_key(&symbol) {
            return Err(format!("Worker {} уже запущен", symbol));
        }

        if matches!(cycle_type, CycleType::Training | CycleType::Sandbox) && self.model_tx.is_none()
        {
            return Err("Model не инициализирована для цикла".to_string());
        }

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let model_tx = self.model_tx.clone();
        let counter_tx = self.counter_tx.clone();
        let symbol_clone = symbol.clone();

        let task = tokio::spawn(async move {
            Self::worker_loop(symbol_clone, cycle_type, counter_tx, model_tx, shutdown_rx).await;
        });

        self.workers.insert(
            symbol.clone(),
            WorkerHandle {
                symbol: symbol.clone(),
                task,
                shutdown_tx,
            },
        );

        log_success(&format!("Worker {} запущен ({:?})", symbol, cycle_type));
        Ok(())
    }

    async fn stop_worker(&mut self, symbol: &str) -> Result<(), String> {
        match self.workers.remove(symbol) {
            Some(handle) => {
                handle.stop().await;
                Ok(())
            }
            None => Err(format!("Worker {} не найден", symbol)),
        }
    }

    async fn stop_all_workers(&mut self) {
        let workers = std::mem::take(&mut self.workers);
        for (_, handle) in workers {
            handle.stop().await;
        }
    }

    async fn worker_loop(
        symbol: String,
        cycle_type: CycleType,
        counter_tx: mpsc::Sender<CounterCommand>,
        model_tx: Option<mpsc::Sender<ModelCommand>>,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) {
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    log_warning(&format!("Worker {} получил сигнал остановки", symbol));
                    break;
                }

                result = Self::run_cycle_once(&symbol, cycle_type, &counter_tx, &model_tx) => {
                    match result {
                        Ok(_) => {
                            log_success(&format!("Worker {} завершился нормально", symbol));
                            break;
                        }
                        Err(e) => {
                            log_error(&format!("Worker {} упал: {}, рестарт через 5 сек", symbol, e));
                            sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
            }
        }
    }

    async fn run_cycle_once(
        symbol: &str,
        cycle_type: CycleType,
        counter_tx: &mpsc::Sender<CounterCommand>,
        model_tx: &Option<mpsc::Sender<ModelCommand>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = BinanceClient::new().await;
        match cycle_type {
            CycleType::Loader => {
                let mut cycle = LoaderCycle::init(symbol.to_string(), client).await;
                sleep(Duration::from_secs(10)).await;
                cycle.run().await?;
            }
            CycleType::Training => {
                let mut cycle = TrainingCycle::init(symbol.to_string(), client).await;
                sleep(Duration::from_secs(10)).await;
                cycle.run(counter_tx, model_tx.as_ref().unwrap()).await?;
            }
            CycleType::Sandbox => {
                let mut cycle = SandboxCycle::init(symbol.to_string(), client).await;
                let account = Arc::new(Mutex::new(DummyAccount::with_balance(100.0)));
                sleep(Duration::from_secs(10)).await;
                cycle
                    .run(model_tx.as_ref().unwrap(), counter_tx, account)
                    .await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum CounterType {
    Threshold,
    Direction,
}

impl CounterType {
    pub fn from_str(counter_type: &str) -> Self {
        match counter_type {
            "threshold" => CounterType::Threshold,
            "direction" => CounterType::Direction,
            _ => panic!("Counter type must be 'threshold' or 'direction'"),
        }
    }
}

#[derive(Debug)]
pub enum CounterCommand {
    Increment {
        symbol: String,
        counter_type: CounterType,
        value: u8,
    },
    GetAccuracy {
        symbol: String,
        counter_type: CounterType,
        respond_to: oneshot::Sender<Option<f64>>,
    },
    GetShiftedAccuracy {
        symbol: String,
        window: usize,
        counter_type: CounterType,
        respond_to: oneshot::Sender<Option<f64>>,
    },
    GetTotalAccuracy {
        counter_type: CounterType,
        respond_to: oneshot::Sender<f64>,
    },
    GetTotalShiftedAccuracy {
        window: usize,
        counter_type: CounterType,
        respond_to: oneshot::Sender<Option<f64>>,
    },
}

struct CounterActor {
    threshold_counters: Counters,
    direction_counters: Counters,
    inbox: mpsc::Receiver<CounterCommand>,
}

impl CounterActor {
    fn new(capacity: usize) -> (Self, mpsc::Sender<CounterCommand>) {
        let (tx, rx) = mpsc::channel(1000);
        (
            Self {
                threshold_counters: Counters::new(capacity),
                direction_counters: Counters::new(capacity),
                inbox: rx,
            },
            tx,
        )
    }

    async fn run(mut self) {
        log_info("CounterActor запущен");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                CounterCommand::Increment {
                    symbol,
                    counter_type,
                    value,
                } => {
                    let counter = match counter_type {
                        CounterType::Threshold => &mut self.threshold_counters,
                        CounterType::Direction => &mut self.direction_counters,
                    };
                    counter.get_mut(&symbol.to_uppercase()).push(value);
                }

                CounterCommand::GetAccuracy {
                    symbol,
                    counter_type,
                    respond_to,
                } => {
                    let counters = match counter_type {
                        CounterType::Threshold => &self.threshold_counters,
                        CounterType::Direction => &self.direction_counters,
                    };
                    let acc = counters
                        .get_option(&symbol.to_uppercase())
                        .map(|c| c.get_accuracy());
                    let _ = respond_to.send(acc);
                }

                CounterCommand::GetShiftedAccuracy {
                    symbol,
                    window,
                    counter_type,
                    respond_to,
                } => {
                    let counters = match counter_type {
                        CounterType::Threshold => &self.threshold_counters,
                        CounterType::Direction => &self.direction_counters,
                    };
                    let acc = counters
                        .get_option(&symbol.to_uppercase())
                        .and_then(|c| c.get_shifted_accuracy(window));
                    let _ = respond_to.send(acc);
                }

                CounterCommand::GetTotalAccuracy {
                    counter_type,
                    respond_to,
                } => {
                    let values = match counter_type {
                        CounterType::Threshold => self.threshold_counters.symbols.values(),
                        CounterType::Direction => self.direction_counters.symbols.values(),
                    };
                    let acc = calculate_average_accuracy(values);
                    let _ = respond_to.send(acc);
                }

                CounterCommand::GetTotalShiftedAccuracy {
                    window,
                    counter_type,
                    respond_to,
                } => {
                    let values = match counter_type {
                        CounterType::Threshold => self.threshold_counters.symbols.values(),
                        CounterType::Direction => self.direction_counters.symbols.values(),
                    };
                    let acc = calculate_average_shifted_accuracy(values, window);
                    let _ = respond_to.send(Some(acc));
                }
            }
        }

        log_warning("CounterActor остановлен");
    }
}

pub enum ModelCommand {
    Predict {
        flattenned_candles: FlattenedData,
        respond_to: oneshot::Sender<f64>,
    },
    Train {
        data: Vec<FlattenedData>,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
}

struct ModelActor {
    model: Arc<Mutex<RFInterface>>,
    inbox: mpsc::Receiver<ModelCommand>,
}

impl ModelActor {
    fn new(model: RFInterface) -> (Self, mpsc::Sender<ModelCommand>) {
        let (tx, rx) = mpsc::channel(100);
        (
            Self {
                model: Arc::new(Mutex::new(model)),
                inbox: rx,
            },
            tx,
        )
    }

    async fn run(mut self) {
        log_info("ModelActor запущен");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                ModelCommand::Predict {
                    flattenned_candles,
                    respond_to,
                } => {
                    let model = self.model.clone();
                    let features = flattenned_candles.features;
                    let token = flattenned_candles.token;

                    let result = tokio::task::spawn_blocking(move || {
                        model.blocking_lock().predict(features, Some(&token))
                    })
                    .await;

                    let prediction = match result {
                        Ok(Ok(pred)) => pred,
                        Ok(Err(e)) => {
                            log_error(&format!("Ошибка предсказания: {}", e));
                            0.0
                        }
                        Err(e) => {
                            log_error(&format!("Ошибка spawn_blocking: {}", e));
                            0.0
                        }
                    };

                    let _ = respond_to.send(prediction);
                }

                ModelCommand::Train { data, respond_to } => {
                    let model = self.model.clone();

                    let result =
                        tokio::task::spawn_blocking(move || model.blocking_lock().train(data))
                            .await;

                    let train_result = match result {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(e.to_string()),
                        Err(e) => Err(format!("Ошибка spawn_blocking: {}", e)),
                    };

                    let _ = respond_to.send(train_result);
                }
            }
        }

        log_warning("ModelActor остановлен");
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CycleType {
    Loader,
    Training,
    Sandbox,
}

impl CycleType {
    pub fn from_str(cycle_type: &str) -> Self {
        match cycle_type {
            "training" => CycleType::Training,
            "loader" => CycleType::Loader,
            "sandbox" => CycleType::Sandbox,
            _ => panic!("Cycle type must be 'training', 'loader', or 'sandbox'"),
        }
    }
}

struct WorkerHandle {
    symbol: String,
    task: tokio::task::JoinHandle<()>,
    shutdown_tx: mpsc::Sender<()>,
}

impl WorkerHandle {
    async fn stop(self) {
        let _ = self.shutdown_tx.send(()).await;
        let _ = self.task.await;
        log_warning(&format!("Worker {} остановлен", self.symbol));
    }
}

pub struct CycleManager {
    supervisor_tx: mpsc::Sender<SupervisorCommand>,
    counter_tx: mpsc::Sender<CounterCommand>,
    _counter_task: tokio::task::JoinHandle<()>,
    _supervisor_task: tokio::task::JoinHandle<()>,
}

impl CycleManager {
    pub fn new() -> Self {
        let capacity = load_config(CONFIG_PATH).behaviour.accuracy_capacity;

        let (counter_actor, counter_tx) = CounterActor::new(capacity);
        let counter_task = tokio::spawn(counter_actor.run());

        let (supervisor, supervisor_tx) = CycleSupervisor::new(counter_tx.clone());
        let supervisor_task = tokio::spawn(supervisor.run());

        Self {
            supervisor_tx,
            counter_tx,
            _counter_task: counter_task,
            _supervisor_task: supervisor_task,
        }
    }

    pub async fn run_all(
        &mut self,
        symbols: Vec<String>,
        cycle_types: HashMap<String, CycleType>,
    ) -> Result<(), String> {
        let needs_model = symbols.iter().any(|symbol| {
            matches!(
                cycle_types.get(symbol).unwrap_or(&CycleType::Loader),
                CycleType::Training | CycleType::Sandbox
            )
        });

        if needs_model {
            self.initialize_model().await?;
        }

        for symbol in &symbols {
            let cycle_type = cycle_types.get(symbol).unwrap_or(&CycleType::Loader);
            self.add_cycle(symbol.clone(), *cycle_type).await?;
        }

        log_info(&format!(
            "Запущено {} циклов: {}",
            symbols.len(),
            symbols.join(", ")
        ));

        Ok(())
    }

    async fn initialize_model(&self) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.supervisor_tx
            .send(SupervisorCommand::StopAll { respond_to: tx })
            .await
            .map_err(|_| "Supervisor недоступен")?;
        let _ = rx.await;

        let pool = PgPool::connect(&load_env().database_url)
            .await
            .map_err(|e| format!("DB connection error: {}", e))?;

        let mut model = RFInterface::new();
        train_model(&pool, &mut model).await;

        let (model_actor, model_tx) = ModelActor::new(model);
        tokio::spawn(model_actor.run());

        self.supervisor_tx
            .send(SupervisorCommand::SetModel { model_tx })
            .await
            .map_err(|_| "Не удалось обновить модель в Supervisor")?;

        Ok(())
    }

    pub async fn add_cycle(&self, symbol: String, cycle_type: CycleType) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.supervisor_tx
            .send(SupervisorCommand::StartCycle {
                symbol,
                cycle_type,
                respond_to: tx,
            })
            .await
            .map_err(|_| "Supervisor недоступен".to_string())?;

        rx.await
            .map_err(|_| "Нет ответа от Supervisor".to_string())?
    }

    pub fn counter_handle(&self) -> mpsc::Sender<CounterCommand> {
        self.counter_tx.clone()
    }

    pub fn supervisor_handle(&self) -> mpsc::Sender<SupervisorCommand> {
        self.supervisor_tx.clone()
    }
}

fn calculate_average_accuracy<'a>(
    values: impl Iterator<Item = &'a crate::engine::state::counters::SymbolCounters<u8>>,
) -> f64 {
    let values: Vec<_> = values.collect();
    let count = values.len();

    if count == 0 {
        0.0
    } else {
        values.iter().map(|c| c.get_accuracy()).sum::<f64>() / count as f64
    }
}

fn calculate_average_shifted_accuracy<'a>(
    values: impl Iterator<Item = &'a crate::engine::state::counters::SymbolCounters<u8>>,
    window: usize,
) -> f64 {
    let values: Vec<_> = values.collect();
    let count = values.len();

    if count == 0 {
        0.0
    } else {
        values
            .iter()
            .map(|c| c.get_shifted_accuracy(window).unwrap_or(0.0))
            .sum::<f64>()
            / count as f64
    }
}

fn log_info(msg: &str) {
    if load_config(CONFIG_PATH)
        .prints
        .manager
        .additional_manager_prints
    {
        println!(
            "{}[{}] {}{}",
            Fore::WHITE.as_str(),
            Local::now().format("%H:%M:%S"),
            Fore::CYAN.as_str(),
            msg
        );
    }
}

fn log_success(msg: &str) {
    if load_config(CONFIG_PATH).prints.manager.manager_init {
        println!(
            "{}[{}] {}{}",
            Fore::WHITE.as_str(),
            Local::now().format("%H:%M:%S"),
            Fore::GREEN.as_str(),
            msg
        );
    }
}

fn log_warning(msg: &str) {
    if load_config(CONFIG_PATH).prints.manager.manager_init {
        println!(
            "{}[{}] {}{}",
            Fore::WHITE.as_str(),
            Local::now().format("%H:%M:%S"),
            Fore::YELLOW.as_str(),
            msg
        );
    }
}

fn log_error(msg: &str) {
    eprintln!("{}{}", Fore::RED.as_str(), msg);
}
