use chrono::Local;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex as TokioMutex, mpsc, oneshot};
use tokio::time::{Duration, sleep};

use crate::CONFIG_PATH;
use crate::engine::cycles::loader::cycle::LoaderCycle;
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
}

#[derive(Debug)]
pub enum CounterCommand {
    Increment {
        symbol: String,
        value: u8,
    },
    GetAccuracy {
        symbol: String,
        respond_to: oneshot::Sender<Option<f64>>,
    },
    GetShiftedAccuracy {
        symbol: String,
        window: usize,
        respond_to: oneshot::Sender<Option<f64>>,
    },
    GetTotalAccuracy {
        respond_to: oneshot::Sender<f64>,
    },
    GetTotalShiftedAccuracy {
        window: usize,
        respond_to: oneshot::Sender<Option<f64>>,
    },
}

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
            _ => panic!("Cycle type must be 'training' or 'loader'"),
        }
    }
}

struct CounterActor {
    counters: Counters,
    inbox: mpsc::Receiver<CounterCommand>,
}

impl CounterActor {
    fn new(capacity: usize) -> (Self, mpsc::Sender<CounterCommand>) {
        let (tx, rx) = mpsc::channel(1000);
        (
            Self {
                counters: Counters::new(capacity),
                inbox: rx,
            },
            tx,
        )
    }

    async fn run(mut self) {
        if load_config(CONFIG_PATH)
            .prints
            .manager
            .additional_manager_prints
        {
            println!(
                "{}[{}] {}CounterActor запущен",
                Fore::WHITE.as_str(),
                Local::now().format("%H:%M:%S"),
                Fore::CYAN.as_str()
            );
        }

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                CounterCommand::Increment { symbol, value } => {
                    self.counters.get_mut(&symbol.to_uppercase()).push(value);
                }
                CounterCommand::GetAccuracy { symbol, respond_to } => {
                    let acc = self
                        .counters
                        .get_option(&symbol.to_uppercase())
                        .map(|c| c.get_accuracy());
                    let _ = respond_to.send(acc);
                }
                CounterCommand::GetShiftedAccuracy {
                    symbol,
                    window,
                    respond_to,
                } => {
                    let acc = self
                        .counters
                        .get_option(&symbol.to_uppercase())
                        .and_then(|c| c.get_shifted_accuracy(window));
                    let _ = respond_to.send(acc);
                }
                CounterCommand::GetTotalAccuracy { respond_to } => {
                    let values = self.counters.symbols.values();
                    let count = values.len();

                    let acc = if count == 0 {
                        0.0
                    } else {
                        values.map(|c| c.get_accuracy()).sum::<f64>() / count as f64
                    };

                    let _ = respond_to.send(acc);
                }

                CounterCommand::GetTotalShiftedAccuracy { window, respond_to } => {
                    let values = self.counters.symbols.values();
                    let count = values.len();

                    let acc = if count == 0 {
                        0.0
                    } else {
                        values
                            .map(|c| c.get_shifted_accuracy(window).unwrap_or(0.0))
                            .sum::<f64>()
                            / count as f64
                    };

                    let _ = respond_to.send(Some(acc));
                }
            }
        }

        if load_config(CONFIG_PATH)
            .prints
            .manager
            .additional_manager_prints
        {
            println!(
                "{}[{}] {}CounterActor остановлен",
                Fore::WHITE.as_str(),
                Local::now().format("%H:%M:%S"),
                Fore::YELLOW.as_str()
            );
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
        if load_config(CONFIG_PATH).prints.manager.manager_init {
            println!(
                "{}[{}] {}Worker {} остановлен",
                Fore::WHITE.as_str(),
                Local::now().format("%H:%M:%S"),
                Fore::YELLOW.as_str(),
                self.symbol
            );
        }
    }
}

struct CycleSupervisor {
    workers: HashMap<String, WorkerHandle>,
    model: Option<Arc<TokioMutex<RFInterface>>>,
    counter_handle: mpsc::Sender<CounterCommand>,
    inbox: mpsc::Receiver<SupervisorCommand>,
}

impl CycleSupervisor {
    fn new(
        model: Option<Arc<TokioMutex<RFInterface>>>,
        counter_handle: mpsc::Sender<CounterCommand>,
    ) -> (Self, mpsc::Sender<SupervisorCommand>) {
        let (tx, rx) = mpsc::channel(50);

        (
            Self {
                workers: HashMap::new(),
                model,
                counter_handle,
                inbox: rx,
            },
            tx,
        )
    }

    async fn run(mut self) {
        if load_config(CONFIG_PATH)
            .prints
            .manager
            .additional_manager_prints
        {
            println!(
                "{}[{}] {}Supervisor запущен",
                Fore::WHITE.as_str(),
                Local::now().format("%H:%M:%S"),
                Fore::CYAN.as_str()
            );
        }

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
            }
        }

        if load_config(CONFIG_PATH)
            .prints
            .manager
            .additional_manager_prints
        {
            println!(
                "{}[{}] {}Supervisor остановлен",
                Fore::WHITE.as_str(),
                Local::now().format("%H:%M:%S"),
                Fore::YELLOW.as_str()
            );
        }
    }

    async fn start_worker(&mut self, symbol: String, cycle_type: CycleType) -> Result<(), String> {
        if self.workers.contains_key(&symbol) {
            return Err(format!("Worker {} уже запущен", symbol));
        }

        if matches!(cycle_type, CycleType::Training) && self.model.is_none() {
            return Err("Model не инициализирована для Training цикла".to_string());
        }

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let model = self.model.clone();
        let counter_tx = self.counter_handle.clone();
        let symbol_clone = symbol.clone();

        let task = tokio::spawn(async move {
            Self::worker_loop(symbol_clone, cycle_type, model, counter_tx, shutdown_rx).await;
        });

        self.workers.insert(
            symbol.clone(),
            WorkerHandle {
                symbol: symbol.clone(),
                task,
                shutdown_tx,
            },
        );

        if load_config(CONFIG_PATH).prints.manager.manager_init {
            println!(
                "{}[{}] {}Worker {} запущен ({:?})",
                Fore::WHITE.as_str(),
                Local::now().format("%H:%M:%S"),
                Fore::GREEN.as_str(),
                symbol,
                cycle_type
            );
        }
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
        model: Option<Arc<TokioMutex<RFInterface>>>,
        counter_tx: mpsc::Sender<CounterCommand>,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) {
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    if load_config(CONFIG_PATH).prints.manager.manager_init {
                        println!(
                            "{}[{}] {}Worker {} получил сигнал остановки",
                            Fore::WHITE.as_str(),
                            Local::now().format("%H:%M:%S"),
                            Fore::YELLOW.as_str(),
                            symbol
                        );
                    }
                    break;
                }

                result = Self::run_cycle_once(&symbol, cycle_type, &model, &counter_tx) => {
                    match result {
                        Ok(_) => {
                            if load_config(CONFIG_PATH).prints.manager.manager_init {
                                println!(
                                    "{}[{}] {}Worker {} завершился нормально",
                                    Fore::WHITE.as_str(),
                                    Local::now().format("%H:%M:%S"),
                                    Fore::GREEN.as_str(),
                                    symbol
                                );
                            }
                            break;
                        }
                        Err(e) => {
                            eprintln!(
                                "{}Worker {} упал: {}, рестарт через 5 сек",
                                Fore::RED.as_str(), symbol, e
                            );
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
        model: &Option<Arc<TokioMutex<RFInterface>>>,
        counter_tx: &mpsc::Sender<CounterCommand>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match cycle_type {
            CycleType::Loader => {
                let mut cycle = LoaderCycle::new(symbol.to_string()).await;
                sleep(Duration::from_secs(10)).await;
                cycle.run().await;
            }
            CycleType::Training => {
                let mut cycle = TrainingCycle::new(symbol.to_string()).await;

                let model = model
                    .as_ref()
                    .expect("Model should be initialized for Training cycle");

                sleep(Duration::from_secs(10)).await;
                cycle.run(model, counter_tx).await;
            }
        }
        Ok(())
    }
}

pub struct CycleManager {
    supervisor_tx: mpsc::Sender<SupervisorCommand>,
    counter_tx: mpsc::Sender<CounterCommand>,
    _counter_task: tokio::task::JoinHandle<()>,
    _supervisor_task: tokio::task::JoinHandle<()>,
}

impl CycleManager {
    pub async fn new() -> Self {
        let capacity = load_config(CONFIG_PATH).data.accuracy_capacity;
        let (counter_actor, counter_tx) = CounterActor::new(capacity);
        let counter_task = tokio::spawn(counter_actor.run());

        let (supervisor, supervisor_tx) = CycleSupervisor::new(None, counter_tx.clone());
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
                CycleType::Training
            )
        });

        if needs_model {
            let (tx, rx) = oneshot::channel();
            self.supervisor_tx
                .send(SupervisorCommand::StopAll { respond_to: tx })
                .await
                .map_err(|_| "Supervisor недоступен")?;
            let _ = rx.await;

            let pool = PgPool::connect(&load_env()[0])
                .await
                .map_err(|e| format!("DB connection error: {}", e))?;
            let model = Arc::new(TokioMutex::new(RFInterface::new()));
            train_model(&pool, &model).await;
            drop(pool);

            let (supervisor, new_supervisor_tx) =
                CycleSupervisor::new(Some(model), self.counter_tx.clone());
            let supervisor_task = tokio::spawn(supervisor.run());

            self.supervisor_tx = new_supervisor_tx;
            self._supervisor_task = supervisor_task;
        }

        for symbol in &symbols {
            let cycle_type = cycle_types.get(symbol).unwrap_or(&CycleType::Loader);
            self.add_cycle(symbol.clone(), *cycle_type).await?;
        }

        if load_config(CONFIG_PATH).prints.manager.manager_init {
            println!(
                "{}[{}] {}Запущено {} циклов: {}",
                Fore::WHITE.as_str(),
                Local::now().format("%H:%M:%S"),
                Fore::CYAN.as_str(),
                symbols.len(),
                symbols.join(", ")
            );
        }

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
