use chrono::Utc;
use log::{debug, error, info, warn};
use serde::Deserialize;
use sqlx::PgPool;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::{Duration, sleep};

use crate::CONFIG_PATH;
use crate::data::data_interfaces::{Candle, CandleWithTimestamp, DataMap, Ticker};
use crate::data::requests::ccxt::client::CCXTClient;
use crate::data::requests::database::standart::SQLStandart;
use crate::engine::cycles::background::cycle::BackgroundCycle;
use crate::engine::cycles::loader::cycle::LoaderCycle;
use crate::engine::cycles::loaderwm::cycle::LoaderWMCycle;
use crate::engine::cycles::sandbox::cycle::SandboxCycle;
use crate::engine::cycles::training::cycle::TrainingCycle;
use crate::engine::state::counters::{Counters, SymbolCounters};
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::{Config, CycleType, RuntimeType};
use crate::engine::utils::config::load_config::load_config;
use crate::engine::utils::config::load_env::load_env;
use crate::engine::utils::parse::parse_symbol;
use crate::models::model::{Model, init_ensemble_model, init_single_model};
use crate::models::{ModelParams, ModelStructure};

pub enum CycleError {
    SymbolDoesNotExist,
    AnyhowError(anyhow::Error),
}

impl From<anyhow::Error> for CycleError {
    fn from(err: anyhow::Error) -> Self {
        CycleError::AnyhowError(err)
    }
}

#[derive(Debug)]
pub enum SupervisorCommand {
    StartCycle {
        symbol: String,
        cycle_type: CycleType,
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    StopCycle {
        symbol: String,
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
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
    server_tx: mpsc::Sender<ServersCommand>,
    inbox: mpsc::Receiver<SupervisorCommand>,
}

impl CycleSupervisor {
    fn new(
        counter_tx: mpsc::Sender<CounterCommand>,
        server_tx: mpsc::Sender<ServersCommand>,
    ) -> (Self, mpsc::Sender<SupervisorCommand>) {
        let (tx, rx) = mpsc::channel(50);

        (
            Self {
                workers: HashMap::new(),
                model_tx: None,
                counter_tx,
                server_tx,
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

    async fn start_worker(
        &mut self,
        symbol: String,
        cycle_type: CycleType,
    ) -> Result<(), anyhow::Error> {
        if self.workers.contains_key(&symbol) {
            return Err(anyhow::anyhow!(format!("Worker {} уже запущен", symbol)));
        }

        if matches!(cycle_type, CycleType::Training | CycleType::Sandbox) && self.model_tx.is_none()
        {
            return Err(anyhow::anyhow!("Model не инициализирована для цикла"));
        }

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let model_tx = self.model_tx.clone();
        let counter_tx = self.counter_tx.clone();
        let server_tx = self.server_tx.clone();
        let symbol_clone = symbol.clone();

        let task = tokio::spawn(async move {
            Self::worker_loop(
                symbol_clone,
                cycle_type,
                counter_tx,
                server_tx,
                model_tx,
                shutdown_rx,
            )
            .await;
        });

        self.workers.insert(
            symbol.clone(),
            WorkerHandle {
                symbol: symbol.clone(),
                task,
                shutdown_tx,
            },
        );

        log_debug(&format!("Worker {} запущен ({:?})", symbol, cycle_type));
        Ok(())
    }

    async fn stop_worker(&mut self, symbol: &str) -> Result<(), anyhow::Error> {
        match self.workers.remove(symbol) {
            Some(handle) => {
                handle.stop().await;
                Ok(())
            }
            None => Err(anyhow::anyhow!(format!("Worker {} не найден", symbol))),
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
        server_tx: mpsc::Sender<ServersCommand>,
        model_tx: Option<mpsc::Sender<ModelCommand>>,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) {
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    log_warning(&format!("Worker {} получил сигнал остановки", symbol));
                    break;
                }

                result = Self::run_cycle_once(&symbol, cycle_type, &counter_tx, &server_tx, &model_tx, ) => {
                    match result {
                        Ok(_) => {
                            log_debug(&format!("Worker {} завершился нормально", symbol));
                            break;
                        }
                        Err(e) => {
                            match e {
                                CycleError::AnyhowError(err) => {
                                    log_error(&format!("Worker {} упал: {}, рестарт через 5 сек", symbol, err));
                                    sleep(Duration::from_secs(5)).await;
                                }
                                CycleError::SymbolDoesNotExist => {
                                    log_error(&format!("Токена {} не существует!", symbol));
                                    break;
                                }
                            }
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
        server_tx: &mpsc::Sender<ServersCommand>,
        model_tx: &Option<mpsc::Sender<ModelCommand>>,
    ) -> Result<(), CycleError> {
        let config = load_config(CONFIG_PATH);
        let client = CCXTClient::new(&config.main_exchange, server_tx.clone());

        match cycle_type {
            CycleType::Loader => {
                match config.model.model_struct {
                    ModelStructure::Ensemble => {
                        println!(
                            "{}ВНИМАНИЕ! LoaderCycle не предназначен для использования с типом модели Ensemble. Используйте LoaderWMCycle вместо этого.",
                            Fore::YELLOW.as_str()
                        );
                    }
                    _ => {}
                }
                let cycle = LoaderCycle::init(symbol.to_string(), client)
                    .await
                    .map_err(CycleError::from)?;
                sleep(Duration::from_secs(10)).await;

                match config.runtime.runtime_type {
                    RuntimeType::Realtime => cycle.run().await.map_err(CycleError::from)?,
                    RuntimeType::Backtest => {
                        cycle.run_backtest().await.map_err(CycleError::from)?
                    }
                }
            }
            CycleType::Loaderwm => {
                match config.model.model_struct {
                    ModelStructure::Single => {
                        println!(
                            "{}ВНИМАНИЕ! LoaderWMCycle не предназначен для использования с типом модели Single. Используйте LoaderCycle вместо этого.",
                            Fore::YELLOW.as_str()
                        );
                    }
                    _ => {}
                }
                let cycle = LoaderWMCycle::init(symbol.to_string(), client)
                    .await
                    .map_err(CycleError::from)?;
                sleep(Duration::from_secs(10)).await;

                match config.runtime.runtime_type {
                    RuntimeType::Realtime => cycle
                        .run(counter_tx, model_tx.as_ref())
                        .await
                        .map_err(CycleError::from)?,
                    RuntimeType::Backtest => cycle
                        .run_backtest(model_tx.as_ref())
                        .await
                        .map_err(CycleError::from)?,
                }
            }
            CycleType::Training => {
                let cycle = TrainingCycle::init(symbol.to_string(), client)
                    .await
                    .map_err(CycleError::from)?;
                sleep(Duration::from_secs(10)).await;

                let model = model_tx.as_ref().ok_or_else(|| {
                    CycleError::AnyhowError(anyhow::anyhow!(
                        "Model not initialized for Training cycle"
                    ))
                })?;
                match config.runtime.runtime_type {
                    RuntimeType::Realtime => cycle
                        .run(counter_tx, model)
                        .await
                        .map_err(CycleError::from)?,
                    RuntimeType::Backtest => {
                        cycle.run_backtest(model).await.map_err(CycleError::from)?
                    }
                }
            }
            CycleType::Sandbox => {
                let cycle = SandboxCycle::init(symbol.to_string(), client)
                    .await
                    .map_err(CycleError::from)?;
                sleep(Duration::from_secs(10)).await;

                let model = model_tx.as_ref().ok_or_else(|| {
                    CycleError::AnyhowError(anyhow::anyhow!(
                        "Model not initialized for Sandbox cycle"
                    ))
                })?;
                match config.runtime.runtime_type {
                    RuntimeType::Realtime => cycle
                        .run(counter_tx, model)
                        .await
                        .map_err(CycleError::from)?,
                    RuntimeType::Backtest => {
                        cycle.run_backtest(model).await.map_err(CycleError::from)?
                    }
                }
            }
        }
        Ok(())
    }
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

pub struct CounterActor {
    counters: Counters,
    inbox: mpsc::Receiver<CounterCommand>,
}

impl CounterActor {
    pub fn new(capacity: usize) -> (Self, mpsc::Sender<CounterCommand>) {
        let (tx, rx) = mpsc::channel(10);
        (
            Self {
                counters: Counters::new(capacity),
                inbox: rx,
            },
            tx,
        )
    }

    pub async fn run(mut self) {
        log_info("CounterActor запущен");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                CounterCommand::Increment { symbol, value } => {
                    let counter = &mut self.counters;
                    counter.get_mut(&symbol.to_uppercase()).push(value);
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
                    let acc = calculate_average_accuracy(values);
                    let _ = respond_to.send(acc);
                }

                CounterCommand::GetTotalShiftedAccuracy { window, respond_to } => {
                    let values = self.counters.symbols.values();
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
        data: DataMap,
        respond_to: oneshot::Sender<DataMap>,
    },
    Train {
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    HandleMistakes {
        true_data: DataMap,
        predicted_data: DataMap,
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    GetAccuracy {
        respond_to: oneshot::Sender<Option<DataMap>>,
    },
}

pub struct ModelActor {
    model: Arc<Mutex<Box<dyn Model + Send + Sync>>>,
    inbox: mpsc::Receiver<ModelCommand>,
}

impl ModelActor {
    pub fn new(model: Box<dyn Model + Send + Sync>) -> (Self, mpsc::Sender<ModelCommand>) {
        let (tx, rx) = mpsc::channel(10);
        (
            Self {
                model: Arc::new(Mutex::new(model)),
                inbox: rx,
            },
            tx,
        )
    }

    pub async fn run(mut self) {
        log_debug("ModelActor запущен");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                ModelCommand::Predict { data, respond_to } => {
                    log_debug(format!("{:#?}", &data).as_str());
                    let model = self.model.clone();
                    let result = model.lock().await.predict(data).await;

                    let prediction = match result {
                        Ok(pred) => pred,
                        Err(e) => {
                            log_error(&format!("Ошибка предсказания: {}", e));
                            DataMap::new("".to_string(), BTreeMap::new())
                        }
                    };

                    let _ = respond_to.send(prediction);
                }

                ModelCommand::Train { respond_to } => {
                    let result = {
                        let model = self.model.clone();

                        let mut locked = model.lock().await;
                        locked.train().await
                    };

                    let _ = respond_to.send(result);
                }

                ModelCommand::HandleMistakes {
                    true_data,
                    predicted_data,
                    respond_to,
                } => {
                    let result = {
                        if true_data.is_empty() {
                            Err(anyhow::anyhow!("True data is empty!"))
                        } else if predicted_data.is_empty() {
                            Err(anyhow::anyhow!("Predicted data is empty!"))
                        } else if true_data.len() != predicted_data.len() {
                            Err(anyhow::anyhow!("Data sizes do not match!"))
                        } else {
                            let model = self.model.clone();

                            let mut locked = model.lock().await;
                            locked.handle_mistakes(true_data, predicted_data).await
                        }
                    };

                    let _ = respond_to.send(result);
                }

                ModelCommand::GetAccuracy { respond_to } => {
                    let result = {
                        let model = self.model.clone();
                        let locked = model.lock().await;
                        locked.get_accuracy()
                    };

                    let _ = respond_to.send(result);
                }
            }
        }

        log_warning("ModelActor остановлен");
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
    config: Config,
    supervisor_tx: mpsc::Sender<SupervisorCommand>,
    counter_tx: mpsc::Sender<CounterCommand>,
    prediction_tx: mpsc::Sender<PredictionsCommand>,
    _counter_task: tokio::task::JoinHandle<()>,
    _supervisor_task: tokio::task::JoinHandle<()>,
    _servers_task: tokio::task::JoinHandle<()>,
    _prediction_task: tokio::task::JoinHandle<()>,
}

impl CycleManager {
    pub async fn new() -> Self {
        let config = load_config(CONFIG_PATH);
        let counter_capacity = config.behaviour.accuracy_capacity;
        let prediction_capacity = config.behaviour.predictions_capacity;

        let (servers_actor, servers_tx) = ServersActor::new().await;
        let servers_task = tokio::spawn(servers_actor.run());

        let (counter_actor, counter_tx) = CounterActor::new(counter_capacity);
        let counter_task = tokio::spawn(counter_actor.run());

        let (prediction_actor, prediction_tx) = PredictionsActor::new(prediction_capacity);
        let prediction_task = tokio::spawn(prediction_actor.run());

        let (supervisor, supervisor_tx) =
            CycleSupervisor::new(counter_tx.clone(), servers_tx.clone());
        let supervisor_task = tokio::spawn(supervisor.run());

        let background_cycle = BackgroundCycle::new(load_config(CONFIG_PATH), servers_tx);
        let _ = tokio::spawn(background_cycle.run());

        Self {
            config,
            supervisor_tx,
            counter_tx,
            prediction_tx,
            _counter_task: counter_task,
            _supervisor_task: supervisor_task,
            _servers_task: servers_task,
            _prediction_task: prediction_task,
        }
    }

    pub async fn run_all(
        &mut self,
        symbols: Vec<String>,
        cycle_types: HashMap<String, CycleType>,
    ) -> Result<(), anyhow::Error> {
        let needs_model = symbols.iter().any(|symbol| {
            matches!(
                cycle_types.get(symbol).unwrap_or(&CycleType::Loader),
                CycleType::Training | CycleType::Sandbox
            ) | (matches!(
                cycle_types.get(symbol).unwrap_or(&CycleType::Loader),
                CycleType::Loaderwm
            ) && self.config.runtime.with_model)
        });

        if needs_model {
            self.initialize_model()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
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

        let params = load_config(CONFIG_PATH).model.params;

        let mut model: Box<dyn Model + Send + Sync> = match params {
            ModelParams::Ensemble {
                future_volatility_model_params,
                future_volume_model_params,
                future_trend_strength_model_params,
                future_range_model_params,
                future_return_mean_model_params,
                future_return_std_model_params,
                future_return_skew_model_params,
                future_return_kurt_model_params,
                risk_score_model_params,
                drawdown_probability_model_params,
                tail_event_probability_model_params,
                volatility_spike_probability_model_params,
                liquidity_drop_probability_model_params,
                future_return_model_params,
                action_type_model_params,
                position_size_model_params,
            } => init_ensemble_model(
                Some(self.prediction_tx.clone()),
                pool.clone(),
                future_volatility_model_params,
                future_volume_model_params,
                future_trend_strength_model_params,
                future_range_model_params,
                future_return_mean_model_params,
                future_return_std_model_params,
                future_return_skew_model_params,
                future_return_kurt_model_params,
                risk_score_model_params,
                drawdown_probability_model_params,
                tail_event_probability_model_params,
                volatility_spike_probability_model_params,
                liquidity_drop_probability_model_params,
                future_return_model_params,
                action_type_model_params,
                position_size_model_params,
            ),
            ModelParams::Single { params } => init_single_model(
                params,
                Some(self.prediction_tx.clone()),
                SQLStandart::SingleModel,
                pool,
            ),
        };

        model
            .train()
            .await
            .map_err(|e| format!("Model training error: {}", e))?;

        let (model_actor, model_tx) = ModelActor::new(model);
        tokio::spawn(model_actor.run());

        self.supervisor_tx
            .send(SupervisorCommand::SetModel { model_tx })
            .await
            .map_err(|_| "Не удалось обновить модель в Supervisor")?;

        Ok(())
    }

    pub async fn add_cycle(
        &self,
        symbol: String,
        cycle_type: CycleType,
    ) -> Result<(), anyhow::Error> {
        let (tx, rx) = oneshot::channel();
        self.supervisor_tx
            .send(SupervisorCommand::StartCycle {
                symbol,
                cycle_type,
                respond_to: tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("Supervisor недоступен"))?;

        rx.await
            .map_err(|_| anyhow::anyhow!("Нет ответа от Supervisor"))?
    }

    pub fn counter_handle(&self) -> mpsc::Sender<CounterCommand> {
        self.counter_tx.clone()
    }

    pub fn supervisor_handle(&self) -> mpsc::Sender<SupervisorCommand> {
        self.supervisor_tx.clone()
    }

    pub fn prediction_handle(&self) -> mpsc::Sender<PredictionsCommand> {
        self.prediction_tx.clone()
    }
}

pub enum PredictionsCommand {
    AddPrediction {
        symbol: String,
        prediction: f64,
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    ListPredictions {
        respond_to: oneshot::Sender<Option<HashMap<String, SymbolCounters<f64>>>>,
    },
    GetLastPrediction {
        symbol: String,
        respond_to: oneshot::Sender<Option<f64>>,
    },
    GetPredictions {
        symbol: String,
        respond_to: oneshot::Sender<Option<SymbolCounters<f64>>>,
    },
}

pub struct PredictionsActor {
    capacity: usize,
    predictions: HashMap<String, SymbolCounters<f64>>,
    inbox: mpsc::Receiver<PredictionsCommand>,
}

impl PredictionsActor {
    pub fn new(capacity: usize) -> (Self, mpsc::Sender<PredictionsCommand>) {
        let (tx, rx) = mpsc::channel(10);

        (
            Self {
                capacity,
                predictions: HashMap::new(),
                inbox: rx,
            },
            tx,
        )
    }

    pub async fn run(mut self) {
        log_info("PredictionsActor запущен");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                PredictionsCommand::AddPrediction {
                    symbol,
                    prediction,
                    respond_to,
                } => {
                    let pred_counter = self
                        .predictions
                        .entry(symbol)
                        .or_insert_with(|| SymbolCounters::new(self.capacity));
                    pred_counter.push(prediction);
                    let _ = respond_to.send(Ok(()));
                }
                PredictionsCommand::GetLastPrediction { symbol, respond_to } => {
                    let pred_counter = self.predictions.get(&symbol);
                    if let Some(counter) = pred_counter {
                        let _ = respond_to.send(counter.data.back().cloned());
                    } else {
                        let _ = respond_to.send(None);
                    }
                }
                PredictionsCommand::GetPredictions { symbol, respond_to } => {
                    let pred_counter = self.predictions.get(&symbol);
                    let _ = respond_to.send(pred_counter.cloned());
                }
                PredictionsCommand::ListPredictions { respond_to } => {
                    let _ = respond_to.send(Some(self.predictions.clone()));
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ServerState {
    pub active: bool,
    pub workload: u8,
}

pub enum ServersCommand {
    #[allow(unused)]
    ListActive {
        respond_to: oneshot::Sender<Option<Vec<String>>>,
    },
    GetPriority {
        respond_to: oneshot::Sender<Option<String>>,
    },
    RemoveAllWorkload {
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    FetchOhlcv {
        symbol: String,
        timeframe: String,
        limit: usize,
        exchange_name: String,
        server: String,
        respond_to: oneshot::Sender<Result<Vec<Candle>, anyhow::Error>>,
    },
    FetchOhlcvWithTimestamps {
        symbol: String,
        timeframe: String,
        limit: usize,
        exchange_name: String,
        server: String,
        respond_to: oneshot::Sender<Result<Vec<CandleWithTimestamp>, anyhow::Error>>,
    },
    #[allow(unused)]
    FetchTicker {
        symbol: String,
        exchange_name: String,
        server: String,
        respond_to: oneshot::Sender<Result<Ticker, anyhow::Error>>,
    },
    TestSymbol {
        symbol: String,
        exchange_name: String,
        server: String,
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    UpdateActive {
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    }, // ... TODO: update enum, after creating account logic
}

async fn test_server(server: &str) -> bool {
    reqwest::Client::new()
        .get(format!("http://{}/", server))
        .send()
        .await
        .is_ok()
}

pub struct ServersActor {
    servers: HashMap<String, ServerState>,
    inbox: mpsc::Receiver<ServersCommand>,
}

impl ServersActor {
    pub async fn new() -> (Self, mpsc::Sender<ServersCommand>) {
        let (tx, rx) = mpsc::channel(10);

        let servers_vec = load_config(CONFIG_PATH).servers;

        let mut servers = HashMap::new();

        for server in servers_vec {
            let active = test_server(&server).await;
            servers.insert(
                server,
                ServerState {
                    active,
                    workload: 0,
                },
            );
        }

        if !servers.values().any(|s| s.active) {
            panic!("Нет ни одного активного сервера");
        }

        (Self { servers, inbox: rx }, tx)
    }

    pub async fn run(mut self) {
        log_info("ServersActor запущен");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                ServersCommand::RemoveAllWorkload { respond_to } => {
                    let result = self.remove_all_workload();
                    let _ = respond_to.send(result);
                }
                ServersCommand::ListActive { respond_to } => {
                    let result = self.list_active();
                    let _ = respond_to.send(result);
                }
                ServersCommand::UpdateActive { respond_to } => {
                    let result = self.update_active().await;
                    let _ = respond_to.send(result);
                }
                ServersCommand::GetPriority { respond_to } => {
                    let result = self.get_priority();
                    let _ = respond_to.send(result);
                }
                ServersCommand::FetchOhlcv {
                    symbol,
                    timeframe,
                    limit,
                    exchange_name,
                    server,
                    respond_to,
                } => {
                    let result = self
                        .fetch_ohlcv(&symbol, &timeframe, limit, &exchange_name, &server)
                        .await;
                    let _ = respond_to.send(result);
                }
                ServersCommand::FetchOhlcvWithTimestamps {
                    symbol,
                    timeframe,
                    limit,
                    exchange_name,
                    server,
                    respond_to,
                } => {
                    let result = self
                        .fetch_ohlcv_with_timestamps(
                            &symbol,
                            &timeframe,
                            limit,
                            &exchange_name,
                            &server,
                        )
                        .await;
                    let _ = respond_to.send(result);
                }
                ServersCommand::FetchTicker {
                    symbol,
                    exchange_name,
                    server,
                    respond_to,
                } => {
                    let result = self.fetch_ticker(&symbol, &exchange_name, &server).await;
                    let _ = respond_to.send(result);
                }
                ServersCommand::TestSymbol {
                    symbol,
                    exchange_name,
                    server,
                    respond_to,
                } => {
                    let result = self.test_symbol(&symbol, &exchange_name, &server).await;
                    let _ = respond_to.send(result);
                }
            }
        }
    }

    fn add_workload(&mut self, server: String, num: u8) -> Result<(), anyhow::Error> {
        let state = self
            .servers
            .get_mut(&server)
            .ok_or_else(|| anyhow::anyhow!("Сервер не найден"))?;

        if !state.active {
            return Err(anyhow::anyhow!("Сервер не активен"));
        }

        state.workload = state.workload.saturating_add(num);
        Ok(())
    }

    fn remove_all_workload(&mut self) -> Result<(), anyhow::Error> {
        for state in self.servers.values_mut() {
            state.workload = 0;
        }
        Ok(())
    }

    fn list_active(&self) -> Option<Vec<String>> {
        let active: Vec<String> = self
            .servers
            .iter()
            .filter(|(_, s)| s.active)
            .map(|(k, _)| k.clone())
            .collect();

        if active.is_empty() {
            None
        } else {
            Some(active)
        }
    }

    async fn update_active(&mut self) -> Result<(), anyhow::Error> {
        for (server, state) in self.servers.iter_mut() {
            let is_active = test_server(server).await;
            state.active = is_active;
        }
        Ok(())
    }

    fn get_priority(&self) -> Option<String> {
        let mut active: Vec<(&String, &ServerState)> =
            self.servers.iter().filter(|(_, s)| s.active).collect();

        if active.is_empty() {
            return None;
        }

        if active.len() == 1 {
            return Some(active[0].0.clone());
        }

        active.sort_by_key(|(_, s)| s.workload);

        Some(active[0].0.clone())
    }

    fn mark_server_inactive(&mut self, server: &str) {
        if let Some(state) = self.servers.get_mut(server) {
            state.active = false;
        }
    }

    async fn fetch_ohlcv(
        &mut self,
        symbol: &str,
        timeframe: &str,
        limit: usize,
        exchange_name: &str,
        server: &str,
    ) -> Result<Vec<Candle>, anyhow::Error> {
        let mut current_server = server.to_string();

        loop {
            let payload = serde_json::json!({
                "exchange_name": exchange_name,
                "symbol": parse_symbol(symbol),
                "timeframe": timeframe,
                "limit": limit
            });

            let res = match reqwest::Client::new()
                .post(format!("http://{}/exchange/fetch/ohlcv", current_server))
                .json(&payload)
                .send()
                .await
            {
                Ok(ohlcv) => ohlcv,
                Err(e) => {
                    log_error(format!("{}", e).as_str());
                    self.mark_server_inactive(&current_server);

                    current_server = self
                        .get_priority()
                        .ok_or_else(|| anyhow::anyhow!("Нет активных серверов"))?;

                    continue;
                }
            };

            let body: ApiResponse<serde_json::Value> = res
                .json()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to parse JSON response: {}", e))?;
            self.add_workload(current_server.clone(), limit as u8)?;
            if !body.success {
                return Err(anyhow::anyhow!(body.message.unwrap_or("".to_string())));
            }
            let raw_ohlcv = match body.data {
                Some(candles) => candles,
                None => return Err(anyhow::anyhow!("Data is None!")),
            };

            let candles = raw_ohlcv
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("ohlcv is not an array"))?
                .iter()
                .map(|item| {
                    let arr = item
                        .as_array()
                        .ok_or_else(|| anyhow::anyhow!("ohlcv item is not an array"))?;

                    if arr.len() < 6 {
                        return Err(anyhow::anyhow!("ohlcv item has less than 6 elements"));
                    }

                    Ok(Candle {
                        open: arr[1]
                            .as_f64()
                            .ok_or_else(|| anyhow::anyhow!("open is not a number"))?,
                        high: arr[2]
                            .as_f64()
                            .ok_or_else(|| anyhow::anyhow!("high is not a number"))?,
                        low: arr[3]
                            .as_f64()
                            .ok_or_else(|| anyhow::anyhow!("low is not a number"))?,
                        close: arr[4]
                            .as_f64()
                            .ok_or_else(|| anyhow::anyhow!("close is not a number"))?,
                        volume: arr[5]
                            .as_f64()
                            .ok_or_else(|| anyhow::anyhow!("volume is not a number"))?,
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?;

            return Ok(candles);
        }
    }

    async fn fetch_ohlcv_with_timestamps(
        &mut self,
        symbol: &str,
        timeframe: &str,
        limit: usize,
        exchange_name: &str,
        server: &str,
    ) -> Result<Vec<CandleWithTimestamp>, anyhow::Error> {
        let mut current_server = server.to_string();

        loop {
            let payload = serde_json::json!({
                "exchange_name": exchange_name,
                "symbol": parse_symbol(symbol),
                "timeframe": timeframe,
                "limit": limit
            });

            let res = match reqwest::Client::new()
                .post(format!("http://{}/exchange/fetch/ohlcv", current_server))
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => response,
                Err(e) => {
                    log_error(format!("{}", e).as_str());
                    self.mark_server_inactive(&current_server);

                    current_server = self
                        .get_priority()
                        .ok_or_else(|| anyhow::anyhow!("Нет активных серверов"))?;

                    continue;
                }
            };

            let body: ApiResponse<serde_json::Value> = res
                .json()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to parse JSON response: {}", e))?;
            self.add_workload(current_server.clone(), limit as u8)?;
            if !body.success {
                return Err(anyhow::anyhow!(body.message.unwrap_or("".to_string())));
            }
            let raw_ohlcv = match body.data {
                Some(candles) => candles,
                None => return Err(anyhow::anyhow!("Data is None!")),
            };

            let candles = raw_ohlcv
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("ohlcv is not an array"))?
                .iter()
                .map(|item| {
                    let arr = item
                        .as_array()
                        .ok_or_else(|| anyhow::anyhow!("ohlcv item is not an array"))?;

                    if arr.len() < 6 {
                        return Err(anyhow::anyhow!("ohlcv item has less than 6 elements"));
                    }

                    Ok(CandleWithTimestamp {
                        timestamp: arr[0]
                            .as_u64()
                            .ok_or_else(|| anyhow::anyhow!("timestamp is not a number"))?,
                        open: arr[1]
                            .as_f64()
                            .ok_or_else(|| anyhow::anyhow!("open is not a number"))?,
                        high: arr[2]
                            .as_f64()
                            .ok_or_else(|| anyhow::anyhow!("high is not a number"))?,
                        low: arr[3]
                            .as_f64()
                            .ok_or_else(|| anyhow::anyhow!("low is not a number"))?,
                        close: arr[4]
                            .as_f64()
                            .ok_or_else(|| anyhow::anyhow!("close is not a number"))?,
                        volume: arr[5]
                            .as_f64()
                            .ok_or_else(|| anyhow::anyhow!("volume is not a number"))?,
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?;

            return Ok(candles);
        }
    }

    async fn fetch_ticker(
        &mut self,
        symbol: &str,
        exchange_name: &str,
        server: &str,
    ) -> Result<Ticker, anyhow::Error> {
        let mut current_server = server.to_string();

        loop {
            let payload = serde_json::json!({
                "exchange_name": exchange_name,
                "symbol": parse_symbol(symbol)
            });

            let res = match reqwest::Client::new()
                .post(format!("http://{}/exchange/fetch/ticker", current_server))
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => response,
                Err(e) => {
                    eprintln!("{}", e);
                    self.mark_server_inactive(&current_server);

                    current_server = self
                        .get_priority()
                        .ok_or_else(|| anyhow::anyhow!("Нет активных серверов"))?;

                    continue;
                }
            };

            let body: ApiResponse<serde_json::Value> = res
                .json()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to parse JSON response: {}", e))?;
            self.add_workload(current_server.clone(), 1)?;
            if !body.success {
                return Err(anyhow::anyhow!(body.message.unwrap_or("".to_string())));
            }
            let data = body
                .data
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Response data is None!"))?;
            let bid = data
                .get("bid")
                .ok_or_else(|| anyhow::anyhow!("bid field is missing"))?
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("bid is not a number"))?;
            let ask = data
                .get("ask")
                .ok_or_else(|| anyhow::anyhow!("ask field is missing"))?
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("ask is not a number"))?;

            return Ok(Ticker { bid, ask });
        }
    }

    async fn test_symbol(
        &mut self,
        symbol: &str,
        exchange_name: &str,
        server: &str,
    ) -> Result<(), anyhow::Error> {
        let payload = serde_json::json!({
            "exchange_name": exchange_name,
            "symbol": parse_symbol(symbol)
        });

        let res = reqwest::Client::new()
            .post(format!("http://{}/exchange/fetch/ticker", server))
            .json(&payload)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send request: {}", e))?;
        self.add_workload(server.to_string(), 1)?;
        let body: ApiResponse<serde_json::Value> = res
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to parse JSON response: {}", e))?;

        if !body.success {
            return Err(anyhow::anyhow!(body.message.unwrap_or("".to_string())));
        }
        if body.data.is_some() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(body.message.unwrap_or("".to_string())))
        }
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
        info!(
            "{}[{}] {}{}",
            Fore::WHITE.as_str(),
            Utc::now().format("%H:%M:%S"),
            Fore::CYAN.as_str(),
            msg
        );
    }
}

fn log_debug(msg: &str) {
    if load_config(CONFIG_PATH).prints.manager.manager_init {
        debug!(
            "{}[{}] {}{}",
            Fore::WHITE.as_str(),
            Utc::now().format("%H:%M:%S"),
            Fore::CYAN.as_str(),
            msg
        );
    }
}

fn log_warning(msg: &str) {
    if load_config(CONFIG_PATH).prints.manager.manager_init {
        warn!(
            "{}[{}] {}{}",
            Fore::WHITE.as_str(),
            Utc::now().format("%H:%M:%S"),
            Fore::YELLOW.as_str(),
            msg
        );
    }
}

fn log_error(msg: &str) {
    error!("{}{}", Fore::RED.as_str(), msg);
}
