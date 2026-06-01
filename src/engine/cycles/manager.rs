use anyhow::anyhow;
use log::{error, info, warn};
use serde::Deserialize;
use sqlx::PgPool;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::{Duration, sleep};

use crate::data::data_interfaces::{Candle, CandleWithTimestamp, DataMap, Ticker};
use crate::data::requests::ccxt::client::CCXTClient;
use crate::data::requests::database::standart::SQLStandart;
use crate::engine::cycles::background::cycle::BackgroundCycle;
use crate::engine::cycles::loader::cycle::LoaderCycle;
use crate::engine::cycles::loaderwm::cycle::LoaderWMCycle;
use crate::engine::cycles::sandbox::cycle::SandboxCycle;
use crate::engine::cycles::training::cycle::TrainingCycle;
use crate::engine::state::chain::{Block, Chain};
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
    SetChain {
        chain_tx: mpsc::Sender<ChainCommand>,
    },
    ChainHandle {
        respond_to: oneshot::Sender<Option<mpsc::Sender<ChainCommand>>>,
    },
}

struct CycleSupervisor {
    workers: HashMap<String, WorkerHandle>,
    model_tx: Option<mpsc::Sender<ModelCommand>>,
    chain_tx: Option<mpsc::Sender<ChainCommand>>,
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
                chain_tx: None,
                counter_tx,
                server_tx,
                inbox: rx,
            },
            tx,
        )
    }

    async fn run(mut self) {
        info!("Supervisor has started!");

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
                    info!("Model has installed in Supervisor");
                }

                SupervisorCommand::SetChain { chain_tx } => {
                    self.chain_tx = Some(chain_tx);
                    info!("Chain has installed in Supervisor");
                }

                SupervisorCommand::ChainHandle { respond_to } => {
                    let _ = respond_to.send(self.chain_tx.clone());
                }
            }
        }

        warn!("Supervisor has stopped!");
    }

    async fn start_worker(
        &mut self,
        symbol: String,
        cycle_type: CycleType,
    ) -> Result<(), anyhow::Error> {
        if self.workers.contains_key(&symbol) {
            return Err(anyhow!(format!("Worker {} already running!", symbol)));
        }

        if matches!(cycle_type, CycleType::Training | CycleType::Sandbox) && self.model_tx.is_none()
        {
            return Err(anyhow!("Model is not initialized for cycle!"));
        }

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let model_tx = self.model_tx.clone();
        let counter_tx = self.counter_tx.clone();
        let server_tx = self.server_tx.clone();
        let chain_tx = self.chain_tx.clone();
        let symbol_clone = symbol.clone();

        let task = tokio::spawn(async move {
            Self::worker_loop(
                symbol_clone,
                cycle_type,
                counter_tx,
                server_tx,
                model_tx,
                chain_tx,
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

        info!("Worker {} has started ({:?})!", symbol, cycle_type);
        Ok(())
    }

    async fn stop_worker(&mut self, symbol: &str) -> Result<(), anyhow::Error> {
        match self.workers.remove(symbol) {
            Some(handle) => {
                handle.stop().await;
                Ok(())
            }
            None => Err(anyhow!(format!("Worker {} not found!", symbol))),
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
        chain_tx: Option<mpsc::Sender<ChainCommand>>,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) {
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    warn!("Worker {} received shutdown signal!", symbol);
                    break;
                }

                result = Self::run_cycle_once(&symbol, cycle_type, &counter_tx, &server_tx, &model_tx, &chain_tx) => {
                    match result {
                        Ok(_) => {
                            info!("Worker {} has finished normally", symbol);
                            if let Some(ctx) = &chain_tx {
                                let (tx, rx) = oneshot::channel();
                                let _ = ctx
                                    .send(ChainCommand::DeleteChain {
                                        symbol: symbol.clone(),
                                        respond_to: tx,
                                    })
                                    .await;
                                let _ = rx.await;
                            }
                            break;
                        }
                        Err(e) => {
                            match e {
                                CycleError::AnyhowError(err) => {
                                    error!("Worker {} crashed: {}, restarting in 5 sec", symbol, err);
                                    if let Some(ctx) = &chain_tx {
                                        let (tx, rx) = oneshot::channel();
                                        let _ = ctx
                                            .send(ChainCommand::DeleteChain {
                                                symbol: symbol.clone(),
                                                respond_to: tx,
                                            })
                                            .await;
                                        let _ = rx.await;
                                    }
                                    sleep(Duration::from_secs(5)).await;
                                }
                                CycleError::SymbolDoesNotExist => {
                                    error!("Token {} does not exist!", symbol);
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
        chain_tx: &Option<mpsc::Sender<ChainCommand>>,
    ) -> Result<(), CycleError> {
        let config = load_config();
        let client = CCXTClient::new(&config.exchange.main_exchange, server_tx.clone());

        match cycle_type {
            CycleType::Loader => {
                match config.model.model_struct {
                    ModelStructure::Ensemble => {
                        warn!(
                            "{}LoaderCycle not for Ensemble model type. Use LoaderWMCycle instead!",
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
                        warn!(
                            "{}LoaderWMCycle not for Single model type. Use LoaderCycle instead!",
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
                        .run(counter_tx, model_tx.as_ref(), chain_tx.as_ref())
                        .await
                        .map_err(CycleError::from)?,
                    RuntimeType::Backtest => cycle
                        .run_backtest(model_tx.as_ref(), chain_tx.as_ref())
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
                    CycleError::AnyhowError(anyhow!("Model not initialized for Training cycle!"))
                })?;
                match config.runtime.runtime_type {
                    RuntimeType::Realtime => cycle
                        .run(counter_tx, model, chain_tx.as_ref())
                        .await
                        .map_err(CycleError::from)?,
                    RuntimeType::Backtest => cycle
                        .run_backtest(model, chain_tx.as_ref())
                        .await
                        .map_err(CycleError::from)?,
                }
            }
            CycleType::Sandbox => {
                let cycle = SandboxCycle::init(symbol.to_string(), client)
                    .await
                    .map_err(CycleError::from)?;
                sleep(Duration::from_secs(10)).await;

                let model = model_tx.as_ref().ok_or_else(|| {
                    CycleError::AnyhowError(anyhow!("Model not initialized for Sandbox cycle!"))
                })?;
                match config.runtime.runtime_type {
                    RuntimeType::Realtime => cycle
                        .run(counter_tx, model, chain_tx.as_ref())
                        .await
                        .map_err(CycleError::from)?,
                    RuntimeType::Backtest => cycle
                        .run_backtest(model, chain_tx.as_ref())
                        .await
                        .map_err(CycleError::from)?,
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
        info!("CounterActor has started!");

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

        warn!("CounterActor has stopped!");
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
        info!("ModelActor has started!");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                ModelCommand::Predict { data, respond_to } => {
                    // debug!("{:#?}", &data);
                    let model = self.model.clone();
                    let result = model.lock().await.predict(data).await;

                    let prediction = match result {
                        Ok(pred) => pred,
                        Err(e) => {
                            error!("Prediction error: {}", e);
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

                    match result {
                        Ok(_) => {
                            let _ = respond_to.send(Ok(()));
                        }
                        Err(e) => {
                            let _ = respond_to.send(Err(e));
                        }
                    }
                }

                ModelCommand::HandleMistakes {
                    true_data,
                    predicted_data,
                    respond_to,
                } => {
                    let result = {
                        if true_data.is_empty() {
                            Err(anyhow!("True data is empty!"))
                        } else if predicted_data.is_empty() {
                            Err(anyhow!("Predicted data is empty!"))
                        } else if true_data.len() != predicted_data.len() {
                            Err(anyhow!("Data sizes do not match!"))
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

        warn!("ModelActor has stopped!");
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
        warn!("Worker {} has stopped!", self.symbol);
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
        let config = load_config();
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

        let background_cycle = BackgroundCycle::new(load_config(), servers_tx);
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
            self.initialize_model().await.map_err(|e| anyhow!(e))?;
        }

        if needs_model && self.config.model.generate_plots {
            self.initialize_chain().await.map_err(|e| anyhow!(e))?;
        }

        for symbol in &symbols {
            let cycle_type = cycle_types.get(symbol).unwrap_or(&CycleType::Loader);
            self.add_cycle(symbol.clone(), *cycle_type).await?;
        }

        info!("Started {} cycles: {}", symbols.len(), symbols.join(", "));

        Ok(())
    }

    async fn initialize_model(&self) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.supervisor_tx
            .send(SupervisorCommand::StopAll { respond_to: tx })
            .await
            .map_err(|_| "Supervisor is unavailable!")?;
        let _ = rx.await;

        let pool = PgPool::connect(&load_env().database_url)
            .await
            .map_err(|e| format!("Database connection error: {}", e))?;

        let params = load_config().model.params;

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
            .map_err(|_| "Failed to update model in Supervisor!")?;

        Ok(())
    }

    async fn initialize_chain(&self) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.supervisor_tx
            .send(SupervisorCommand::StopAll { respond_to: tx })
            .await
            .map_err(|_| "Supervisor is unavailable!")?;
        let _ = rx.await;

        let (chain_actor, chain_tx) = ChainActor::new();
        tokio::spawn(chain_actor.run());

        self.supervisor_tx
            .send(SupervisorCommand::SetChain { chain_tx })
            .await
            .map_err(|_| "Failed to update chain in Supervisor!")?;

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
            .map_err(|_| anyhow!("Supervisor is unavailable!"))?;

        rx.await
            .map_err(|_| anyhow!("No response from Supervisor!"))?
    }

    // Handles need only for REST API. If tx isn't need for API, don't do handle for this
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
        info!("PredictionsActor has started!");

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

pub enum ChainCommand {
    AddBlock {
        symbol: String,
        block: Block,
        respond_to: oneshot::Sender<()>,
    },
    DeleteChain {
        symbol: String,
        respond_to: oneshot::Sender<()>,
    },
    SavePlots {
        symbol: String,
        respond_to: oneshot::Sender<Result<(), anyhow::Error>>,
    },
}

pub struct ChainActor {
    chains: Chain,
    inbox: mpsc::Receiver<ChainCommand>,
}

impl ChainActor {
    pub fn new() -> (Self, mpsc::Sender<ChainCommand>) {
        let (tx, rx) = mpsc::channel(1000);

        let chains = Chain::new();

        (Self { chains, inbox: rx }, tx)
    }

    pub async fn run(mut self) {
        info!("ChainActor has started!");

        while let Some(cmd) = self.inbox.recv().await {
            match cmd {
                ChainCommand::AddBlock {
                    respond_to,
                    symbol,
                    block,
                } => {
                    let result = self.chains.add_block(&symbol, block);
                    let _ = respond_to.send(result);
                }
                ChainCommand::DeleteChain { symbol, respond_to } => {
                    let result = self.chains.delete_chain(&symbol);
                    let _ = respond_to.send(result);
                }
                ChainCommand::SavePlots { symbol, respond_to } => {
                    let result = self.chains.save_plots(&symbol);
                    let _ = respond_to.send(result);
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
    },
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

        let servers_vec = load_config().exchange.servers;

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
            panic!("No active servers available!");
        }

        (Self { servers, inbox: rx }, tx)
    }

    pub async fn run(mut self) {
        info!("ServersActor has started!");

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
            .ok_or_else(|| anyhow!("Server not found!"))?;

        if !state.active {
            return Err(anyhow!("Server is inactive!"));
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
                    error!("{}", e);
                    self.mark_server_inactive(&current_server);

                    current_server = self
                        .get_priority()
                        .ok_or_else(|| anyhow!("All servers are inactive!"))?;

                    continue;
                }
            };

            let body: ApiResponse<serde_json::Value> = res
                .json()
                .await
                .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))?;
            self.add_workload(current_server.clone(), limit as u8)?;
            if !body.success {
                return Err(anyhow!(body.message.unwrap_or("".to_string())));
            }
            let raw_ohlcv = match body.data {
                Some(candles) => candles,
                None => return Err(anyhow!("Data is None!")),
            };

            let candles = raw_ohlcv
                .as_array()
                .ok_or_else(|| anyhow!("ohlcv is not an array"))?
                .iter()
                .map(|item| {
                    let arr = item
                        .as_array()
                        .ok_or_else(|| anyhow!("ohlcv item is not an array"))?;

                    if arr.len() < 6 {
                        return Err(anyhow!("ohlcv item has less than 6 elements"));
                    }

                    Ok(Candle {
                        open: arr[1]
                            .as_f64()
                            .ok_or_else(|| anyhow!("open is not a number"))?,
                        high: arr[2]
                            .as_f64()
                            .ok_or_else(|| anyhow!("high is not a number"))?,
                        low: arr[3]
                            .as_f64()
                            .ok_or_else(|| anyhow!("low is not a number"))?,
                        close: arr[4]
                            .as_f64()
                            .ok_or_else(|| anyhow!("close is not a number"))?,
                        volume: arr[5]
                            .as_f64()
                            .ok_or_else(|| anyhow!("volume is not a number"))?,
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
                    error!("{}", e);
                    self.mark_server_inactive(&current_server);

                    current_server = self
                        .get_priority()
                        .ok_or_else(|| anyhow!("All servers are inactive!"))?;

                    continue;
                }
            };

            let body: ApiResponse<serde_json::Value> = res
                .json()
                .await
                .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))?;
            self.add_workload(current_server.clone(), limit as u8)?;
            if !body.success {
                return Err(anyhow!(body.message.unwrap_or("".to_string())));
            }
            let raw_ohlcv = match body.data {
                Some(candles) => candles,
                None => return Err(anyhow!("Data is None!")),
            };

            let candles = raw_ohlcv
                .as_array()
                .ok_or_else(|| anyhow!("ohlcv is not an array"))?
                .iter()
                .map(|item| {
                    let arr = item
                        .as_array()
                        .ok_or_else(|| anyhow!("ohlcv item is not an array"))?;

                    if arr.len() < 6 {
                        return Err(anyhow!("ohlcv item has less than 6 elements"));
                    }

                    Ok(CandleWithTimestamp {
                        timestamp: arr[0]
                            .as_u64()
                            .ok_or_else(|| anyhow!("timestamp is not a number"))?,
                        open: arr[1]
                            .as_f64()
                            .ok_or_else(|| anyhow!("open is not a number"))?,
                        high: arr[2]
                            .as_f64()
                            .ok_or_else(|| anyhow!("high is not a number"))?,
                        low: arr[3]
                            .as_f64()
                            .ok_or_else(|| anyhow!("low is not a number"))?,
                        close: arr[4]
                            .as_f64()
                            .ok_or_else(|| anyhow!("close is not a number"))?,
                        volume: arr[5]
                            .as_f64()
                            .ok_or_else(|| anyhow!("volume is not a number"))?,
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
                    error!("{}", e);
                    self.mark_server_inactive(&current_server);

                    current_server = self
                        .get_priority()
                        .ok_or_else(|| anyhow!("All servers are inactive!"))?;

                    continue;
                }
            };

            let body: ApiResponse<serde_json::Value> = res
                .json()
                .await
                .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))?;
            self.add_workload(current_server.clone(), 1)?;
            if !body.success {
                return Err(anyhow!(body.message.unwrap_or("".to_string())));
            }
            let data = body
                .data
                .as_ref()
                .ok_or_else(|| anyhow!("Response data is None!"))?;
            let bid = data
                .get("bid")
                .ok_or_else(|| anyhow!("bid field is missing"))?
                .as_f64()
                .ok_or_else(|| anyhow!("bid is not a number"))?;
            let ask = data
                .get("ask")
                .ok_or_else(|| anyhow!("ask field is missing"))?
                .as_f64()
                .ok_or_else(|| anyhow!("ask is not a number"))?;

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
            .map_err(|e| anyhow!("Failed to send request: {}", e))?;
        self.add_workload(server.to_string(), 1)?;
        let body: ApiResponse<serde_json::Value> = res
            .json()
            .await
            .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))?;

        if !body.success {
            return Err(anyhow!(body.message.unwrap_or("".to_string())));
        }
        if body.data.is_some() {
            Ok(())
        } else {
            Err(anyhow!(body.message.unwrap_or("".to_string())))
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
