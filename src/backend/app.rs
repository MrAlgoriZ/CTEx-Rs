use crate::data::requests::ccxt::binance::BinanceClient;
use crate::{
    CONFIG_PATH,
    backend::{
        commands,
        structure::{ApiState, ApiStructure},
    },
    engine::{
        cycles::manager::{CounterCommand, SupervisorCommand},
        utils::config::load_config::load_config,
    },
};
use axum::{
    Router,
    routing::{delete, get, post},
};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct Api {
    listener: tokio::net::TcpListener,
    app: Router,
}

impl Api {
    pub async fn new(
        supervisor_handle: mpsc::Sender<SupervisorCommand>,
        counter_handle: mpsc::Sender<CounterCommand>,
    ) -> Self {
        let config = load_config(CONFIG_PATH);
        let listener = tokio::net::TcpListener::bind(&config.backend.listener)
            .await
            .expect(&format!("Failed to bind to {}", config.backend.listener));

        Api {
            listener,
            app: Self::init_app(supervisor_handle, counter_handle).await,
        }
    }

    async fn init_app(
        supervisor_handle: mpsc::Sender<SupervisorCommand>,
        counter_handle: mpsc::Sender<CounterCommand>,
    ) -> Router {
        let structure = ApiStructure::default();
        let client = Arc::new(BinanceClient::new().await);
        let state = ApiState {
            supervisor_handle,
            counter_handle,
            client,
        };

        Router::new()
            .route(&structure.root, get(commands::root))
            .route(&structure.health, get(commands::health))
            .route(&structure.cycles_list, get(commands::cycles_list))
            .route(&structure.cycle_add, post(commands::cycle_add))
            .route(&structure.cycle_stop, delete(commands::cycle_stop))
            .route(
                &structure.cycles_stop_all,
                delete(commands::cycles_stop_all),
            )
            .route(&structure.accuracy_total, get(commands::accuracy_total))
            .route(&structure.accuracy_token, get(commands::accuracy_token))
            .route(
                &structure.accuracy_all_tokens,
                get(commands::accuracy_all_tokens),
            )
            .with_state(state)
    }

    pub async fn run(self) {
        axum::serve(self.listener, self.app)
            .await
            .expect("API server failed");
    }
}
