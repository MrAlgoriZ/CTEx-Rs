use crate::CONFIG_PATH;
use crate::backend::commands;
use crate::backend::structure::{ApiState, ApiStructure};
use crate::engine::cycles::manager::{CounterCommand, PredictionsCommand, SupervisorCommand};
use crate::engine::utils::config::load_config::load_config;

use anyhow::anyhow;
use axum::Router;
use axum::routing::{delete, get, post};
use tokio::sync::mpsc;

pub struct Api {
    listener: tokio::net::TcpListener,
    app: Router,
}

impl Api {
    pub async fn new(
        supervisor_handle: mpsc::Sender<SupervisorCommand>,
        counter_handle: mpsc::Sender<CounterCommand>,
        prediction_handle: mpsc::Sender<PredictionsCommand>,
    ) -> Result<Self, anyhow::Error> {
        let config = load_config(CONFIG_PATH);
        let listener = tokio::net::TcpListener::bind(&config.backend.listener)
            .await
            .map_err(|_| anyhow!(format!("Failed to bind to {}", config.backend.listener)))?;

        Ok(Api {
            listener,
            app: Self::init_app(supervisor_handle, counter_handle, prediction_handle),
        })
    }

    fn init_app(
        supervisor_handle: mpsc::Sender<SupervisorCommand>,
        counter_handle: mpsc::Sender<CounterCommand>,
        prediction_handle: mpsc::Sender<PredictionsCommand>,
    ) -> Router {
        let structure = ApiStructure::default();
        let state = ApiState {
            supervisor_handle,
            counter_handle,
            prediction_handle,
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
            .route(
                &structure.get_last_prediction,
                get(commands::get_last_prediction),
            )
            .route(&structure.predictions_list, get(commands::predictions_list))
            .route(
                &structure.all_predictions_list,
                get(commands::all_predictions_list),
            )
            .route(&structure.generate_plots, post(commands::generate_plots))
            .with_state(state)
    }

    pub async fn run(self) {
        axum::serve(self.listener, self.app)
            .await
            .expect("API server failed");
    }
}
