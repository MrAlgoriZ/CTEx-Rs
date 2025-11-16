use crate::{
    CONFIG_PATH,
    backend::{
        commands,
        stucture::{ApiState, ApiStructure},
    },
    engine::{
        cycles::manager::CycleManager, state::counters::Counters,
        utils::config::load_config::load_config,
    },
};

use axum::{Router, routing::get};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub struct Api {
    listener: tokio::net::TcpListener,
    app: Router,
}

impl Api {
    pub async fn new(manager: Arc<RwLock<CycleManager>>, counters: Arc<Mutex<Counters>>) -> Self {
        Api {
            listener: tokio::net::TcpListener::bind(load_config(CONFIG_PATH).backend.listener)
                .await
                .unwrap(),
            app: Self::init_app(manager, counters).await,
        }
    }

    pub async fn init_app(
        manager: Arc<RwLock<CycleManager>>,
        counters: Arc<Mutex<Counters>>,
    ) -> Router {
        let structure = ApiStructure::default();
        let state = ApiState { manager, counters };

        Router::new()
            .route(&structure.root, get(commands::root))
            .route(&structure.active_tokens, get(commands::tokens))
            .route(&structure.total_accuracy, get(commands::total_accuracy))
            .route(&structure.token_accuracy, get(commands::token_accuracy))
            .with_state(state)
    }

    pub async fn run(self) {
        axum::serve(self.listener, self.app).await.unwrap();
    }
}
