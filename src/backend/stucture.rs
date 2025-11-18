use crate::engine::{cycles::manager::CycleManager, state::counters::Counters};

use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug)]
pub struct ApiStructure {
    pub root: String,
    pub active_tokens: String,
    pub total_accuracy: String,
    pub token_accuracy: String,
}

impl Default for ApiStructure {
    fn default() -> Self {
        ApiStructure {
            root: "/".to_string(),
            active_tokens: "/tokens".to_string(),
            total_accuracy: "/accuracy/total".to_string(),
            token_accuracy: "/accuracy/token/{token}".to_string(),
        }
    }
}

#[derive(Clone)]
pub struct ApiState {
    pub manager: Arc<RwLock<CycleManager>>,
    pub counters: Arc<Mutex<Counters>>,
}
