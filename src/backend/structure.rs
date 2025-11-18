// ============================================================================
// backend/structure.rs
// ============================================================================
use crate::engine::cycles::manager::{CounterCommand, SupervisorCommand};
use serde::Serialize;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct ApiState {
    pub supervisor_handle: mpsc::Sender<SupervisorCommand>,
    pub counter_handle: mpsc::Sender<CounterCommand>,
}

#[derive(Debug, Serialize)]
pub struct ApiStructure {
    // Информационные
    pub root: String,
    pub health: String,

    // Управление циклами
    pub cycles_list: String,
    pub cycle_add: String,
    pub cycle_stop: String,
    pub cycles_stop_all: String,

    // Метрики
    pub accuracy_total: String,
    pub accuracy_token: String,
    pub accuracy_all_tokens: String,
}

impl Default for ApiStructure {
    fn default() -> Self {
        Self {
            // Информационные
            root: "/".to_string(),
            health: "/health".to_string(),

            // Управление циклами
            cycles_list: "/cycles".to_string(),
            cycle_add: "/cycles".to_string(),
            cycle_stop: "/cycles/{symbol}".to_string(),
            cycles_stop_all: "/cycles".to_string(),

            // Метрики
            accuracy_total: "/accuracy/total".to_string(),
            accuracy_token: "/accuracy/{symbol}".to_string(),
            accuracy_all_tokens: "/accuracy".to_string(),
        }
    }
}
