use crate::{
    CONFIG_PATH,
    backend::stucture::{ApiState, ApiStructure},
    engine::utils::config::load_config::load_config,
};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct Tokens {
    tokens: Vec<String>,
}

fn default_period() -> usize {
    load_config(CONFIG_PATH).data.accuracy_capacity
}

#[derive(Deserialize)]
pub struct Window {
    #[serde(default = "default_period")]
    period: usize,
}

pub async fn root() -> String {
    format!("{:#?}", ApiStructure::default())
}

pub async fn tokens(State(state): State<ApiState>) -> Json<Tokens> {
    let manager = state.manager.read().await;
    let active = manager.active_cycles().await;

    Json(Tokens { tokens: active })
}

pub async fn total_accuracy(
    State(state): State<ApiState>,
    Query(window): Query<Window>,
) -> Json<f64> {
    let counters = state.counters.lock().await;

    Json(
        counters
            .total
            .get_shifted_accuracy(window.period)
            .unwrap_or(0.0),
    )
}

pub async fn token_accuracy(
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
    Query(params): Query<Window>,
) -> Result<Json<f64>, StatusCode> {
    let counters_guard = state.counters.lock().await;
    match counters_guard.get_option(&symbol) {
        Some(counters) => {
            let acc = counters.get_shifted_accuracy(params.period).unwrap_or(0.0);
            Ok(Json(acc))
        }
        None => Ok(Json(0.0)),
    }
}

// #[derive(Debug, Deserialize)]
// pub struct AddCycleRequest {
//     hashed_password: String,
// }

// pub async fn add_cycle(
//     State(state): State<ApiState>,
//     Path(token): Path<String>,
//     Json(payload): Json<AddCycleRequest>,
// ) -> Result<Json<String>, StatusCode> {
//     if payload.hashed_password != state.hashed_password {
//         return Ok(Json("Неверный пароль".to_string()));
//     }
//     let mut manager = state.manager.write().await;

//     let success = manager
//         .add_cycle(
//             token.clone(),
//             CycleType::from_str(&load_config(CONFIG_PATH).cycle_type),
//         )
//         .await
//         .is_ok();

//     if success {
//         Ok(Json(format!("Цикл для токена '{}' успешно запущен", token)))
//     } else {
//         Ok(Json("Не удалось запустить цикл".to_string()))
//     }
// }
