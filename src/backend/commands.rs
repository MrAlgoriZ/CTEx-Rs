use std::collections::HashMap;

use crate::backend::structure::{ApiState, ApiStructure};
use crate::engine::cycles::manager::{
    ChainCommand, CounterCommand, PredictionsCommand, SupervisorCommand,
};
use crate::engine::utils::config::config_types::CycleType;
use crate::engine::utils::config::load_config::load_config;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

fn verify_password(input: String) -> bool {
    let cfg = load_config();
    input == cfg.backend.admin_password
}

#[derive(Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub message: Option<String>,
}

impl<T> ApiResponse<T> {
    fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: None,
        }
    }

    fn error(message: String) -> Self {
        Self {
            success: false,
            data: None,
            message: Some(message),
        }
    }
}

#[derive(Deserialize)]
pub struct AddCycleRequest {
    pub symbol: String,
    #[serde(rename = "type")]
    pub cycle_type: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct AccuracyQuery {
    #[serde(default = "default_window")]
    pub window: usize,
    #[serde(rename = "type")]
    pub counter_type: String,
}

#[derive(Deserialize)]
pub struct SymbolQuery {
    pub symbol: String,
}

fn default_window() -> usize {
    load_config().behaviour.accuracy_capacity
}

#[derive(Serialize)]
pub struct CycleInfo {
    pub symbol: String,
}

#[derive(Serialize)]
pub struct AccuracyInfo {
    pub symbol: String,
    pub accuracy: f64,
    pub window: usize,
    #[serde(rename = "type")]
    pub counter_type: String,
}

#[derive(Serialize)]
pub struct HealthStatus {
    pub status: String,
    pub version: String,
}

#[derive(Deserialize)]
pub struct PasswordRequest {
    pub password: String,
}

pub async fn root() -> Json<ApiResponse<ApiStructure>> {
    Json(ApiResponse::success(ApiStructure::default()))
}

pub async fn health() -> Json<ApiResponse<HealthStatus>> {
    Json(ApiResponse::success(HealthStatus {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    }))
}

pub async fn cycles_list(
    State(state): State<ApiState>,
) -> Result<Json<ApiResponse<Vec<CycleInfo>>>, StatusCode> {
    let (tx, rx) = oneshot::channel();

    state
        .supervisor_handle
        .send(SupervisorCommand::ListActive { respond_to: tx })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let active = rx.await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let cycles: Vec<CycleInfo> = active
        .into_iter()
        .map(|symbol| CycleInfo { symbol })
        .collect();

    Ok(Json(ApiResponse::success(cycles)))
}

pub async fn cycle_add(
    State(state): State<ApiState>,
    Json(payload): Json<AddCycleRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    if !verify_password(payload.password) {
        return Ok(Json(ApiResponse::error("Неверный пароль".to_string())));
    }

    let cycle_type = match payload.cycle_type.to_lowercase().as_str() {
        "training" => CycleType::Training,
        "loader" => CycleType::Loader,
        "loaderwm" => CycleType::Loaderwm,
        "sandbox" => CycleType::Sandbox,
        _ => {
            return Ok(Json(ApiResponse::error(
                "Тип цикла должен быть 'training', 'loader', 'loaderwm' или 'sandbox'".to_string(),
            )));
        }
    };

    let (tx, rx) = oneshot::channel();

    state
        .supervisor_handle
        .send(SupervisorCommand::StartCycle {
            symbol: payload.symbol.clone(),
            cycle_type,
            respond_to: tx,
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match rx.await {
        Ok(Ok(())) => Ok(Json(ApiResponse::success(format!(
            "Цикл {} успешно запущен",
            payload.symbol
        )))),
        Ok(Err(e)) => Ok(Json(ApiResponse::error(e.to_string()))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn cycle_stop(
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
    Json(payload): Json<PasswordRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    if !verify_password(payload.password) {
        return Ok(Json(ApiResponse::error("Неверный пароль".to_string())));
    }

    let (tx, rx) = oneshot::channel();

    state
        .supervisor_handle
        .send(SupervisorCommand::StopCycle {
            symbol: symbol.clone(),
            respond_to: tx,
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match rx.await {
        Ok(Ok(())) => Ok(Json(ApiResponse::success(format!(
            "Цикл {} остановлен",
            symbol
        )))),
        Ok(Err(e)) => Ok(Json(ApiResponse::error(e.to_string()))),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

pub async fn cycles_stop_all(
    State(state): State<ApiState>,
    Json(payload): Json<PasswordRequest>,
) -> Result<Json<ApiResponse<String>>, StatusCode> {
    if !verify_password(payload.password) {
        return Ok(Json(ApiResponse::error("Неверный пароль".to_string())));
    }

    let (tx, rx) = oneshot::channel();

    state
        .supervisor_handle
        .send(SupervisorCommand::StopAll { respond_to: tx })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    rx.await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(ApiResponse::success(
        "Все циклы остановлены".to_string(),
    )))
}

pub async fn accuracy_total(
    State(state): State<ApiState>,
    Query(query): Query<AccuracyQuery>,
) -> Result<Json<ApiResponse<f64>>, StatusCode> {
    let (tx, rx) = oneshot::channel();

    state
        .counter_handle
        .send(CounterCommand::GetTotalShiftedAccuracy {
            window: query.window,
            respond_to: tx,
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let accuracy = rx
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .unwrap_or(0.0);

    Ok(Json(ApiResponse::success(accuracy)))
}

pub async fn accuracy_token(
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
    Query(query): Query<AccuracyQuery>,
) -> Result<Json<ApiResponse<f64>>, StatusCode> {
    let (tx, rx) = oneshot::channel();

    state
        .counter_handle
        .send(CounterCommand::GetShiftedAccuracy {
            symbol: symbol.to_uppercase(),
            window: query.window,
            respond_to: tx,
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let accuracy = rx
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .unwrap_or(0.0);

    Ok(Json(ApiResponse::success(accuracy)))
}

pub async fn accuracy_all_tokens(
    State(state): State<ApiState>,
    Query(query): Query<AccuracyQuery>,
) -> Result<Json<ApiResponse<Vec<AccuracyInfo>>>, StatusCode> {
    let (tx_list, rx_list) = oneshot::channel();
    state
        .supervisor_handle
        .send(SupervisorCommand::ListActive {
            respond_to: tx_list,
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let symbols = rx_list
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut accuracies = Vec::new();

    for symbol in symbols {
        let (tx, rx) = oneshot::channel();

        let _ = state
            .counter_handle
            .send(CounterCommand::GetShiftedAccuracy {
                symbol: symbol.to_uppercase(),
                window: query.window,
                respond_to: tx,
            })
            .await;

        if let Ok(Some(accuracy)) = rx.await {
            accuracies.push(AccuracyInfo {
                symbol,
                accuracy,
                window: query.window,
                counter_type: query.counter_type.to_lowercase(),
            });
        }
    }

    Ok(Json(ApiResponse::success(accuracies)))
}

pub async fn get_last_prediction(
    State(state): State<ApiState>,
    Query(query): Query<SymbolQuery>,
) -> Result<Json<ApiResponse<f64>>, StatusCode> {
    let (tx, rx) = oneshot::channel();

    state
        .prediction_handle
        .send(PredictionsCommand::GetLastPrediction {
            symbol: query.symbol,
            respond_to: tx,
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let last = rx
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NO_CONTENT)?;

    Ok(Json(ApiResponse::success(last)))
}

pub async fn predictions_list(
    State(state): State<ApiState>,
    Query(query): Query<SymbolQuery>,
) -> Result<Json<ApiResponse<Vec<f64>>>, StatusCode> {
    let (tx, rx) = oneshot::channel();

    state
        .prediction_handle
        .send(PredictionsCommand::GetPredictions {
            symbol: query.symbol,
            respond_to: tx,
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let result: Vec<f64> = rx
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NO_CONTENT)?
        .data
        .clone()
        .into_iter()
        .collect();
    Ok(Json(ApiResponse::success(result)))
}

pub async fn all_predictions_list(
    State(state): State<ApiState>,
) -> Result<Json<ApiResponse<HashMap<String, Vec<f64>>>>, StatusCode> {
    let (tx, rx) = oneshot::channel();

    state
        .prediction_handle
        .send(PredictionsCommand::ListPredictions { respond_to: tx })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let result: HashMap<String, Vec<f64>> = rx
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NO_CONTENT)?
        .clone()
        .into_iter()
        .map(|data| (data.0, data.1.data.into_iter().collect()))
        .collect();
    Ok(Json(ApiResponse::success(result)))
}

pub async fn generate_plots(
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
    Json(payload): Json<PasswordRequest>,
) -> Result<Json<ApiResponse<()>>, StatusCode> {
    if !verify_password(payload.password) {
        return Ok(Json(ApiResponse::error("Неверный пароль".to_string())));
    }

    let (tx, rx) = oneshot::channel();

    state
        .supervisor_handle
        .send(SupervisorCommand::ChainHandle { respond_to: tx })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let chain_handle = rx
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NO_CONTENT)?;

    let (tx, rx) = oneshot::channel();

    chain_handle
        .send(ChainCommand::SavePlots {
            symbol,
            respond_to: tx,
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let result = rx
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?
        .map_err(|_| StatusCode::NO_CONTENT)?;
    Ok(Json(ApiResponse::success(result)))
}
