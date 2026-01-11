use crate::{
    CONFIG_PATH,
    backend::structure::{ApiState, ApiStructure},
    engine::{
        cycles::manager::{CounterCommand, CounterType, CycleType, SupervisorCommand},
        utils::config::load_config::load_config,
    },
};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

fn verify_password(input: String) -> bool {
    let cfg = load_config(CONFIG_PATH);
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

fn default_window() -> usize {
    load_config(CONFIG_PATH).behaviour.accuracy_capacity
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

    if !state.client.test_token(&payload.symbol).await.is_ok() {
        return Ok(Json(ApiResponse::error(format!(
            "Токена {} не существует",
            payload.symbol
        ))));
    }

    let cycle_type = match payload.cycle_type.to_lowercase().as_str() {
        "training" => CycleType::Training,
        "loader" => CycleType::Loader,
        _ => {
            return Ok(Json(ApiResponse::error(
                "Тип цикла должен быть 'training' или 'loader'".to_string(),
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
        Ok(Err(e)) => Ok(Json(ApiResponse::error(e))),
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
        Ok(Err(e)) => Ok(Json(ApiResponse::error(e))),
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
            counter_type: CounterType::from_str(&query.counter_type.to_lowercase()),
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
            counter_type: CounterType::from_str(&query.counter_type.to_lowercase()),
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
                counter_type: CounterType::from_str(&query.counter_type.to_lowercase()),
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
