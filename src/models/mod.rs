pub mod decisiontree;
pub mod ensemble;
pub mod extratrees;
pub mod knn;
pub mod linear;
pub mod metrics;
pub mod model;
pub mod randomforest;
pub mod ridge;
pub mod xgboost;

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ModelType {
    Ensemble,
    Single,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum TargetType {
    FutureVolatility,
    FutureVolume,
    FutureTrendStrength,
    FutureRange,
    FutureReturnMean,
    FutureReturnStd,
    FutureReturnSkew,
    FutureReturnKurt,
    RiskScore,
    DrawdownProbability,
    TailEventProbability,
    VolatilitySpikeProbability,
    LiquidityDropProbability,
    FutureReturn,
    ActionType,
    PositionSize,
}

impl TargetType {
    pub fn get_name(&self) -> &str {
        match self {
            TargetType::FutureVolatility => "future_volatility",
            TargetType::FutureVolume => "future_volume",
            TargetType::FutureTrendStrength => "future_trend_strength",
            TargetType::FutureRange => "future_range",
            TargetType::FutureReturnMean => "future_return_mean",
            TargetType::FutureReturnStd => "future_return_std",
            TargetType::FutureReturnSkew => "future_return_skewness",
            TargetType::FutureReturnKurt => "future_return_kurtosis",
            TargetType::RiskScore => "risk_score",
            TargetType::DrawdownProbability => "drawdown_probability",
            TargetType::TailEventProbability => "tail_event_probability",
            TargetType::VolatilitySpikeProbability => "volatility_spike_probability",
            TargetType::LiquidityDropProbability => "liquidity_drop_probability",
            TargetType::FutureReturn => "future_return",
            TargetType::ActionType => "action_type",
            TargetType::PositionSize => "position_size",
        }
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
#[serde(tag = "kind")]
pub enum SingleModelParams {
    XGBoost {
        target_type: TargetType,
        n_estimators: usize,
        max_depth: u16,
    },
    RandomForest {
        target_type: TargetType,
        n_trees: usize,
        max_depth: u16,
        min_samples_leaf: usize,
        min_samples_split: usize,
        m: usize,
    },
    ExtraTrees {
        target_type: TargetType,
        n_trees: usize,
        max_depth: u16,
        min_samples_leaf: usize,
        min_samples_split: usize,
        m: usize,
    },
    Linear {
        target_type: TargetType,
        solver: String,
    },
    Ridge {
        target_type: TargetType,
        alpha: f64,
        solver: String,
    },
    DecisionTree {
        target_type: TargetType,
        max_depth: u16,
        min_samples_leaf: usize,
        min_samples_split: usize,
    },
    KNN {
        target_type: TargetType,
        algorithm: String,
        weight: String,
        k: usize,
    },
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ModelParams {
    Ensemble {
        future_volatility_model_params: SingleModelParams,
        future_volume_model_params: SingleModelParams,
        future_trend_strength_model_params: SingleModelParams,
        future_range_model_params: SingleModelParams,
        future_return_mean_model_params: SingleModelParams,
        future_return_std_model_params: SingleModelParams,
        future_return_skew_model_params: SingleModelParams,
        future_return_kurt_model_params: SingleModelParams,
        risk_score_model_params: SingleModelParams,
        drawdown_probability_model_params: SingleModelParams,
        tail_event_probability_model_params: SingleModelParams,
        liquidity_drop_probability_model_params: SingleModelParams,
        future_return_model_params: SingleModelParams,
        action_type_model_params: SingleModelParams,
        position_size_model_params: SingleModelParams,
    },
    Single {
        params: SingleModelParams,
    },
}
