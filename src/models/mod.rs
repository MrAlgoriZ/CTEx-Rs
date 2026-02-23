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
    FutureSpread,
    FutureTrendStrength,
    FutureRange,
    FutureReturn,
    FutureReturnMean,
    FutureReturnStd,
    FutureReturnSkew,
    FutureReturnKurt,
    ActionType,
}

impl TargetType {
    pub fn get_index(&self) -> i32 {
        match self {
            TargetType::FutureVolatility => -10,
            TargetType::FutureVolume => -9,
            TargetType::FutureSpread => -8,
            TargetType::FutureTrendStrength => -7,
            TargetType::FutureRange => -6,
            TargetType::FutureReturn => -5,
            TargetType::FutureReturnMean => -4,
            TargetType::FutureReturnStd => -3,
            TargetType::FutureReturnSkew => -2,
            TargetType::FutureReturnKurt => -1,
            TargetType::ActionType => -1,
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
        volatility_model_params: SingleModelParams,
        volume_model_params: SingleModelParams,
        spread_model_params: SingleModelParams,
        trend_strength_model_params: SingleModelParams,
        range_model_params: SingleModelParams,
        return_model_params: SingleModelParams,
        return_mean_model_params: SingleModelParams,
        return_std_model_params: SingleModelParams,
        return_skew_model_params: SingleModelParams,
        return_kurt_model_params: SingleModelParams,
        action_model_params: SingleModelParams,
        interpretator_model_params: SingleModelParams,
    },
    Single {
        params: SingleModelParams,
    },
}
