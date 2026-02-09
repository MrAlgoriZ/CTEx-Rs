pub mod metrics;
pub mod model;
pub mod randomforest;
pub mod xgboost;

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub enum ModelType {
    RandomForest,
    XGBoost,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
#[serde(tag = "kind")]
pub enum ModelParams {
    XGBoost { n_estimators: usize, max_depth: u16 },
    RandomForest { n_trees: usize, max_depth: u16 },
}
