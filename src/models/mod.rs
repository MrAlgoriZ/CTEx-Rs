pub mod decisiontree;
pub mod extratrees;
pub mod knn;
pub mod linear;
pub mod metrics;
pub mod model;
pub mod randomforest;
pub mod ridge;
pub mod xgboost;

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub enum ModelType {
    RandomForest,
    XGBoost,
    Linear,
    Ridge,
    DecisionTree,
    KNN,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
#[serde(tag = "kind")]
pub enum ModelParams {
    XGBoost {
        n_estimators: usize,
        max_depth: u16,
    },
    RandomForest {
        n_trees: usize,
        max_depth: u16,
        min_samples_leaf: usize,
        min_samples_split: usize,
        m: usize,
    },
    ExtraTrees {
        n_trees: usize,
        max_depth: u16,
        min_samples_leaf: usize,
        min_samples_split: usize,
        m: usize,
    },
    Linear {
        solver: String,
    },
    Ridge {
        alpha: f64,
        solver: String,
    },
    DecisionTree {
        max_depth: u16,
        min_samples_leaf: usize,
        min_samples_split: usize,
    },
    KNN {
        algorithm: String,
        weight: String,
        k: usize,
    },
}
