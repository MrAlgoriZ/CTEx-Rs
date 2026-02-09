pub mod metrics;
pub mod model;
pub mod xgboost;

// pub enum ModelType {
//     RandomForest,
//     XGBoost,
// }

// impl ModelType {
//     pub fn from_str(model_name: String) -> Option<ModelType> {
//         let mn: &str = &model_name.to_lowercase();
//         match mn {
//             "xgboost" => Some(ModelType::XGBoost),
//             "randomforest" => Some(ModelType::RandomForest),
//             _ => None,
//         }
//     }
// }
