use anyhow::anyhow;
use chrono::Utc;

use log::debug;
use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::metrics::{accuracy, mean_absolute_error, mean_squared_error, r2};
use smartcore::model_selection::train_test_split;
use sqlx::PgPool;
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::{mpsc, oneshot};

use crate::data::data_interfaces::DataMap;
use crate::data::requests::database::standart::SQLStandart;
use crate::engine::cycles::manager::PredictionsCommand;
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::{Config, MetricType};
use crate::models::SingleModelParams;
use crate::models::ensemble::Ensemble;
use crate::models::metrics::*;

pub trait ModelDependencies {
    fn get_name(&self) -> &str;
    fn get_symbol_columns(&self) -> &Option<Vec<String>>;
    fn change_symbol_columns(&mut self, symbol_columns: Option<Vec<String>>);
    fn get_config(&self) -> &Config;
    fn get_prediction_tx(&self) -> &Option<mpsc::Sender<PredictionsCommand>>;
    fn get_target_name(&self) -> &str;
    fn check_model_trained(&self) -> bool;
    fn get_standart(&self) -> &SQLStandart;
    fn get_pool(&self) -> Option<&PgPool>;
    fn get_accuracy(&self) -> Option<DataMap> {
        None
    }
}

#[async_trait::async_trait]
pub trait Model: ModelDependencies {
    fn load_data(
        &mut self,
        data: Vec<DataMap>,
    ) -> Result<(DenseMatrix<f64>, Vec<f64>), anyhow::Error> {
        let n_samples = data.len();
        debug!("n_samples: {}", &n_samples);
        if n_samples == 0 {
            return Err(anyhow!("No data provided"));
        }

        let feature_len = data[0].get_only_features().iter().len();
        debug!("feature_len: {}", &feature_len);

        let symbols: Vec<&str> = data.iter().map(|d| d.symbol.as_str()).collect();
        let unique_symbols: Vec<&str> = {
            let mut set = BTreeSet::new();
            symbols.iter().for_each(|s| {
                set.insert(*s);
            });
            set.into_iter().collect()
        };
        let n_symbols = unique_symbols.len();
        debug!("usymbols: {:?}, len: {}", &unique_symbols, &n_symbols);
        let symbol_to_idx: BTreeMap<&str, usize> = unique_symbols
            .iter()
            .enumerate()
            .map(|(i, &s)| (s, i))
            .collect();

        self.change_symbol_columns(Some(
            unique_symbols
                .iter()
                .map(|s| format!("symbol_name_{}", s))
                .collect(),
        ));

        let mut x_rows: Vec<Vec<f64>> = Vec::with_capacity(n_samples);
        let mut y_target: Vec<f64> = Vec::with_capacity(n_samples);
        let mut skipped_nan_features = 0;
        let mut skipped_nan_target = 0;

        for row in data.iter() {
            let target = row.get(self.get_target_name()).copied().unwrap_or_default();
            // debug!("target: {} = {}", self.get_target_name(), &target);
            if target.is_nan() {
                skipped_nan_target += 1;
                continue;
            }

            let has_nan_features = row
                .get_only_features()
                .values()
                .into_iter()
                .any(|&v| v.is_nan());
            if has_nan_features {
                skipped_nan_features += 1;
                continue;
            }

            let mut full_row = vec![0.0; n_symbols + feature_len];

            if let Some(&idx) = symbol_to_idx.get(row.symbol.as_str()) {
                full_row[idx] = 1.0;
            }

            for (i, val) in row.get_only_features().values().enumerate() {
                full_row[n_symbols + i] = *val;
            }

            x_rows.push(full_row);
            y_target.push(target);
        }

        if self.get_config().prints.model.skipped_values {
            println!(
                "{}[{}] Skipped rows: {} (NaN in target), {} (NaN in features)",
                Fore::YELLOW.as_str(),
                Utc::now().format("%H:%M:%S"),
                skipped_nan_target,
                skipped_nan_features
            );
            println!(
                "{}[{}] Remaining {} valid rows from {}",
                Fore::GREEN.as_str(),
                Utc::now().format("%H:%M:%S"),
                x_rows.len(),
                n_samples
            );
        }

        if x_rows.is_empty() {
            return Err(anyhow!("No valid data after removing NaN values"));
        }

        let n_features = n_symbols + feature_len;
        debug!("n_features: {}", &n_features);
        let mut flat_x = Vec::with_capacity(x_rows.len() * n_features);
        for row in x_rows.iter() {
            flat_x.extend(row);
        }

        let x = DenseMatrix::new(x_rows.len(), n_features, flat_x, false)
            .map_err(|e| anyhow!("Failed to create DenseMatrix for features: {}", e))?;

        Ok((x, y_target))
    }

    fn prepare_data(
        &mut self,
        x: DenseMatrix<f64>,
        y_target: Vec<f64>,
        train_ratio: f32,
    ) -> Result<(DenseMatrix<f64>, DenseMatrix<f64>, Vec<f64>, Vec<f64>), anyhow::Error> {
        let (x_train, x_val, y_train, y_val) = train_test_split(
            &x,
            &y_target,
            train_ratio,
            true,
            Some(self.get_config().model.seed),
        );
        let (x_train, x_val) = self.normalize(x_train, x_val);

        Ok((x_train, x_val, y_train, y_val))
    }

    fn evaluate(&self, x_val: &DenseMatrix<f64>, y_val: &Vec<f64>) -> Result<f64, anyhow::Error> {
        if !self.check_model_trained() {
            return Err(anyhow!("Model not trained yet"));
        }
        let proba = self
            .model_predict(x_val)
            .map_err(|e| anyhow!("Model prediction failed during evaluation: {}", e))?;
        let y_float: Vec<f64> = y_val.to_vec();

        let thr_accuracy = threshold_accuracy(
            &y_float,
            &proba,
            self.get_config().behaviour.success_threshold,
        );

        let metric = match self.get_config().model.metric {
            MetricType::RAll => {
                let mae = mean_absolute_error(&y_float, &proba);
                let mse = mean_squared_error(&y_float, &proba);
                let r2_score = r2(&y_float, &proba);
                let rmse = mean_squared_error(&y_float, &proba).sqrt();

                if self.get_config().prints.model.metrics {
                    println!(
                        "{}[{}] MAE for {}: {:.3} pp",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        mae
                    );
                    println!(
                        "{}[{}] MSE for {}: {:.3} (pp²)",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        mse
                    );
                    println!(
                        "{}[{}] R2 for {}: {:.3}",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        r2_score
                    );
                    println!(
                        "{}[{}] Acc on threshold {} for {}: {:.3}%",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_config().behaviour.success_threshold,
                        self.get_name(),
                        thr_accuracy * 100.0
                    );
                    println!(
                        "{}[{}] RMSE for {}: {:.3} pp",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        rmse
                    );
                }
                thr_accuracy
            }
            MetricType::Threshold => {
                let thr_accuracy = threshold_accuracy(
                    &y_float,
                    &proba,
                    self.get_config().behaviour.success_threshold,
                );
                if self.get_config().prints.model.metrics {
                    println!(
                        "{}[{}] Acc on threshold {} for {}: {:.3}%",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_config().behaviour.success_threshold,
                        self.get_name(),
                        thr_accuracy * 100.0
                    );
                }
                thr_accuracy
            }
            MetricType::MAE => {
                let mae = mean_absolute_error(&y_float, &proba);
                if self.get_config().prints.model.metrics {
                    println!(
                        "{}[{}] MAE for {}: {:.3} pp",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        mae
                    );
                }
                mae
            }
            MetricType::MSE => {
                let mse = mean_squared_error(&y_float, &proba);
                if self.get_config().prints.model.metrics {
                    println!(
                        "{}[{}] MSE for {}: {:.3} pp",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        mse
                    );
                }
                mse
            }
            MetricType::R2 => {
                let r2_score = r2(&y_float, &proba);
                if self.get_config().prints.model.metrics {
                    println!(
                        "{}[{}] R2 for {}: {:.3}",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        r2_score
                    );
                }
                r2_score
            }
            MetricType::RMSE => {
                let rmse = mean_squared_error(&y_float, &proba).sqrt();
                if self.get_config().prints.model.metrics {
                    println!(
                        "{}[{}] RMSE for {}: {:.3} pp",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        rmse
                    );
                }
                rmse
            }
            MetricType::Acc => {
                let acc = accuracy(
                    &y_float.iter().map(|v| *v as i32).collect::<Vec<i32>>(),
                    &proba.iter().map(|v| *v as i32).collect::<Vec<i32>>(),
                );
                if self.get_config().prints.model.metrics {
                    println!(
                        "{}[{}] Accuracy for {}: {:.3} pp",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        acc
                    );
                }
                acc
            }
        };

        Ok(metric)
    }

    async fn predict(&self, data: DataMap) -> Result<DataMap, anyhow::Error> {
        let symbol_name = data.symbol.clone();
        let x = data
            .to_standart(self.get_standart())
            .get_only_features()
            .values()
            .cloned()
            .collect::<Vec<_>>();

        let symbol_cols = self
            .get_symbol_columns()
            .clone()
            .ok_or_else(|| anyhow!("No symbol columns defined"))?;

        if !self.check_model_trained() {
            return Err(anyhow!("Model not trained yet"));
        }

        let mut input: Vec<f64> = Vec::with_capacity(symbol_cols.len() + x.len());

        let mut symbol_vec = vec![0.0; symbol_cols.len()];
        if let Some(idx) = symbol_cols
            .iter()
            .position(|col| col == &format!("symbol_name_{}", symbol_name))
        {
            symbol_vec[idx] = 1.0;
        }
        input.extend(symbol_vec);

        input.extend(x);

        let input_mat = DenseMatrix::new(1, input.len(), input, false)
            .map_err(|e| anyhow!("Failed to create DenseMatrix for prediction input: {}", e))?;
        let proba = self
            .model_predict(&input_mat)
            .map_err(|e| anyhow!("Model prediction failed: {}", e))?;

        if let Some(ptx) = self.get_prediction_tx().clone() {
            let (tx, rx) = oneshot::channel();

            if let Err(e) = ptx
                .send(PredictionsCommand::AddPrediction {
                    symbol: symbol_name.to_string(),
                    prediction: proba[0],
                    respond_to: tx,
                })
                .await
            {
                println!("Prediction channel closed: {}", e);
            } else {
                if let Err(e) = rx.await {
                    println!("Prediction response cancelled: {}", e);
                }
            }
        }

        Ok(DataMap::new(
            symbol_name,
            BTreeMap::from([(self.get_target_name().to_string(), proba[0])]),
        ))
    }

    async fn train(&mut self) -> Result<(), anyhow::Error> {
        let pool = self
            .get_pool()
            .ok_or_else(|| anyhow!("Pool is not available"))?;
        let data = self
            .get_standart()
            .select_all(pool)
            .await
            .map_err(|e| anyhow!("Failed to select data from database: {}", e))?;
        // debug!(
        //     "Data for train: {:#?}\nStandart: {:?}",
        //     &data[0],
        //     self.get_standart()
        // );
        let (x, y_target) = self
            .load_data(data)
            .map_err(|e| anyhow!("Failed to load data: {}", e))?;
        debug!("Data successfully loaded");
        let (x_train, x_val, y_train, y_val) = self
            .prepare_data(
                x,
                y_target,
                self.get_config().model.train_test_split.train_ratio,
            )
            .map_err(|e| anyhow!("Failed to prepare data: {}", e))?;
        debug!("Data successfully prepared");
        self.model_fit(&x_train, &y_train, Some(&x_val), Some(&y_val))
            .map_err(|e| anyhow!("Model fitting failed: {}", e))?;
        debug!("Model trained successfully");
        Ok(())
    }

    fn model_predict(&self, values: &DenseMatrix<f64>) -> Result<Vec<f64>, anyhow::Error>;

    fn model_fit(
        &mut self,
        x_train: &DenseMatrix<f64>,
        y_train: &Vec<f64>,
        x_val: Option<&DenseMatrix<f64>>,
        y_val: Option<&Vec<f64>>,
    ) -> Result<(), anyhow::Error>;

    fn normalize(
        &self,
        x_train: DenseMatrix<f64>,
        x_val: DenseMatrix<f64>,
    ) -> (DenseMatrix<f64>, DenseMatrix<f64>) {
        (x_train, x_val)
    }

    async fn handle_mistakes(
        &mut self,
        true_data: DataMap,
        predicted_data: DataMap,
    ) -> Result<(), anyhow::Error>;
}

pub fn init_single_model(
    params: SingleModelParams,
    prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
    standart: SQLStandart,
    pool: PgPool,
) -> Box<dyn Model + Send + Sync> {
    let model: Box<dyn Model + Send + Sync> = match params {
        SingleModelParams::XGBoost {
            task_type,
            target_type,
            n_estimators,
            max_depth,
        } => Box::new(crate::models::xgboost::XGBoost::new(
            prediction_tx,
            task_type,
            target_type,
            standart,
            pool,
            n_estimators,
            max_depth,
        )),
        SingleModelParams::RandomForest {
            task_type,
            target_type,
            n_trees,
            max_depth,
            min_samples_leaf,
            min_samples_split,
            m,
        } => Box::new(crate::models::randomforest::RandomForest::new(
            prediction_tx,
            task_type,
            target_type,
            standart,
            pool,
            n_trees,
            max_depth,
            min_samples_leaf,
            min_samples_split,
            m,
        )),
        SingleModelParams::Linear {
            task_type,
            target_type,
            solver,
        } => Box::new(crate::models::linear::Linear::new(
            prediction_tx,
            task_type,
            target_type,
            standart,
            pool,
            solver,
        )),
        SingleModelParams::Ridge {
            task_type,
            target_type,
            alpha,
            solver,
        } => Box::new(crate::models::ridge::Ridge::new(
            prediction_tx,
            task_type,
            target_type,
            standart,
            pool,
            solver,
            alpha,
        )),
        SingleModelParams::DecisionTree {
            task_type,
            target_type,
            max_depth,
            min_samples_leaf,
            min_samples_split,
        } => Box::new(crate::models::decisiontree::DecisionTree::new(
            prediction_tx,
            task_type,
            target_type,
            standart,
            pool,
            max_depth,
            min_samples_leaf,
            min_samples_split,
        )),
        SingleModelParams::KNN {
            task_type,
            target_type,
            algorithm,
            weight,
            k,
        } => Box::new(crate::models::knn::KNN::new(
            prediction_tx,
            task_type,
            target_type,
            standart,
            pool,
            algorithm,
            weight,
            k,
        )),
        SingleModelParams::ExtraTrees {
            task_type,
            target_type,
            n_trees,
            max_depth,
            min_samples_leaf,
            min_samples_split,
            m,
        } => Box::new(crate::models::extratrees::ExtraTrees::new(
            prediction_tx,
            task_type,
            target_type,
            standart,
            pool,
            n_trees,
            max_depth,
            min_samples_leaf,
            min_samples_split,
            m,
        )),
    };
    model
}

pub fn init_ensemble_model(
    prediction_tx: Option<mpsc::Sender<PredictionsCommand>>,
    pool: PgPool,
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
    volatility_spike_probability_model_params: SingleModelParams,
    liquidity_drop_probability_model_params: SingleModelParams,
    future_return_model_params: SingleModelParams,
    action_type_model_params: SingleModelParams,
    position_size_model_params: SingleModelParams,
) -> Box<dyn Model + Send + Sync> {
    let model = Ensemble::init(
        prediction_tx,
        pool,
        future_volatility_model_params,
        future_volume_model_params,
        future_trend_strength_model_params,
        future_range_model_params,
        future_return_mean_model_params,
        future_return_std_model_params,
        future_return_skew_model_params,
        future_return_kurt_model_params,
        risk_score_model_params,
        drawdown_probability_model_params,
        tail_event_probability_model_params,
        volatility_spike_probability_model_params,
        liquidity_drop_probability_model_params,
        future_return_model_params,
        action_type_model_params,
        position_size_model_params,
    );
    Box::new(model)
}

#[tokio::test]
async fn test_training() -> Result<(), anyhow::Error> {
    let pool =
        sqlx::PgPool::connect(&crate::engine::utils::config::load_env::load_env().database_url)
            .await
            .map_err(|e| return anyhow!(format!("{}", e)))?;
    let params = crate::engine::utils::config::load_config::load_config(crate::CONFIG_PATH)
        .model
        .params;

    match params {
        crate::models::ModelParams::Ensemble {
            future_volatility_model_params,
            future_volume_model_params,
            future_trend_strength_model_params,
            future_range_model_params,
            future_return_mean_model_params,
            future_return_std_model_params,
            future_return_skew_model_params,
            future_return_kurt_model_params,
            risk_score_model_params,
            drawdown_probability_model_params,
            tail_event_probability_model_params,
            volatility_spike_probability_model_params,
            liquidity_drop_probability_model_params,
            future_return_model_params,
            action_type_model_params,
            position_size_model_params,
        } => {
            let mut model = init_ensemble_model(
                None,
                pool,
                future_volatility_model_params,
                future_volume_model_params,
                future_trend_strength_model_params,
                future_range_model_params,
                future_return_mean_model_params,
                future_return_std_model_params,
                future_return_skew_model_params,
                future_return_kurt_model_params,
                risk_score_model_params,
                drawdown_probability_model_params,
                tail_event_probability_model_params,
                volatility_spike_probability_model_params,
                liquidity_drop_probability_model_params,
                future_return_model_params,
                action_type_model_params,
                position_size_model_params,
            );
            model.train().await?;
        }
        crate::models::ModelParams::Single { params } => {
            let mut model = init_single_model(params, None, SQLStandart::SingleModel, pool);
            model.train().await?;
        }
    }

    Ok(())
}

// #[tokio::test]
#[allow(unused)]
async fn find_best_model_config() -> Result<(), anyhow::Error> {
    use crate::models::TargetType;

    let pool =
        sqlx::PgPool::connect(&crate::engine::utils::config::load_env::load_env().database_url)
            .await
            .map_err(|e| return anyhow!(format!("{}", e)))?;

    let targets = [
        (TargetType::FutureVolatility, SQLStandart::FirstLayer),
        (TargetType::FutureVolume, SQLStandart::FirstLayer),
        (TargetType::FutureTrendStrength, SQLStandart::FirstLayer),
        (TargetType::FutureRange, SQLStandart::FirstLayer),
        (TargetType::FutureReturnMean, SQLStandart::FirstLayer),
        (TargetType::FutureReturnStd, SQLStandart::FirstLayer),
        (TargetType::FutureReturnSkewness, SQLStandart::FirstLayer),
        (TargetType::FutureReturnKurtosis, SQLStandart::FirstLayer),
        (TargetType::RiskScore, SQLStandart::SecondLayer),
        (TargetType::DrawdownProbability, SQLStandart::SecondLayer),
        (TargetType::TailEventProbability, SQLStandart::SecondLayer),
        (
            TargetType::VolatilitySpikeProbability,
            SQLStandart::SecondLayer,
        ),
        (
            TargetType::LiquidityDropProbability,
            SQLStandart::SecondLayer,
        ),
        (TargetType::FutureReturn, SQLStandart::ThirdLayer),
        (TargetType::ActionType, SQLStandart::ThirdLayer),
        (TargetType::PositionSize, SQLStandart::ThirdLayer),
    ];

    for target in targets {
        // XGBoost
        {
            let n_estimators_arr = [10, 25, 50, 100, 150];
            let max_depth_arr = [1, 2, 3, 4, 5];

            for n_estimators in n_estimators_arr.into_iter() {
                for max_depth in max_depth_arr.into_iter() {
                    let mut xgboost = match target.clone().0 {
                        TargetType::ActionType => crate::models::xgboost::XGBoost::new(
                            None,
                            crate::models::TaskType::Classification,
                            target.0,
                            target.1,
                            pool.clone(),
                            n_estimators,
                            max_depth,
                        ),
                        _ => crate::models::xgboost::XGBoost::new(
                            None,
                            crate::models::TaskType::Regression,
                            target.0,
                            target.1,
                            pool.clone(),
                            n_estimators,
                            max_depth,
                        ),
                    };
                    xgboost.train().await?;
                }
            }
        }
    }

    Ok(())
}
