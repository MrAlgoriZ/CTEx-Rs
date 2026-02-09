use anyhow::anyhow;
use chrono::Utc;

use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::metrics::{mean_absolute_error, mean_squared_error, r2};
use smartcore::model_selection::train_test_split;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

use crate::data::data_interfaces::FlattenedData;
use crate::engine::cycles::manager::PredictionCommand;
use crate::engine::utils::colors::Fore;
use crate::engine::utils::config::config_types::{Config, MetricType};
use crate::models::metrics::*;

pub trait ModelDependencies {
    fn get_name(&self) -> &str;
    fn get_symbol_columns(&self) -> &Option<Vec<String>>;
    fn change_symbol_columns(&mut self, symbol_columns: Option<Vec<String>>);
    fn get_config(&self) -> &Config;
    fn get_prediction_tx(&self) -> &Option<mpsc::Sender<PredictionCommand>>;
    fn check_model_trained(&self) -> bool;
}

#[async_trait::async_trait]
pub trait Model: ModelDependencies {
    fn load_data(
        &mut self,
        data: Vec<FlattenedData>,
    ) -> Result<(DenseMatrix<f64>, Vec<f64>), anyhow::Error> {
        let n_samples = data.len();
        if n_samples == 0 {
            return Err(anyhow!("No data provided"));
        }

        let total_len = data[0].features.len();
        // if total_len < 2 {
        //     return Err(anyhow!(
        //         "Each row must have at least one feature and one target"
        //     ));
        // }

        // if total_len != 30 {
        //     return Err(anyhow!(
        //         "Expected 30 columns (29 features + 1 target), got {}",
        //         total_len
        //     ));
        // }

        let feature_len = total_len - 1;
        let target_idx = feature_len;

        if data.iter().any(|d| d.features.len() != total_len) {
            return Err(anyhow!(
                "All rows must have the same number of features + target"
            ));
        }

        let symbols: Vec<&str> = data.iter().map(|d| d.symbol.as_str()).collect();
        let unique_symbols: Vec<&str> = {
            let mut set = std::collections::HashSet::new();
            symbols.iter().for_each(|s| {
                set.insert(*s);
            });
            set.into_iter().collect()
        };
        let n_symbols = unique_symbols.len();

        let symbol_to_idx: HashMap<&str, usize> = unique_symbols
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
            let target = row.features[target_idx];

            if target.is_nan() {
                skipped_nan_target += 1;
                continue;
            }

            let has_nan_features = row.features[..feature_len].iter().any(|&v| v.is_nan());
            if has_nan_features {
                skipped_nan_features += 1;
                continue;
            }

            let mut full_row = vec![0.0; n_symbols + feature_len];

            if let Some(&idx) = symbol_to_idx.get(row.symbol.as_str()) {
                full_row[idx] = 1.0;
            }

            for (i, &val) in row.features[..feature_len].iter().enumerate() {
                full_row[n_symbols + i] = val;
            }

            x_rows.push(full_row);
            y_target.push(target);
        }

        if self.get_config().prints.model.skipped_values {
            println!(
                "{}[{}] Пропущено строк: {} (NaN в target), {} (NaN в признаках)",
                Fore::YELLOW.as_str(),
                Utc::now().format("%H:%M:%S"),
                skipped_nan_target,
                skipped_nan_features
            );
            println!(
                "{}[{}] Осталось {} валидных строк из {}",
                Fore::GREEN.as_str(),
                Utc::now().format("%H:%M:%S"),
                x_rows.len(),
                n_samples
            );
        }

        if x_rows.is_empty() {
            return Err(anyhow!("No valid data after removing NaN values"));
        }

        // assert!(
        //     x_rows.len() == y_target.len(),
        //     "X and y length mismatch: X={}, y={}",
        //     x_rows.len(),
        //     y_target.len()
        // );

        let n_features = n_symbols + feature_len;
        let mut flat_x = Vec::with_capacity(x_rows.len() * n_features);
        for row in x_rows.iter() {
            flat_x.extend(row);
        }

        let x = DenseMatrix::new(x_rows.len(), n_features, flat_x, false)?;

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
        let proba = self.model_predict(x_val)?;
        let y_float: Vec<f64> = y_val.to_vec();

        let thr_accuracy = threshold_accuracy(
            &y_float,
            &proba,
            self.get_config().behaviour.success_threshold.default,
        );

        let metric = match self.get_config().model.metric {
            MetricType::All => {
                let dir_accuracy = direction_accuracy(&y_float, &proba);
                let mae = mean_absolute_error(&y_float, &proba);
                let mse = mean_squared_error(&y_float, &proba);
                let r2_score = r2(&y_float, &proba);
                let rmse = mean_squared_error(&y_float, &proba).sqrt();

                if self.get_config().prints.model.metrics {
                    println!(
                        "{}[{}] Ошибка по MAE для {}: {:.3} pp",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        mae
                    );
                    println!(
                        "{}[{}] Ошибка по MSE для {}: {:.3} (pp²)",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        mse
                    );
                    println!(
                        "{}[{}] Ошибка по R2 для {}: {:.3}",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        r2_score
                    );

                    println!(
                        "{}[{}] Точность по порогу {} для {} составляет {:.3}%",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_config().behaviour.success_threshold.default,
                        self.get_name(),
                        thr_accuracy * 100.0
                    );
                    println!(
                        "{}[{}] Точность по направлению для {} составляет {:.3}%",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        dir_accuracy * 100.0
                    );
                    println!(
                        "{}[{}] Ошибка по RMSE для {}: {:.3} pp",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        rmse
                    );
                }
                thr_accuracy
            }
            MetricType::Direction => {
                let dir_accuracy = direction_accuracy(&y_float, &proba);
                if self.get_config().prints.model.metrics {
                    println!(
                        "{}[{}] Точность по направлению для {} составляет {:.3}%",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        dir_accuracy * 100.0
                    );
                }
                dir_accuracy
            }
            MetricType::Threshold => {
                let thr_accuracy = threshold_accuracy(
                    &y_float,
                    &proba,
                    self.get_config().behaviour.success_threshold.default,
                );
                if self.get_config().prints.model.metrics {
                    println!(
                        "{}[{}] Точность по порогу {} для {} составляет {:.3}%",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_config().behaviour.success_threshold.default,
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
                        "{}[{}] Ошибка по MAE для {}: {:.3} pp",
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
                        "{}[{}] Ошибка по MSE для {}: {:.3} pp",
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
                        "{}[{}] Ошибка по R2 для {}: {:.3}",
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
                        "{}[{}] Ошибка по RMSE для {}: {:.3} pp",
                        Fore::WHITE.as_str(),
                        Utc::now().format("%H:%M:%S"),
                        self.get_name(),
                        rmse
                    );
                }
                rmse
            }
        };

        Ok(metric)
    }

    async fn predict(&self, x: Vec<f64>, symbol_name: Option<&str>) -> Result<f64, anyhow::Error> {
        let symbol_cols = self
            .get_symbol_columns()
            .clone()
            .ok_or(anyhow!("No symbol columns defined"))?;

        if !self.check_model_trained() {
            return Err(anyhow!("Model not trained yet"));
        }

        let mut input: Vec<f64> = Vec::with_capacity(symbol_cols.len() + x.len());

        let mut symbol_vec = vec![0.0; symbol_cols.len()];
        if let Some(sn) = symbol_name {
            if let Some(idx) = symbol_cols
                .iter()
                .position(|col| col == &format!("symbol_name_{}", sn))
            {
                symbol_vec[idx] = 1.0;
            }
        }
        input.extend(symbol_vec);

        input.extend(x);

        let input_mat = DenseMatrix::new(1, input.len(), input, false)?;
        let proba = self.model_predict(&input_mat)?;

        if let Some(sn) = symbol_name
            && let Some(ptx) = self.get_prediction_tx().clone()
        {
            let (tx, rx) = oneshot::channel();

            let _ = ptx.send(PredictionCommand::AddPrediction {
                symbol: sn.to_string(),
                prediction: proba[0],
                respond_to: tx,
            });
            rx.await??;
        }

        Ok(proba[0])
    }

    fn train(&mut self, data: Vec<FlattenedData>) -> Result<(), anyhow::Error> {
        let (x, y_target) = self.load_data(data)?;
        let (x_train, x_val, y_train, y_val) = self.prepare_data(
            x,
            y_target,
            self.get_config().model.train_test_split.train_ratio,
        )?;
        self.model_fit(&x_train, &y_train, Some(&x_val), Some(&y_val))?;
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
}

#[tokio::test]
async fn test_training() -> Result<(), anyhow::Error> {
    let pool =
        sqlx::PgPool::connect(&crate::engine::utils::config::load_env::load_env().database_url)
            .await
            .map_err(|e| return anyhow::anyhow!(format!("{}", e)))?;
    let params = crate::engine::utils::config::load_config::load_config(crate::CONFIG_PATH)
        .model
        .params;
    match params {
        crate::models::ModelParams::XGBoost {
            n_estimators,
            max_depth,
        } => {
            let mut xgboost = crate::models::xgboost::XGBoost::new(None, n_estimators, max_depth);

            let data = crate::data::requests::database::db_req::select_all_candles(&pool).await?;
            xgboost.train(data)?;
        }
        crate::models::ModelParams::RandomForest {
            n_trees,
            max_depth,
            min_samples_leaf,
            min_samples_split,
            m,
        } => {
            let mut rf = crate::models::randomforest::RandomForest::new(
                None,
                n_trees,
                max_depth,
                min_samples_leaf,
                min_samples_split,
                m,
            );

            let data = crate::data::requests::database::db_req::select_all_candles(&pool).await?;
            rf.train(data)?;
        }
        crate::models::ModelParams::Linear { solver } => {
            let mut ln = crate::models::linear::Linear::new(None, solver);

            let data = crate::data::requests::database::db_req::select_all_candles(&pool).await?;
            ln.train(data)?;
        }
        crate::models::ModelParams::Ridge { alpha, solver } => {
            let mut ridge = crate::models::ridge::Ridge::new(None, solver, alpha);

            let data = crate::data::requests::database::db_req::select_all_candles(&pool).await?;
            ridge.train(data)?;
        }
        crate::models::ModelParams::DecisionTree {
            max_depth,
            min_samples_leaf,
            min_samples_split,
        } => {
            let mut decision = crate::models::decisiontree::DecisionTree::new(
                None,
                max_depth,
                min_samples_leaf,
                min_samples_split,
            );

            let data = crate::data::requests::database::db_req::select_all_candles(&pool).await?;
            decision.train(data)?;
        }
        crate::models::ModelParams::KNN {
            algorithm,
            weight,
            k,
        } => {
            let mut knn = crate::models::knn::KNN::new(None, algorithm, weight, k);
            let data = crate::data::requests::database::db_req::select_all_candles(&pool).await?;
            knn.train(data)?;
        }
        crate::models::ModelParams::ExtraTrees {
            n_trees,
            max_depth,
            min_samples_leaf,
            min_samples_split,
            m,
        } => {
            let mut et = crate::models::extratrees::ExtraTrees::new(
                None,
                n_trees,
                max_depth,
                min_samples_leaf,
                min_samples_split,
                m,
            );

            let data = crate::data::requests::database::db_req::select_all_candles(&pool).await?;
            et.train(data)?;
        }
    }
    Ok(())
}
