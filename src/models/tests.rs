#[tokio::test]
async fn test_training() -> anyhow::Result<()> {
    use crate::data::requests::database::standart::SQLStandart;

    dotenvy::dotenv().ok();

    let pool =
        sqlx::PgPool::connect(&crate::engine::utils::config::load_env::load_env().database_url)
            .await
            .map_err(|e| return anyhow::anyhow!(format!("{}", e)))?;
    let params = crate::engine::utils::config::load_config::load_config()
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
            let mut model = crate::models::model::init_ensemble_model(
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
            let mut model = crate::models::model::init_single_model(
                params,
                None,
                SQLStandart::SingleModel,
                pool,
            );
            model.train().await?;
        }
    }

    Ok(())
}

#[tokio::test]
async fn find_best_model_config() -> anyhow::Result<()> {
    use crate::data::requests::database::standart::SQLStandart;
    use crate::models::TargetType;
    use crate::models::model::{Model, ModelDependencies};
    use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
    use std::time::Duration;

    dotenvy::dotenv().ok();

    let pool =
        sqlx::PgPool::connect(&crate::engine::utils::config::load_env::load_env().database_url)
            .await
            .map_err(|e| return anyhow::anyhow!(format!("{}", e)))?;

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
        (TargetType::PositionSize, SQLStandart::SingleModel),
    ];

    let total_runs: u64 = ((targets.len() - 1) as u64 * (10 + 1250 + 2 + 10 + 50 + 20 + 1250))
        + (1250 + 50 + 20 + 1250);

    let mp = MultiProgress::new();

    let overall_style = ProgressStyle::with_template(
        "{spinner:.cyan} [{elapsed_precise}] [{bar:50.cyan/blue}] {pos}/{len} runs  ETA {eta}",
    )
    .unwrap()
    .progress_chars("> ");

    let target_style =
        ProgressStyle::with_template("  {spinner:.green} {prefix:.bold}: {msg}").unwrap();

    let overall_pb = mp.add(ProgressBar::new(total_runs));
    overall_pb.set_style(overall_style);
    overall_pb.enable_steady_tick(Duration::from_millis(100));

    let target_pb = mp.add(ProgressBar::new_spinner());
    target_pb.set_style(target_style);
    target_pb.enable_steady_tick(Duration::from_millis(120));

    let mut all_configs: Vec<(
        TargetType,
        std::collections::HashMap<String, f64>,
        crate::engine::utils::config::config_types::ModelConfig,
    )> = Vec::new();

    for target in targets.iter() {
        let target_name = format!("{:?}", target.0);
        target_pb.set_prefix(target_name.clone());

        // XGBoost
        {
            let n_estimators_arr = [10, 25, 50, 100, 150];
            let max_depth_arr = [1, 2];

            for n_estimators in n_estimators_arr.into_iter() {
                for max_depth in max_depth_arr.into_iter() {
                    match target.clone().0 {
                        TargetType::ActionType => (),
                        _ => {
                            target_pb.set_message(format!(
                                "XGBoost n_estimators={n_estimators} max_depth={max_depth}"
                            ));

                            let mut xgboost = crate::models::xgboost::XGBoost::new(
                                None,
                                crate::models::TaskType::Regression,
                                target.0,
                                target.1,
                                pool.clone(),
                                n_estimators,
                                max_depth,
                            );
                            all_configs.push((
                                target.clone().0,
                                xgboost.train().await?.unwrap(),
                                xgboost.get_config().model.clone(),
                            ));
                            overall_pb.inc(1);
                        }
                    };
                }
            }
        }

        // RandomForest
        {
            let n_trees_arr = [10, 25, 50, 75, 100];
            let max_depth_arr = [1, 2];
            let min_samples_leaf_arr = [1, 2, 5, 8, 10];
            let min_samples_split_arr = [2, 5, 10, 15, 20];
            let m_arr = [1, 2, 3, 4, 5];

            for n_trees in n_trees_arr.into_iter() {
                for max_depth in max_depth_arr.into_iter() {
                    for min_samples_leaf in min_samples_leaf_arr.into_iter() {
                        for min_samples_split in min_samples_split_arr.into_iter() {
                            for m in m_arr.into_iter() {
                                target_pb.set_message(format!("RandomForest n_trees={n_trees} depth={max_depth} leaf={min_samples_leaf} split={min_samples_split} m={m}"));

                                match target.clone().0 {
                                    TargetType::ActionType => {
                                        let mut rf = crate::models::randomforest::RandomForest::new(
                                            None,
                                            crate::models::TaskType::Classification,
                                            target.0,
                                            target.1,
                                            pool.clone(),
                                            n_trees,
                                            max_depth,
                                            min_samples_leaf,
                                            min_samples_split,
                                            m,
                                        );
                                        all_configs.push((
                                            target.clone().0,
                                            rf.train().await?.unwrap(),
                                            rf.get_config().model.clone(),
                                        ));
                                    }
                                    _ => {
                                        let mut rf = crate::models::randomforest::RandomForest::new(
                                            None,
                                            crate::models::TaskType::Regression,
                                            target.0,
                                            target.1,
                                            pool.clone(),
                                            n_trees,
                                            max_depth,
                                            min_samples_leaf,
                                            min_samples_split,
                                            m,
                                        );
                                        all_configs.push((
                                            target.clone().0,
                                            rf.train().await?.unwrap(),
                                            rf.get_config().model.clone(),
                                        ));
                                    }
                                };
                                overall_pb.inc(1);
                            }
                        }
                    }
                }
            }
        }

        // Linear
        {
            let solver_arr = [String::from("SVD"), String::from("QR")];

            for solver in solver_arr.into_iter() {
                target_pb.set_message(format!("Linear solver={solver}"));
                match target.clone().0 {
                    TargetType::ActionType => (),
                    _ => {
                        let mut lr = crate::models::linear::Linear::new(
                            None,
                            crate::models::TaskType::Regression,
                            target.0,
                            target.1,
                            pool.clone(),
                            solver.clone(),
                        );
                        all_configs.push((
                            target.clone().0,
                            lr.train().await?.unwrap(),
                            lr.get_config().model.clone(),
                        ));
                        overall_pb.inc(1);
                    }
                };
            }
        }

        // Ridge
        {
            let alpha_arr = [0.1, 0.5, 1.0, 5.0, 10.0];
            let solver_arr = ["SVD".to_string(), "Cholesky".to_string()];

            for alpha in alpha_arr.into_iter() {
                for solver in solver_arr.clone().into_iter() {
                    match target.clone().0 {
                        TargetType::ActionType => (),
                        _ => {
                            target_pb.set_message(format!("Ridge alpha={alpha} solver={solver}"));
                            let mut ridge = crate::models::ridge::Ridge::new(
                                None,
                                crate::models::TaskType::Regression,
                                target.0,
                                target.1,
                                pool.clone(),
                                solver.clone(),
                                alpha.clone(),
                            );
                            all_configs.push((
                                target.clone().0,
                                ridge.train().await?.unwrap(),
                                ridge.get_config().model.clone(),
                            ));
                            overall_pb.inc(1);
                        }
                    };
                }
            }
        }

        // DecisionTree
        {
            let max_depth_arr = [1, 2];
            let min_samples_leaf_arr = [1, 2, 5, 8, 10];
            let min_samples_split_arr = [2, 5, 10, 15, 20];

            for max_depth in max_depth_arr.into_iter() {
                for min_samples_leaf in min_samples_leaf_arr.into_iter() {
                    for min_samples_split in min_samples_split_arr.into_iter() {
                        target_pb.set_message(format!(
                            "DecisionTree depth={max_depth} leaf={min_samples_leaf} split={min_samples_split}"
                        ));

                        match target.clone().0 {
                            TargetType::ActionType => {
                                let mut dt = crate::models::decisiontree::DecisionTree::new(
                                    None,
                                    crate::models::TaskType::Classification,
                                    target.0,
                                    target.1,
                                    pool.clone(),
                                    max_depth,
                                    min_samples_leaf,
                                    min_samples_split,
                                );
                                all_configs.push((
                                    target.clone().0,
                                    dt.train().await?.unwrap(),
                                    dt.get_config().model.clone(),
                                ));
                            }
                            _ => {
                                let mut dt = crate::models::decisiontree::DecisionTree::new(
                                    None,
                                    crate::models::TaskType::Regression,
                                    target.0,
                                    target.1,
                                    pool.clone(),
                                    max_depth,
                                    min_samples_leaf,
                                    min_samples_split,
                                );
                                all_configs.push((
                                    target.clone().0,
                                    dt.train().await?.unwrap(),
                                    dt.get_config().model.clone(),
                                ));
                            }
                        }

                        overall_pb.inc(1);
                    }
                }
            }
        }

        // KNN
        {
            let algorithm_arr = [String::from("CoverTree"), String::from("LinearSearch")];
            let weight_arr = [String::from("Uniform"), String::from("Distance")];
            let k_arr = [3, 5, 7, 10, 15];

            for algorithm in algorithm_arr.iter() {
                for weight in weight_arr.iter() {
                    for k in k_arr.into_iter() {
                        target_pb.set_message(format!(
                            "KNN algorithm={algorithm} weight={weight} k={k}"
                        ));
                        match target.clone().0 {
                            TargetType::ActionType => {
                                let mut knn = crate::models::knn::KNN::new(
                                    None,
                                    crate::models::TaskType::Classification,
                                    target.0,
                                    target.1,
                                    pool.clone(),
                                    algorithm.clone(),
                                    weight.clone(),
                                    k,
                                );
                                all_configs.push((
                                    target.clone().0,
                                    knn.train().await?.unwrap(),
                                    knn.get_config().model.clone(),
                                ));
                            }
                            _ => {
                                let mut knn = crate::models::knn::KNN::new(
                                    None,
                                    crate::models::TaskType::Regression,
                                    target.0,
                                    target.1,
                                    pool.clone(),
                                    algorithm.clone(),
                                    weight.clone(),
                                    k,
                                );
                                all_configs.push((
                                    target.clone().0,
                                    knn.train().await?.unwrap(),
                                    knn.get_config().model.clone(),
                                ));
                            }
                        }
                        overall_pb.inc(1)
                    }
                }
            }
        }

        // ExtraTrees
        {
            let n_trees_arr = [10, 25, 50, 75, 100];
            let max_depth_arr = [1, 2];
            let min_samples_leaf_arr = [1, 2, 5, 8, 10];
            let min_samples_split_arr = [2, 5, 10, 15, 20];
            let m_arr = [1, 2, 3, 4, 5];

            for n_trees in n_trees_arr.into_iter() {
                for max_depth in max_depth_arr.into_iter() {
                    for min_samples_leaf in min_samples_leaf_arr.into_iter() {
                        for min_samples_split in min_samples_split_arr.into_iter() {
                            for m in m_arr.into_iter() {
                                match target.clone().0 {
                                    TargetType::ActionType => {}
                                    _ => {
                                        target_pb.set_message(format!(
                                            "ExtraTrees n_trees={n_trees} depth={max_depth} leaf={min_samples_leaf} split={min_samples_split} m={m}"
                                        ));

                                        let mut extra_trees =
                                            crate::models::randomforest::RandomForest::new(
                                                None,
                                                crate::models::TaskType::Regression,
                                                target.0,
                                                target.1,
                                                pool.clone(),
                                                n_trees,
                                                max_depth,
                                                min_samples_leaf,
                                                min_samples_split,
                                                m,
                                            );
                                        all_configs.push((
                                            target.clone().0,
                                            extra_trees.train().await?.unwrap(),
                                            extra_trees.get_config().model.clone(),
                                        ));

                                        overall_pb.inc(1);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        overall_pb.set_message(format!("✓ {target_name} done"));
    }

    overall_pb.finish_with_message("✓ All models trained!");
    target_pb.finish_and_clear();

    let metrics = ["mae", "mse", "rmse", "r2", "thr"];
    let higher_is_better: std::collections::HashMap<&str, bool> =
        std::collections::HashMap::from([
            ("mae", false),
            ("mse", false),
            ("rmse", false),
            ("r2", true),
            ("thr", true),
        ]);

    let mut best_by_metric: std::collections::HashMap<
        &str,
        std::collections::HashMap<
            String,
            (
                TargetType,
                std::collections::HashMap<String, f64>,
                crate::engine::utils::config::config_types::ModelConfig,
            ),
        >,
    > = std::collections::HashMap::new();

    for metric in metrics.iter() {
        best_by_metric.insert(metric, std::collections::HashMap::new());
    }

    for entry in all_configs.iter() {
        let (target_type, metrics_map, config) = entry;
        let target_key = format!("{:?}", target_type);

        for metric in metrics.iter() {
            let is_higher_better = *higher_is_better.get(metric).unwrap_or(&false);
            let current_value = match metrics_map.get(*metric) {
                Some(v) => *v,
                None => continue,
            };

            let per_target = best_by_metric.get_mut(metric).unwrap();

            let should_replace = match per_target.get(&target_key) {
                None => true,
                Some((_, best_metrics, _)) => {
                    let best_value = *best_metrics.get(*metric).unwrap_or(&f64::NAN);
                    if is_higher_better {
                        current_value > best_value
                    } else {
                        current_value < best_value
                    }
                }
            };

            if should_replace {
                per_target.insert(
                    target_key.clone(),
                    (target_type.clone(), metrics_map.clone(), config.clone()),
                );
            }
        }
    }

    for metric in metrics.iter() {
        println!("Best configs by metric: {}", metric);

        if let Some(per_target) = best_by_metric.get(metric) {
            let mut sorted_targets: Vec<_> = per_target.values().collect();
            sorted_targets.sort_by(|a, b| format!("{:?}", a.0).cmp(&format!("{:?}", b.0)));

            for (target_type, metrics_map, model_config) in sorted_targets.iter() {
                let metric_value = metrics_map.get(*metric).copied().unwrap_or(f64::NAN);
                println!(
                    "Target: {:?} | {}: {:.6} | Config: {:#?}",
                    target_type, metric, metric_value, model_config
                );
            }
        }
    }

    Ok(())
}
