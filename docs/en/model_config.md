Model settings are configured under the `model` section in `config/model.yaml`.

## `model_struct` — model structure

```yaml
model:
  model_struct: single
```

Model structure types:
- `single` — a single model for one symbol/task
- `ensemble` — an ensemble of models

---

## `params` — model parameters

### For `model_struct: single` with XGBoost

```yaml
model:
  model_struct: single
  params:
    type: single
    kind: XGBoost
    task_type: regression
    target_type: position_size
    n_estimators: 100
    max_depth: 5
```

**`task_type`** — ML task type:
- `regression` — predict a numeric value
- `classification` — predict a class

**`target_type`** — the predicted target:
- `position_size` — the position size to allocate

### For `model_struct: ensemble`

```yaml
model:
  model_struct: ensemble
  params:
    type: ensemble
    future_volatility_model_params:
      kind: XGBoost
      task_type: regression
      target_type: future_volatility
      n_estimators: 100
      max_depth: 1
    future_volume_model_params:
      kind: XGBoost
      task_type: regression
      target_type: future_volume
      n_estimators: 100
      max_depth: 1
    future_trend_strength_model_params:
      kind: XGBoost
      task_type: regression
      target_type: future_trend_strength
      n_estimators: 100
      max_depth: 1
    future_range_model_params:
      kind: XGBoost
      task_type: regression
      target_type: future_range
      n_estimators: 100
      max_depth: 1
    future_return_mean_model_params:
      kind: XGBoost
      task_type: regression
      target_type: future_return_mean
      n_estimators: 100
      max_depth: 1
    future_return_std_model_params:
      kind: XGBoost
      task_type: regression
      target_type: future_return_std
      n_estimators: 100
      max_depth: 1
    future_return_skew_model_params:
      kind: XGBoost
      task_type: regression
      target_type: future_return_skewness
      n_estimators: 100
      max_depth: 1
    future_return_kurt_model_params:
      kind: XGBoost
      task_type: regression
      target_type: future_return_kurtosis
      n_estimators: 100
      max_depth: 1
    risk_score_model_params:
      kind: XGBoost
      task_type: regression
      target_type: risk_score
      n_estimators: 100
      max_depth: 1
    drawdown_probability_model_params:
      kind: XGBoost
      task_type: regression
      target_type: drawdown_probability
      n_estimators: 100
      max_depth: 1
    tail_event_probability_model_params:
      kind: XGBoost
      task_type: regression
      target_type: tail_event_probability
      n_estimators: 100
      max_depth: 1
    volatility_spike_probability_model_params:
      kind: XGBoost
      task_type: regression
      target_type: volatility_spike_probability
      n_estimators: 100
      max_depth: 1
    liquidity_drop_probability_model_params:
      kind: XGBoost
      task_type: regression
      target_type: liquidity_drop_probability
      n_estimators: 100
      max_depth: 1
    future_return_model_params:
      kind: XGBoost
      task_type: regression
      target_type: future_return
      n_estimators: 100
      max_depth: 1
    action_type_model_params:
      kind: RandomForest
      task_type: classification
      target_type: action_type
      n_trees: 100
      max_depth: 1
      min_samples_leaf: 1
      min_samples_split: 1
      m: 1
    position_size_model_params:
      kind: XGBoost
      task_type: regression
      target_type: position_size
      n_estimators: 100
      max_depth: 1
```

---

## `train_test_split` — data split

```yaml
model:
  train_test_split:
    train_ratio: 0.8
```

**`train_ratio`** — the fraction of data used for training. The remainder (`1 - train_ratio`) is used for testing.
- `0.8` = 80% training, 20% testing
- Range: `0.0` to `1.0`

---

## `metric` — evaluation metric

```yaml
model:
  metric: R2
```

| Value       | Description                                     | Use case                                  |
|-------------|-------------------------------------------------|-------------------------------------------|
| `MAE`       | mean absolute error                             | regression, outlier robustness            |
| `MSE`       | mean squared error                              | regression, penalizes large errors        |
| `RMSE`      | root mean squared error                         | regression, interpretable units           |
| `R2`        | coefficient of determination                     | regression, overall model fit             |
| `ACC`       | classification accuracy                          | classification                            |
| `THRESHOLD` | threshold-based accuracy                         | binary classification with custom margin  |
| `RALL`      | combined metric                                  | comprehensive assessment                  |

---

## `generate_plots` — plot generation

```yaml
model:
  generate_plots: false
```

`true` — save plots after training.

---

## `seed` — random seed

```yaml
model:
  seed: 42
```

Fixes randomness for reproducible results. Any integer value is valid.
