Настройка модели осуществляется в блоке `model` по пути `config/model.yaml` :

### `model_struct` — Структура модели

```yaml
model:
  model_struct: single
```

Тип структуры модели:
- `single` — одна модель на один символ/задачу
- `ensemble` — ансамбль моделей

---

### `params` — Параметры модели

#### Для `model_struct: single` с XGBoost

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

**`task_type`** — тип ML-задачи:
- `regression` — предсказание числового значения (цены, доходности)
- `classification` — предсказание класса (рост/падение)

**`target_type`** — что именно предсказывает модель:
- `position_size` — размер вкладываемого процента от бюджета

#### Для `model_struct: ensemble`

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

### `train_test_split` — Разбивка данных

```yaml
model:
  train_test_split:
    train_ratio: 0.8
```

**`train_ratio`** — доля данных, используемых для обучения. Оставшаяся часть (`1 - train_ratio`) идёт в тест.
- `0.8` = 80% обучение, 20% тест
- Диапазон: `0.0` – `1.0`

---

### `metric` — Метрика качества

```yaml
model:
  metric: R2
```

| Значение    | Описание                                       | Когда использовать                         |
|-------------|------------------------------------------------|--------------------------------------------|
| `MAE`       | Средняя абсолютная ошибка                      | Регрессия, устойчивость к выбросам         |
| `MSE`       | Среднеквадратичная ошибка                      | Регрессия, штраф за большие ошибки         |
| `RMSE`      | Корень из MSE                                  | Регрессия, интерпретируемые единицы        |
| `R2`        | Коэффициент детерминации (0–1)                 | Регрессия, общее качество подгонки         |
| `ACC`       | Точность классификации                         | Классификация                              |
| `THRESHOLD` | Метрика по порогу                              | Бинарная классификация с настройкой порога |
| `RALL`      | Составная метрика                              | Комплексная оценка                         |

---

### `generate_plots` — Генерация графиков

```yaml
model:
  generate_plots: false
```

`true` — сохранять графики после обучения (кривые обучения, предсказания и т.д.).

---

### `seed` — Зерно генератора случайных чисел

```yaml
model:
  seed: 42
```

Фиксирует случайность для воспроизводимых результатов. Любое целое число.
