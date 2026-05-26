# System configuration

The configuration is stored in `config/config.yaml`. The sections below describe all fields.

---

## `model` — Model settings

The most important section. It defines which model is used, how it is trained, and how it is evaluated. See [model_config.md](model_config.md).

---

## `backend` — HTTP backend settings

```yaml
backend:
  enabled: true
  listener: "0.0.0.0:3000"
  admin_password: "123"
```

**`enabled`** — enable or disable the HTTP API.

**`listener`** — listen address and port.
- `0.0.0.0:3000` — accessible from all interfaces on port 3000
- `127.0.0.1:3000` — accessible only locally

**`admin_password`** — password for protected endpoints. Change it before deployment.

---

## `servers` — data source addresses

```yaml
servers:
  - "127.0.0.1:3737"
```

A list of server addresses that provide market data. Multiple servers can be specified:

```yaml
servers:
  - "127.0.0.1:3737"
  - "127.0.0.1:3738"
```

---

## `symbols` — trading symbols

```yaml
symbols:
  - "BTCUSDT"
```

A list of trading pairs to process:

```yaml
symbols:
  - "BTCUSDT"
  - "ETHUSDT"
  - "SOLUSDT"
```

---

## `main_exchange` — primary exchange

```yaml
main_exchange: binance
```

The exchange identifier used as the data source.

---

## `timeframes` — timeframes

```yaml
timeframes:
  main: "15m"
  background: "1m"
```

**`main`** — the main timeframe for model signals (for example `1m`, `5m`, `15m`, `1h`, `4h`, `1d`).

**`background`** — the background timeframe for auxiliary data or indicators. Usually smaller than the main timeframe.

---

## `mode` — output mode

```yaml
mode: print
```

- `print` — output to stdout (console)
- `log` — write to log file

---

## `runtime` — runtime behaviour

```yaml
runtime:
  type: realtime
  with_training: false
  with_saves: true
  with_model: false
  cycle_type: loader
```

**`type`** — execution type:
- `realtime` — live realtime execution
- `backtest` — historical replay

**`with_training`** — train the model at startup (`true`) or use a loaded model (`false`).

**`with_saves`** — save model and state after training.

**`with_model`** — use an ML model for predictions. `false` means data loading only.

**`cycle_type`** — processing cycle type:

| Value      | Description                                      |
|------------|--------------------------------------------------|
| `loader`   | Load data without a model                         |
| `loaderwm` | Load data with model-aware handling               |
| `training` | Train the model                                   |
| `sandbox`  | Sandbox experiment mode                           |

---

## `behaviour` — system behaviour

```yaml
behaviour:
  success_threshold: 0.125
  accuracy_capacity: 192
  predictions_capacity: 96
```

**`success_threshold`** — threshold for successful prediction. For example, `0.125` means a prediction is considered correct when the error is below 12.5%.

**`accuracy_capacity`** — sliding window size for accuracy calculation (how many latest predictions are included). `192` means the last 192 cycles.

**`predictions_capacity`** — maximum number of stored predictions in memory. `96` means a buffer of 96 entries.

---

## `prints` — output control

Controls detailed console/log output.

### `prints.model`

```yaml
prints:
  model:
    skipped_values: true
    metrics: false
```

**`skipped_values`** — print warnings for missing or invalid values during training.

**`metrics`** — print quality metrics after each training.

### `prints.cycle`

```yaml
prints:
  cycle:
    volatility: true
    cycle_start: true
    price: false
    target: true
    prediction: true
    accuracy: true
```

Controls per-cycle output:

| Field         | Description                                  |
|---------------|----------------------------------------------|
| `volatility`  | current volatility of the instrument         |
| `cycle_start` | message when a new cycle begins              |
| `price`       | current price                                |
| `target`      | target value (actual result)                 |
| `prediction`  | model prediction                             |
| `accuracy`    | current accuracy over the sliding window     |

### `prints.manager`

```yaml
prints:
  manager:
    manager_init: true
    additional_manager_prints: true
```

**`manager_init`** — print when the manager initializes.

**`additional_manager_prints`** — additional debug output from the manager.

---

## Minimal backtest config example

```yaml
symbols:
  - "BTCUSDT"
main_exchange: binance
mode: print

model:
  model_struct: single
  generate_plots: true
  seed: 42
  params:
    Single:
      params:
        XGBoost:
          task_type: regression
          target_type: future_return
          n_estimators: 200
          max_depth: 4
  train_test_split:
    train_ratio: 0.8
  metric: R2

backend:
  enabled: false
  listener: "127.0.0.1:3000"
  admin_password: "changeme"

servers:
  - "127.0.0.1:3737"

runtime:
  type: backtest
  with_training: true
  with_saves: true
  with_model: true
  cycle_type: loaderwm

timeframes:
  main: "15m"
  background: "1m"

behaviour:
  success_threshold: 0.1
  accuracy_capacity: 192
  predictions_capacity: 96

prints:
  model:
    skipped_values: true
    metrics: true
  cycle:
    volatility: false
    cycle_start: true
    price: false
    target: true
    prediction: true
    accuracy: true
  manager:
    manager_init: true
    additional_manager_prints: false
```