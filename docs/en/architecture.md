# CTEx-Rs Architecture

CTEx-Rs is a modular realtime trading system written in Rust, designed to work with exchange data and machine learning. The project is organized into several layers: an external CCXT service, a data layer, a cycle engine, a model layer, and an HTTP API.

## Main components

1. `src/main.rs`
   - Application entry point.
   - Loads configuration from `config/config.yaml`, `config/model.yaml` and environment variables from `.env`.
   - Initializes `CycleManager` and starts cycles for the configured symbols.
   - If backend is enabled, starts an HTTP API using `axum`.

2. Configuration
   - `config/config.yaml` contains settings for models, backend, runtime, symbols, CCXT servers, and timeframes.
   - `src/engine/utils/config/config_types.rs` defines the configuration structure:
     - `ModelConfig`
     - `BackendConfig`
     - `RuntimeConfig`
     - `BehaviourConfig`
     - `PrintsConfig`
     - `TimeframesConfig`

3. External CCXT service
   - The `ccxt_service/` folder contains a Python microservice that uses the `ccxt` library.
   - The Rust application communicates with this service through `src/data/requests/ccxt/client.rs`.
   - The service provides OHLCV loading, ticker data, and symbol validation.

## Data layer

The data layer has two directions:

- `src/data/requests/ccxt/`: CCXT service requests
  - `client.rs` implements `CCXTClient`, which requests a priority server and fetches symbol data.
  - Methods: `fetch_ohlcv`, `fetch_ohlcv_with_timestamp`, `fetch_ticker`, `test_symbol`, `collect_all`.

- `src/data/requests/database/`: database access
  - Implements SQL-standard queries for loading historical data.

- `src/data/process/`: preprocessing and feature generation
  - `data_collection.rs` collects data for training and predictions.
  - `volatility.rs` computes volatility.
  - `features/` contains helper and basic feature-building utilities.

## Engine and cycles

The engine lives in `src/engine/`.

### CycleManager

- `src/engine/cycles/manager.rs` manages task lifecycle and orchestrates cycles.
- It creates actors and channels for:
  - `SupervisorCommand`
  - `CounterCommand`
  - `ModelCommand`
  - `PredictionsCommand`
  - `ChainCommand`
  - `ServersCommand`
- It launches independent worker tasks for each symbol.
- It restarts cycles on errors and ensures proper state cleanup.

### Cycle types

`src/engine/utils/config/config_types.rs` defines `CycleType`:
- `Loader` — data loading without a model, suitable for single-model workflows.
- `Loaderwm` — data loading with model-aware processing, suitable for ensemble models.
- `Training` — model training mode.
- `Sandbox` — experimental sandbox mode.

Each cycle type is implemented in its own module:
- `src/engine/cycles/loader/cycle.rs`
- `src/engine/cycles/loaderwm/cycle.rs`
- `src/engine/cycles/training/cycle.rs`
- `src/engine/cycles/sandbox/cycle.rs`
- `src/engine/cycles/background/cycle.rs`

### Runtime modes

`RuntimeConfig` supports two modes:
- `Realtime` — live data execution.
- `Backtest` — historical replay.

All cycles can run in either mode and switch based on configuration.

## Model layer

The `src/models/` folder contains model implementations and the shared `Model` interface.

- `src/models/model.rs` defines a common contract for all models:
  - data loading (`load_data`)
  - train/validation split (`prepare_data`)
  - quality evaluation (`evaluate`)
  - prediction and training logic
- Supported models:
  - `decisiontree.rs`
  - `randomforest.rs`
  - `extratrees.rs`
  - `knn.rs`
  - `linear.rs`
  - `ridge.rs`
  - `xgboost.rs`
  - `ensemble.rs`

`ModelStructure` can be:
- `Single` — a single model
- `Ensemble` — an ensemble scheme

Supported metrics include: MAE, MSE, RMSE, R2, Accuracy, and Threshold.

## HTTP API

The API server is implemented in `src/backend/`.

- `src/backend/app.rs` builds `axum` routes.
- `src/backend/commands.rs` contains request handlers.
- `src/backend/structure.rs` defines route structure and API state.

Main endpoints:
- `/` — root endpoint
- `/health` — health check
- `/cycles` — active cycles list
- `/cycles/add` — start a new cycle
- `/cycles/stop` — stop a cycle
- `/cycles/stop_all` — stop all cycles
- `/accuracy` — prediction accuracy
- `/predictions` — prediction list
- `/generate_plots` — generate plots

The API uses channels to send commands to `CycleManager` and receive statistics.

## Data and control flow

1. The application starts and reads configuration.
2. `CycleManager` is initialized.
3. A worker is created for each configured symbol.
4. Each worker selects a `CycleType` and starts the corresponding cycle.
5. The cycle requests data through `CCXTClient` or the database.
6. The data is preprocessed and fed to the model.
7. Results are stored in application state, counters, and API output.
8. If a cycle fails, the manager restarts it after a pause.

## External integrations

- `ccxt_service/` — Python microservice for exchange requests. It must run separately and be reachable at the address configured in `config/config.yaml`.
- PostgreSQL is connected via `DATABASE_URL`.
- The API server listens on the address from `backend.listener`.

## Architectural highlights

- Asynchronous runtime based on `tokio`.
- Actor model using `tokio::sync::mpsc` and `oneshot` channels.
- Clear separation of responsibility:
  - data collection,
  - cycle management,
  - model training and prediction,
  - API and state handling.
- Support for multiple symbols and multiple cycle types concurrently.
- Flexible configuration of modes, models, and logging output.

## Key modules

- `src/main.rs` — application startup and optional API launch.
- `src/engine/cycles/manager.rs` — cycle orchestration.
- `src/data/requests/ccxt/client.rs` — CCXT service interface.
- `src/engine/utils/config/config_types.rs` — configuration model.
- `src/models/model.rs` — shared machine learning interface.
- `src/backend/app.rs` — HTTP API setup.
