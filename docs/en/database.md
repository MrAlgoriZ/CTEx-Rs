# PostgreSQL dataset setup for CTEx-Rs

In the current Rust implementation, SQL queries use the `dataset` table. The application reads data from PostgreSQL via the `DATABASE_URL` environment variable.

## Create the database

1. Open a terminal.
2. Create a PostgreSQL database:

```bash
createdb ctex_rs
```

or with `psql`:

```bash
psql -U postgres -c "CREATE DATABASE ctex_rs;"
```

3. Create a `.env` file next to the project root:

```bash
DATABASE_URL=postgresql://user:password@localhost:5432/ctex_rs
```

## Table name

The code expects a table named `dataset`.
If the README mentions `candles`, the actual database table used by the current Rust code is `dataset`.

## Table schema

The table should contain the following fields:

- `symbol` — text identifier for the symbol (`BTCUSDT`, `ETHUSDT`, etc.).
- All other fields are numeric and read as `f64` in the code.
- `timeframe` is stored in seconds, for example `900` for `15m`.

### Recommended SQL schema

```sql
CREATE TABLE dataset (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    timeframe DOUBLE PRECISION,
    hour_sin DOUBLE PRECISION,
    hour_cos DOUBLE PRECISION,
    minute_sin DOUBLE PRECISION,
    minute_cos DOUBLE PRECISION,
    return_1 DOUBLE PRECISION,
    return_3 DOUBLE PRECISION,
    return_6 DOUBLE PRECISION,
    return_12 DOUBLE PRECISION,
    log_return_1 DOUBLE PRECISION,
    log_return_3 DOUBLE PRECISION,
    log_return_6 DOUBLE PRECISION,
    log_return_12 DOUBLE PRECISION,
    vol_rolling_3 DOUBLE PRECISION,
    vol_rolling_6 DOUBLE PRECISION,
    vol_rolling_12 DOUBLE PRECISION,
    volume_change_1 DOUBLE PRECISION,
    volume_change_3 DOUBLE PRECISION,
    volume_change_6 DOUBLE PRECISION,
    ema_fast DOUBLE PRECISION,
    ema_slow DOUBLE PRECISION,
    rsi_6 DOUBLE PRECISION,
    rsi_12 DOUBLE PRECISION,
    macd_diff DOUBLE PRECISION,
    bb_percent DOUBLE PRECISION,
    zscore DOUBLE PRECISION,
    mean_reversion DOUBLE PRECISION,
    breakout_high_12 DOUBLE PRECISION,
    breakout_low_12 DOUBLE PRECISION,
    breakout_high_24 DOUBLE PRECISION,
    breakout_low_24 DOUBLE PRECISION,
    return_1_over_vol DOUBLE PRECISION,
    return_6_over_vol DOUBLE PRECISION,
    trend_strength DOUBLE PRECISION,
    trend_persistence_3 DOUBLE PRECISION,
    trend_persistence_6 DOUBLE PRECISION,
    trend_persistence_12 DOUBLE PRECISION,
    volatility_regime DOUBLE PRECISION,
    compression_ratio_6 DOUBLE PRECISION,
    compression_ratio_12 DOUBLE PRECISION,
    range_ratio_6 DOUBLE PRECISION,
    range_ratio_12 DOUBLE PRECISION,
    volume_acceleration DOUBLE PRECISION,
    volume_volatility_3 DOUBLE PRECISION,
    volume_volatility_6 DOUBLE PRECISION,
    volume_volatility_12 DOUBLE PRECISION,
    return_autocorr_3 DOUBLE PRECISION,
    return_autocorr_6 DOUBLE PRECISION,
    return_autocorr_12 DOUBLE PRECISION,
    vol_autocorr_3 DOUBLE PRECISION,
    vol_autocorr_6 DOUBLE PRECISION,
    vol_autocorr_12 DOUBLE PRECISION,
    momentum_decay DOUBLE PRECISION,
    trend_memory_3 DOUBLE PRECISION,
    trend_memory_6 DOUBLE PRECISION,
    trend_memory_12 DOUBLE PRECISION,
    downside_vol_3 DOUBLE PRECISION,
    upside_vol_3 DOUBLE PRECISION,
    downside_vol_6 DOUBLE PRECISION,
    upside_vol_6 DOUBLE PRECISION,
    skewness_returns_3 DOUBLE PRECISION,
    kurtosis_returns_3 DOUBLE PRECISION,
    skewness_returns_6 DOUBLE PRECISION,
    kurtosis_returns_6 DOUBLE PRECISION,
    tail_risk_proxy_3 DOUBLE PRECISION,
    tail_risk_proxy_6 DOUBLE PRECISION,
    tail_risk_proxy_12 DOUBLE PRECISION,
    distance_to_vwap DOUBLE PRECISION,
    future_volatility DOUBLE PRECISION,
    future_volume DOUBLE PRECISION,
    future_trend_strength DOUBLE PRECISION,
    future_range DOUBLE PRECISION,
    future_return_mean DOUBLE PRECISION,
    future_return_std DOUBLE PRECISION,
    future_return_skewness DOUBLE PRECISION,
    future_return_kurtosis DOUBLE PRECISION,
    risk_score DOUBLE PRECISION,
    drawdown_probability DOUBLE PRECISION,
    tail_event_probability DOUBLE PRECISION,
    volatility_spike_probability DOUBLE PRECISION,
    liquidity_drop_probability DOUBLE PRECISION,
    future_volatility_pred DOUBLE PRECISION,
    future_volatility_confidence DOUBLE PRECISION,
    future_volume_pred DOUBLE PRECISION,
    future_volume_confidence DOUBLE PRECISION,
    future_trend_strength_pred DOUBLE PRECISION,
    future_trend_strength_confidence DOUBLE PRECISION,
    future_range_pred DOUBLE PRECISION,
    future_range_confidence DOUBLE PRECISION,
    future_return_mean_pred DOUBLE PRECISION,
    future_return_mean_confidence DOUBLE PRECISION,
    future_return_std_pred DOUBLE PRECISION,
    future_return_std_confidence DOUBLE PRECISION,
    future_return_skewness_pred DOUBLE PRECISION,
    future_return_skewness_confidence DOUBLE PRECISION,
    future_return_kurtosis_pred DOUBLE PRECISION,
    future_return_kurtosis_confidence DOUBLE PRECISION,
    risk_score_pred DOUBLE PRECISION,
    risk_score_confidence DOUBLE PRECISION,
    drawdown_probability_pred DOUBLE PRECISION,
    drawdown_probability_confidence DOUBLE PRECISION,
    tail_event_probability_pred DOUBLE PRECISION,
    tail_event_probability_confidence DOUBLE PRECISION,
    volatility_spike_probability_pred DOUBLE PRECISION,
    volatility_spike_probability_confidence DOUBLE PRECISION,
    liquidity_drop_probability_pred DOUBLE PRECISION,
    liquidity_drop_probability_confidence DOUBLE PRECISION,
    future_return DOUBLE PRECISION,
    action_type DOUBLE PRECISION,
    position_size DOUBLE PRECISION
);
```

## Column notes

- `symbol` should be `TEXT` or `VARCHAR`; it identifies the row.
- `timeframe` is converted to seconds in the code: for example `1m` → `60`, `15m` → `900`.
- First-layer columns (`return_*`, `ema_*`, `rsi_*`, `macd_diff`, `zscore`, `trend_*`, etc.) are used for first-stage model training.
- Second-layer columns (`future_*_pred`, `future_*_confidence`, `risk_score_*`, `drawdown_*`, `tail_event_*`, `volatility_spike_*`, `liquidity_drop_*`) are used for ensemble workflows and intermediate predictions.
- Third-layer columns (`future_return`, `action_type`, `position_size`) are used for final decisions and the single model.

## Index recommendations

You can add indexes on `symbol` and `symbol + timeframe`:

```sql
CREATE INDEX idx_dataset_symbol ON dataset(symbol);
CREATE INDEX idx_dataset_symbol_timeframe ON dataset(symbol, timeframe);
```

## Data validation

After creating the table, make sure the loaded values are inserted as `double precision` numbers.

> Note: if you want to use the table name `candles`, change the SQL queries in `src/data/requests/database/requests.rs`.
