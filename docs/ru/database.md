# Подготовка базы данных для CTEx-Rs

В текущей реализации проекта Rust SQL-запросы работают с таблицей `dataset`.
По умолчанию приложение читает данные из PostgreSQL, используя значение из переменной окружения `DATABASE_URL`.

## Создание базы данных

1. Откройте терминал.
2. Создайте базу данных PostgreSQL:

```bash
createdb ctex_rs
```

или через `psql`:

```bash
psql -U postgres -c "CREATE DATABASE ctex_rs;"
```

3. Проверьте подключение и создайте `.env` рядом с проектом:

```bash
DATABASE_URL=postgresql://user:password@localhost:5432/ctex_rs
```

## Имя таблицы

Код проекта ожидает таблицу с именем `dataset`.
Если в README указан `candles`, то сейчас фактически используется `dataset`.

## Схема таблицы

В таблице должны быть следующие поля:

- `symbol` — текстовый идентификатор символа (`BTCUSDT`, `ETHUSDT` и т.п.).
- Остальные поля — числовые (в коде читаются как `f64`).
- `timeframe` хранится в секундах, например, `900` для `15m`.

### Рекомендуемая SQL-схема

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

## Комментарии по колонкам

- `symbol` должен быть `TEXT` или `VARCHAR`, он используется для идентификации строки.
- `timeframe` в коде приводится к числу секунд: например, `1m` → `60`, `15m` → `900`.
- Колонки из первого слоя (`return_*`, `ema_*`, `rsi_*`, `macd_diff`, `zscore`, `trend_*` и т.п.) нужны для обучения моделей первого этапа.
- Колонки из второго слоя (`future_*_pred`, `future_*_confidence`, `risk_score_*`, `drawdown_*`, `tail_event_*`, `volatility_spike_*`, `liquidity_drop_*`) нужны для ансамблевых схем и промежуточных прогнозов.
- Колонки из третьего слоя (`future_return`, `action_type`, `position_size`) используются для окончательных решений и `SingleModel`.

## Проверка данных

После создания таблицы убедитесь, что данные вставляются как числа в формате `double precision`.
