# Конфигурация системы

Конфиг задаётся в файле `config.yaml`. Ниже описаны все секции и поля.

---

## `model` — Настройки модели

Самая важная секция. Определяет, какая модель используется, как обучается и оценивается. Расписано в [model.md](docs/model.md)

---

## `backend` — Настройки HTTP-бэкенда

```yaml
backend:
  enabled: true
  listener: "0.0.0.0:3000"
  admin_password: "123"
```

**`enabled`** — включить/выключить HTTP API.

**`listener`** — адрес и порт для прослушивания.
- `0.0.0.0:3000` — доступен со всех интерфейсов на порту 3000
- `127.0.0.1:3000` — только локально

**`admin_password`** — пароль для защищённых эндпоинтов. **Обязательно смените перед деплоем.**

---

## `servers` — Адреса источников данных

```yaml
servers:
  - "127.0.0.1:3737"
```

Список адресов серверов, откуда система получает рыночные данные. Можно указать несколько:

```yaml
servers:
  - "127.0.0.1:3737"
  - "127.0.0.1:3738"
```

---

## `symbols` — Торговые символы

```yaml
symbols:
  - "BTCUSDT"
```

Список торговых пар для обработки:

```yaml
symbols:
  - "BTCUSDT"
  - "ETHUSDT"
  - "SOLUSDT"
```

---

## `main_exchange` — Основная биржа

```yaml
main_exchange: binance
```

Идентификатор биржи, используемой как источник данных.

---

## `timeframes` — Таймфреймы

```yaml
timeframes:
  main: "15m"
  background: "1m"
```

**`main`** — основной таймфрейм для сигналов модели (например, `1m`, `5m`, `15m`, `1h`, `4h`, `1d`).

**`background`** — фоновый таймфрейм для дополнительных данных или индикаторов. Обычно меньше основного.

---

## `mode` — Режим вывода

```yaml
mode: print
```

- `print` — выводить всё в stdout (консоль)
- `log` — писать в лог-файл

---

## `runtime` — Режим работы системы

```yaml
runtime:
  type: realtime
  with_training: false
  with_saves: true
  with_model: false
  cycle_type: loader
```

**`type`** — тип исполнения:
- `realtime` — работа в реальном времени, подключение к бирже
- `backtest` — тестирование на исторических данных

**`with_training`** — обучать модель при старте (`true`) или загрузить сохранённую (`false`).

**`with_saves`** — сохранять модель и состояние после обучения.

**`with_model`** — использовать ML-модель для предсказаний. `false` = режим без модели (только загрузка данных).

**`cycle_type`** — тип цикла обработки:

| Значение    | Описание                                                    |
|-------------|-------------------------------------------------------------|
| `loader`    | Загрузка данных без модели                                  |
| `loaderwm`  | Загрузка данных с моделью (loader with model)               |
| `training`  | Режим обучения                                              |
| `sandbox`   | Песочница для экспериментов                                 |

---

## `behaviour` — Поведение системы

```yaml
behaviour:
  success_threshold: 0.125
  accuracy_capacity: 192
  predictions_capacity: 96
```

**`success_threshold`** — порог для определения успешного предсказания. Например, `0.125` означает, что предсказание считается верным, если ошибка меньше 12.5%.

**`accuracy_capacity`** — размер скользящего окна для расчёта точности (сколько последних предсказаний учитывается). `192` = последние 192 цикла.

**`predictions_capacity`** — максимальное количество хранимых предсказаний в памяти. `96` = буфер на 96 записей.

---

## `prints` — Управление выводом

Позволяет детально настроить, что выводится в консоль/лог.

### `prints.model`

```yaml
prints:
  model:
    skipped_values: true
    metrics: false
```

**`skipped_values`** — выводить предупреждения о пропущенных/некорректных значениях при обучении.

**`metrics`** — выводить метрики качества после каждого обучения.

---

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

Управляет выводом в каждом цикле обработки:

| Поле          | Описание                                       |
|---------------|------------------------------------------------|
| `volatility`  | Текущая волатильность инструмента              |
| `cycle_start` | Сообщение о начале нового цикла                |
| `price`       | Текущая цена                                   |
| `target`      | Целевое значение (реальный результат)          |
| `prediction`  | Предсказание модели                            |
| `accuracy`    | Текущая точность на скользящем окне            |

---

### `prints.manager`

```yaml
prints:
  manager:
    manager_init: true
    additional_manager_prints: true
```

**`manager_init`** — вывод при инициализации менеджера символов.

**`additional_manager_prints`** — дополнительная отладочная информация менеджера.

---

## Пример минимального конфига для бэктеста с обучением

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
