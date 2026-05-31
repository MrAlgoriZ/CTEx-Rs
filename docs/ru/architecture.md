# Архитектура CTEx-Rs

CTEx-Rs - это модульная realtime система алгоритмической торговли на Rust, ориентированная на работу с данными бирж и машинное обучение. Проект разделён на несколько слоёв: внешний сервис для CCXT, слой данных, движок циклов, модельный слой и HTTP API.

## Основные компоненты

1. `src/main.rs`
   - Точка входа приложения
   - Загружает конфигурацию `config/config.yaml`, `config/model.yaml` и переменные окружения `.env`
   - Инициализирует `CycleManager` и запускает циклы для списка активов (`symbols`)
   - Если включён бэкенд, запускает HTTP API на основе `axum`

2. Конфигурация
   - `config/config.yaml` содержит настройки модели, backend, runtime, symbols, сервера CCXT и временные интервалы
   - `src/engine/utils/config/config_types.rs` определяет структуру конфигурации:
     - `ModelConfig`
     - `BackendConfig`
     - `RuntimeConfig`
     - `BehaviourConfig`
     - `PrintsConfig`
     - `TimeframesConfig`

3. Внешний сервис CCXT
   - Каталог `ccxt_service/` содержит Python-микросервис, который работает с библиотекой `ccxt`
   - Rust-приложение обращается к этому сервису через командный интерфейс в `src/data/requests/ccxt/client.rs`
   - Сервис предоставляет загрузку OHLCV, тикеров и тест символов

## Слой данных

Слой данных состоит из двух направлений:

- `src/data/requests/ccxt/`: запросы к CCXT-сервису
  - `client.rs` реализует `CCXTClient`, который запрашивает приоритетный сервер и данные по символу
  - Методы: `fetch_ohlcv`, `fetch_ohlcv_with_timestamp`, `fetch_ticker`, `test_symbol`, `collect_all`

- `src/data/requests/database/`: доступ к базе данных
  - Реализация SQL-стандартных запросов для загрузки исторических данных

- `src/data/process/`: предобработка и генерация признаков
  - `data_collection.rs` собирает данные для обучения и предсказаний
  - `volatility.rs` рассчитывает волатильность
  - `features/` содержит вспомогательные и базовые функции для создания признаков

## Движок и циклы

Движок находится в `src/engine/`

### CycleManager

- `src/engine/cycles/manager.rs` отвечает за управление жизненным циклом задач
- Создаёт акторов и каналы для:
  - `SupervisorCommand`
  - `CounterCommand`
  - `ModelCommand`
  - `PredictionsCommand`
  - `ChainCommand`
  - `ServersCommand`
- Запускает независимые worker-потоки для каждого символа
- Перезапускает цикл при ошибках и обеспечивает корректное удаление состояния

### Типы циклов

`src/engine/utils/config/config_types.rs` определяет `CycleType`:
- `Loader`: загрузка данных и работа без модели, подходит для одиночных моделей
- `Loaderwm`: загрузка с учётом точности моделей, подходит для ансамблевых моделей
- `Training`: обучение модели на данных
- `Sandbox`: торговый цикл без использования кошелька и API бирж

Каждый тип цикла реализован в своём модуле:
- `src/engine/cycles/loader/cycle.rs`
- `src/engine/cycles/loaderwm/cycle.rs`
- `src/engine/cycles/training/cycle.rs`
- `src/engine/cycles/sandbox/cycle.rs`
- `src/engine/cycles/background/cycle.rs`

### Режимы выполнения

В `RuntimeConfig` поддерживается два режима:
- `Realtime`: работа в режиме реального времени
- `Backtest`: воспроизведение исторических данных

Все циклы могут работать в одном из этих режимов и переключаются по конфигурации

## Модельный слой

Папка `src/models/` содержит реализацию алгоритмов и общий интерфейс `Model`

- `src/models/model.rs` описывает общий контракт для всех моделей:
  - загрузка данных (`load_data`)
  - разделение на train/validation (`prepare_data`)
  - оценка качества (`evaluate`)
  - предсказание и обучение
- Поддерживаются модели:
  - `decisiontree.rs`
  - `randomforest.rs`
  - `extratrees.rs`
  - `knn.rs`
  - `linear.rs`
  - `ridge.rs`
  - `xgboost.rs`
  - `ensemble.rs`

`ModelStructure` может быть:
- `Single`: одиночная модель
- `Ensemble`: ансамблевая схема

Метрики включают: MAE, MSE, RMSE, R2, Accuracy и Threshold

## HTTP API

Сервер API реализован в `src/backend/`

- `src/backend/app.rs` строит маршруты `axum`
- `src/backend/commands.rs` содержит обработчики запросов
- `src/backend/structure.rs` описывает структуру пути и состояние API

Основные эндпоинты:
- `/`: корневой маршрут
- `/health`: проверка состояния
- `/cycles`: список активных циклов
- `/cycles/add`: запуск нового цикла
- `/cycles/stop`: остановка цикла
- `/cycles/stop_all`: остановить все циклы
- `/accuracy`: точность предсказаний
- `/predictions`: список предсказаний
- `/generate_plots`: генерация графиков

API использует каналы для отправки команд в `CycleManager` и получения статистики.

## Поток данных и управление

1. Приложение запускается и читает конфигурацию
2. Инициализируется `CycleManager`
3. Для каждого символа из `symbols` создаётся worker
4. Worker выбирает `CycleType` и запускает соответствующий цикл
5. Цикл запрашивает данные через `CCXTClient` или из базы данных
6. Данные проходят предобработку и подаются в модель
7. Результаты сохраняются в состоянии, счётчиках и в API-представлении
8. Если цикл падает, менеджер перезапускает его через паузу

## Взаимодействие с внешними системами

- `ccxt_service/`: Python-сервис для биржевых запросов. Он должен работать отдельно и доступен по адресу из `config/config.yaml`
- База данных PostgreSQL подключается через `DATABASE_URL`
- Веб-сервер API слушает адрес из `backend.listener`

## Особенности архитектуры

- Асинхронный runtime на `tokio`
- Акторная модель через `tokio::sync::mpsc` и `oneshot` каналы
- Разделение ответственности:
  - сбор данных,
  - управление циклами,
  - обучение и предсказание,
  - API и состояние
- Поддержка нескольких символов и нескольких типов циклов одновременно
- Гибкая конфигурация режимов, моделей и вывода логов

## Ключевые модули

- `src/main.rs`: старт приложения и запуск API
- `src/engine/cycles/manager.rs`: оркестрация всех циклов
- `src/data/requests/ccxt/client.rs`: интерфейс к CCXT-сервису
- `src/engine/utils/config/config_types.rs`: модель конфигурации
- `src/models/model.rs`: общий интерфейс машинного обучения
- `src/backend/app.rs`: построение HTTP API
