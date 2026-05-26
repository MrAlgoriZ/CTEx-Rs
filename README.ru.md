# CTEx-Rs - Rust подход проекта 'CTEx-Ai'

![Status](https://img.shields.io/badge/status-completed-brightgreen) ![License](https://img.shields.io/badge/license-GPL-blue) ![Language](https://img.shields.io/badge/language-Rust-orange)

Репозиторий представляет из себя реализацию проекта 'CTEx-Ai' на языке Rust. Проект CTEx-Ai это полноценная модульная realtime система для алгоритмического трейдинга на биржах (Binance) с использованием технологий машинного обучения

## Запуск
Чтобы запустить бинарный файл, вы должны подготовить окружение:
  - Запустите микросервис с биржами https://github.com/MrAlgoriZ/ccxt-python-service (./ccxt_service/start.sh)
  - Создайте .env файл, в котором должен быть реализован параметр DATABASE_URL
  - Ваша база данных должна быть подготовлена до запуска приложения. Подробнее о подготовке базе указано ниже в пункте "Подготовка базы данных"
  - Настройка запуска расписывается в файле по пути "config/config.yaml"

## Настройка запуска
Конфиги создаются автоматически, но их параметры можно менять. Подробнее в [документации](docs/ru/config.md)

## Подготовка базы данных
Ваша таблица должна иметь определенное название (`candles`) и определенное количество столбцов. Все колонки расписаны в [database.md](docs/ru/database.md)
Ссылку на базу данных вы должны передать в переменной окружения с названием DATABASE_URL.
Формат этой ссылки должен быть следующим: "postgresql://example.user:example.password@example.ip:example.port/example.database"

## Инструкция по сборке
Скопируйте файлы (требуется доступ к приватному репозиторию)
```bash
git clone https://github.com/MrAlgoriZ/CTEx-Rs.git
```
Создайте .env (подробнее описано выше), конфиг лежит по пути config/config.yaml.
Зависимости в сборке: Rust
**Запустите**
```bash
cargo run --release
```
**Либо скомпилируйте**
```bash
cargo build --release && ./target/release/CTEx-Rs
```

## О проекте
В проекте реализовано 7 моделей с регрессионным обучением и 3 модели классификации. А также кастомная модель Ensemble, вывод которой и регрессионный и классификационный.

### Структура проекта [architecture.md](docs/ru/architecture.md):
```
├── Cargo.lock
├── Cargo.toml
├── ccxt_service
│   ├── backend
│   │   └── backend.py
│   ├── ccxt_requests
│   │   └── requests.py
│   ├── private_key.pem
│   ├── public_key.pem
│   ├── pyproject.toml
│   ├── README.md
│   ├── start.sh
│   ├── utils
│   │   ├── cache.py
│   │   ├── crypto.py
│   │   └── errors.py
│   └── uv.lock
├── CHANGELOG.md
├── config
│   └── config.yaml
├── docs
├── LICENSE
├── README.md
└── src
    ├── backend
    │   ├── app.rs
    │   ├── commands.rs
    │   ├── mod.rs
    │   ├── README.md
    │   └── structure.rs
    ├── data
    │   ├── data_interfaces.rs
    │   ├── mod.rs
    │   ├── process
    │   │   ├── data_collection.rs
    │   │   ├── features
    │   │   │   ├── auxiliary.rs
    │   │   │   ├── basic.rs
    │   │   │   └── mod.rs
    │   │   ├── mod.rs
    │   │   └── volatility.rs
    │   └── requests
    │       ├── ccxt
    │       │   ├── account.rs
    │       │   ├── client.rs
    │       │   └── mod.rs
    │       ├── database
    │       │   ├── mod.rs
    │       │   ├── requests.rs
    │       │   └── standart.rs
    │       ├── mod.rs
    │       └── time.rs
    ├── engine
    │   ├── cycles
    │   │   ├── background
    │   │   │   ├── cycle.rs
    │   │   │   └── mod.rs
    │   │   ├── loader
    │   │   │   ├── cycle.rs
    │   │   │   └── mod.rs
    │   │   ├── loaderwm
    │   │   │   ├── cycle.rs
    │   │   │   └── mod.rs
    │   │   ├── manager.rs
    │   │   ├── mod.rs
    │   │   ├── sandbox
    │   │   │   ├── cycle.rs
    │   │   │   └── mod.rs
    │   │   ├── training
    │   │   │   ├── cycle.rs
    │   │   │   └── mod.rs
    │   │   └── traits.rs
    │   ├── mod.rs
    │   ├── state
    │   │   ├── chain.rs
    │   │   ├── counters.rs
    │   │   └── mod.rs
    │   └── utils
    │       ├── colors.rs
    │       ├── config
    │       │   ├── config_types.rs
    │       │   ├── load_config.rs
    │       │   ├── load_env.rs
    │       │   └── mod.rs
    │       ├── mod.rs
    │       └── parse.rs
    ├── main.rs
    └── models
        ├── decisiontree.rs
        ├── ensemble.rs
        ├── extratrees.rs
        ├── knn.rs
        ├── linear.rs
        ├── metrics.rs
        ├── model.rs
        ├── mod.rs
        ├── randomforest.rs
        ├── ridge.rs
        └── xgboost.rs
```
## Поддержка

- Email: [b.a.d.xdev@proton.me](mailto:b.a.d.xdev@proton.me)
- Telegram: [@QmralgorizQ](https://t.me/QmralgorizQ)
- Crypto: 
  Network: Ethereum
  Address: `0x1a98835815b2b47d6B4d4Ab830C369980Dcb9E69`
  Currency: ETH
