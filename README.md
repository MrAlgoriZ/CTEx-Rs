# CTEx-Rs - Rust implementation of the 'CTEx-Ai' project

![Status](https://img.shields.io/badge/status-completed-brightgreen) ![License](https://img.shields.io/badge/license-GPL-blue) ![Language](https://img.shields.io/badge/language-Rust-orange)

This repository is a Rust implementation of the 'CTEx-Ai' project. CTEx-Ai is a fully modular realtime algorithmic trading system for exchanges (Binance) using machine learning technologies.

## Run
To run the binary, you must prepare the environment:
  - Start the exchange microservice at https://github.com/MrAlgoriZ/ccxt-python-service (./ccxt_service/start.sh)
  - Create a .env file containing the DATABASE_URL parameter
  - Your database must be prepared before launching the application. More details on database preparation are below in the "Database preparation" section
  - Startup configuration is described in the file at `config/config.yaml`

## Startup configuration
Configs are created automatically, but their parameters can be changed. More details in the [documentation](docs/en/config.md)

## Database preparation
Your table must have a specific name (`candles`) and a specific number of columns. All columns are listed in [database.md](docs/en/database.md)
You must pass the database URL in the environment variable named DATABASE_URL.
The format of this URL must be: `postgresql://example.user:example.password@example.ip:example.port/example.database`

## Build instructions
Copy the files (access to the private repository is required)
```bash
git clone https://github.com/MrAlgoriZ/CTEx-Rs.git
```
Create a .env file (details above), the config file is located at `config/config.yaml`.
Build dependencies: Rust
**Run**
```bash
cargo run --release
```
**Or compile**
```bash
cargo build --release && ./target/release/CTEx-Rs
```

## About the project
The project implements 7 regression models and 3 classification models. It also includes a custom Ensemble model that outputs both regression and classification results.

### Project structure [architecture.md](docs/en/architecture.md):
```
├── Cargo.lock
├── Cargo.toml
├── ccxt_service
│   ├── backend
│   │   └── backend.py
│   ├── ccxt_requests
│   │   └── requests.py
│   ├── private_key.pem
│   ├── public_key.pem
│   ├── pyproject.toml
│   ├── README.md
│   ├── start.sh
│   ├── utils
│   │   ├── cache.py
│   │   ├── crypto.py
│   │   └── errors.py
│   └── uv.lock
├── CHANGELOG.md
├── config
│   └── config.yaml
├── docs
├── LICENSE
├── README.md
└── src
    ├── backend
    │   ├── app.rs
    │   ├── commands.rs
    │   ├── mod.rs
    │   ├── README.md
    │   └── structure.rs
    ├── data
    │   ├── data_interfaces.rs
    │   ├── mod.rs
    │   ├── process
    │   │   ├── data_collection.rs
    │   │   ├── features
    │   │   │   ├── auxiliary.rs
    │   │   │   ├── basic.rs
    │   │   │   └── mod.rs
    │   │   ├── mod.rs
    │   │   └── volatility.rs
    │   └── requests
    │       ├── ccxt
    │       │   ├── account.rs
    │       │   ├── client.rs
    │       │   └── mod.rs
    │       ├── database
    │       │   ├── mod.rs
    │       │   ├── requests.rs
    │       │   └── standart.rs
    │       ├── mod.rs
    │       └── time.rs
    ├── engine
    │   ├── cycles
    │   │   ├── background
    │   │   │   ├── cycle.rs
    │   │   │   └── mod.rs
    │   │   ├── loader
    │   │   │   ├── cycle.rs
    │   │   │   └── mod.rs
    │   │   ├── loaderwm
    │   │   │   ├── cycle.rs
    │   │   │   └── mod.rs
    │   │   ├── manager.rs
    │   │   ├── mod.rs
    │   │   ├── sandbox
    │   │   │   ├── cycle.rs
    │   │   │   └── mod.rs
    │   │   ├── training
    │   │   │   ├── cycle.rs
    │   │   │   └── mod.rs
    │   │   └── traits.rs
    │   ├── mod.rs
    │   ├── state
    │   │   ├── chain.rs
    │   │   ├── counters.rs
    │   │   └── mod.rs
    │   └── utils
    │       ├── colors.rs
    │       ├── config
    │       │   ├── config_types.rs
    │       │   ├── load_config.rs
    │       │   ├── load_env.rs
    │       │   └── mod.rs
    │       ├── mod.rs
    │       └── parse.rs
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
## Support

- Email: [b.a.d.xdev@proton.me](mailto:b.a.d.xdev@proton.me)
- Telegram: [@QmralgorizQ](https://t.me/QmralgorizQ)
- Crypto:
  Network: Ethereum
  Address: `0x1a98835815b2b47d6B4d4Ab830C369980Dcb9E69`
  Currency: ETH
