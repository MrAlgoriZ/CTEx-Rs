# --2025--

< 22-6-2025:
  - Added dynamic_b
  - Added minimal data collection
  - Added binary classification model on PyTorch

22-6-2025:
  - Added output that everything is saved (on program exit)
  - Changed the neural network algorithm in PyTorch (added Mish, removed LeakyReLU, Adam replaced with AdamW)

  - Tests are run on ETH instead of BTC
  - Binance request logic uncommented
  
25-6-2025:
  - Renamed config_ag.py to config.py
  - Added .gitignore
  - Renamed model from "algoriz" to "ct"

27-6-2025:
  - Changed data_collection.py (introduced minimal OOP)
  - Moved dynamic_b from config.py to data_collection.py
  - Added minimal logging
  - Moved model structure to OOP

  - Added ohlcv_deriv.py (it contains the powerful_ohlcv function)
  - Renamed dynamic_b to dynamic_percent and changed its logic

28-6-2025:
  - Main Binance request was optimized and stopped being a "test"
  - Added candle writing to requests.csv

1-7-2025:
  - There are now 3 models: hard_cap_model, low_cap_model, medium_cap_model
  - Updated .gitignore
  - Added support for more tokens
  - list_of_data() method started accepting parameter is_current: bool
  - Added counters
  - **Added self-learning**
  - Removed extra round() calls to improve logic

2-7-2025:
  - Added token filter by capitalization levels (high-cap, medium-cap, low-cap)
  - Added a function to get volatility
  - Added test function save_all_data()
  - Changed main cycle logic. Added method wait_to_next_minute()
  - Added EarlyStopping to the model

3-7-2025:
  - Added group_size setting in config.py
  
  - Added README.md
  - Main cycle now accepts any supported token
  - Main cycle started checking token volatility

  - Updated README.md

4-7-2025:
  - Main timeframe is now 3 minutes instead of 1 minute
  - Changed main cycle logic
  
  - Main cycle became asynchronous, allowing multiple cycles to run simultaneously

  - Counters became global objects
  - Added project status checking via Telegram (using aiogram)
  - Added a special model training cycle, but it is singleton

20-7-2025:
  - Added parameters to config.py: global_input_size, global_seed
  - Added global seed change via global_seed

21-7-2025:
  - Added explanatory comments
  - Now all cycles start like cycle_with_training
  
  - Changed powerful_ohlcv()

26-7-2025:
  - Changed project structure
  
11-8-2025:
  - Removed hard-cap, medium-cap, low-cap models and replaced them all with binary_model
  - Added indicator and ATR calculation

12-8-2025:
  - Added more indicators
  
13-8-2025:
  - **Added save_cycle**
  - Updated Telegram bot
  - Removed first open for saving to dataset

18-8-2025:
  - Added a C library for indicator calculation and powerful_ohlcv

19-8-2025:
  - Changes in process_targets function
  - Code was prepared for long server runtime

20-8-2025:
  - Small changes in the C library and structure

21-8-2025:
  - Added calculations for daily timeframe candles
  - Changed checking_volatility function

22-8-2025:
  - Architecture changed, code became cleaner and more readable
  - **Added trading_cycle**
  - **Added managers for trading_cycle and loader_cycle (save_cycle)**
  - Updated README
  - Model structure changed again: now each token has its own model and dataset
  - Each token now has its own separate accuracy counter

23-8-2025:
  - Fixed bug with loading the C library

24-8-2025:
  - Wrote a test TUI in C (with macro support and project checks)

25-8-2025:
  - Fixed bug with incorrect copying of nested lists

27-8-2025:
   - Fixed logical bug related to multiple use of powerful_ohlcv and dynamic_percent
   - Updated Telegram bot for new counter structure

28-8-2025:
  - Added requirements.txt file, at that time it included 5 libraries: aiogram, ccxt, colorama, numpy, torch

29-8-2025:
  - Automatic counter reset time reduced by almost 2x

2-10-2025:
  - Another major structure update

3-10-2025:
  - Improved async, added asyncio.to_thread for heavy computations
  - Fixed an error where code could not be stopped with ^C

  - **Transitioned from PyTorch to scikit-learn with HistGradientBoosting model**
  - Improved data saving with pandas + numpy
  - Updated README due to transition to standard ML models
  - Added project license (proprietary)
  
4-10-2025:
  - Returned to original target logic
  - Removed old target logic, almost immediately
  - Then restored the old target logic again
  - Had a crisis and switched from classification to regression. Added a so-called soft-target (with a unique formula, not just percentage change), and with it **success_threshold** for error calculation

5-10-2025:
  - Added one-hot encoding for token to include it in the dataset
  - Changed model structure toward one model that sees tokens

6-10-2025:
  - CSV files replaced by an SQLite database
  - Finalized soft-target and polished logic around it

7-10-2025:
  - Completed full LoaderCycle
  - Optimized code toward imprecise trading data operations

8-10-2025:
  - Added time-features so the model understands time cyclicality
  - soft-target became more sensitive

10-10-2025:
  - soft-target became dependent on ATR
  - Added junk data filter
  - Added default Dockerfile and docker-compose.yml for easy deployment
  - Added settings via .json file

16-10-2025:
  - Structure update: config initialization via factory
  - Now can set scale for soft-target and success_threshold via settings.json
  - Switched to 15-minute timeframe

26-10-2025:
  - Final README update and preparation for migration to Rust

18-10-2025:
  - Start of transition to Rust:
  > Transition to Rust was driven by two reasons:
  > 1. type safety and memory safety
  > 2. speed of compiled code.
  >
  > But overall, it improved code quality and readability, and also removed a lot of unused variables

19-10-2025:
  - Created basic project structure, copied license
  - Created Binance request module, time-features module, and ATR module
  
23-10-2025:
  - Created structures for storing exchange data

24-10-2025:
  - Added async support
  - Immediate refactor of Binance request module to secure it for the long term
  
26-10-2025:
  - Attempted to abandon dynamic_percent, but still had to implement it because there was no alternative
  - Implemented dynamic_percent for all exchange data structures

27-10-2025:
  - Failed attempt to integrate Python for ML model management

29-10-2025:
  - Finalized the full data collection function for use in cycles
  - Added config.yaml as the main configuration file

30-10-2025:
  - Dataset is now stored in PostgreSQL database, integrated via sqlx

3-11-2025:
  - Full config.yaml support via serde_yaml
  - **RandomForestRegressor model ported and RFInterface created**
 
5-11-2025:
  - Transferred TradingCycle
  - Transferred LoaderCycle
  
6-11-2025:
  - Created **CycleManager**
  - Slightly changed counter logic

7-11-2025:
  - Added ability to configure the model directly from config
  - Removed is_significant
  - All counters now use Mutex
  - trading_cycle renamed to training_cycle (because in essence it is training, not trading)

8-11-2025:
  - Added colored output (the most important thing in the project), and ability to disable it via config

15-11-2025:
  - Eliminated soft-target-like philosophies, target is now just percentage change
  - is_significant completely removed
  - Model no longer initializes if LoaderCycle starts first and does not require a model
  - Counters are no longer reset; they now act as a sliding window whose size can be set in config
  
16-11-2025:
  - Added backend as an alternative to Telegram bot

18-11-2025:
  - Attempted to integrate cycle logic into backend (so cycles could be started remotely, even after the program and other cycles were already running)
  - Unsuccessful integration attempts
  
19-11-2025:
  - Complete manager structure overhaul. Now counters are not an object with Mutex, but a blocking service. And cycle start/stop is also a separate blocking service
  - Added ability to stop and start cycles remotely
  - Completed integration, code cleanup, backend documentation for future frontend
  - Updated config, now it is possible to configure ALMOST EVERYTHING
  - Added method test_token(), which allows checking whether a token is valid

20-11-2025:
  - Fixed counter hash table, now there is a guarantee that everything will be in a consistent format
  - Updated README due to config changes
  
23-11-2025:
  - Model training now occurs much less frequently, only if the last 2 iterations on one token were erroneous

22-12-2025:
  - Fixed CounterActor (previously cycle accuracy and backend accuracy were different)
  - Added ability to disable/enable backend via config

25-12-2025:
  - First attempts to write logic for SandboxCycle

27-12-2025:
  - Now minimum and maximum values for success_threshold can be set in config
  - SandboxCycle can now be launched from CycleManager because all dependencies are already known
  - Ability to configure SandboxCycle more deeply via config

28-12-2025:
  - Updates in SandboxCycle logic

# --2026--

9-1-2026:
  - Removed std::sync::Mutex, replaced it everywhere with tokio::sync::Mutex
  - Removed extra parameters from .env
  - Added common traits for all cycles
  - Removed model accuracy check by classification, added custom O(n) success_threshold calculation
  - Removed StandardScaler from the model because the model is RandomForestRegressor

11-1-2026:
  - Added new counter - direction_accuracy, which shows whether the model guessed market direction
  - Changed volatility logic, now it is calculated every iteration of any cycle
  - Code was heavily optimized
  - success_threshold now requires a coefficient from config and volatility for its calculation
  - Added an unsuccessful Binance API refactor

12-1-2026:
  - Model became a separate blocking service
  - Binance API refactor was canceled

13-1-2026:
  - Added rayon for computations related to collect_all()

16-1-2026:
  - Removed async from functions that do not require async
  - Wrote CHANGELOG.md
  - Updated README due to changes in config and CHANGELOG.md

25-1-2026:
  - Wrote the ccxt microservice in Python

28-1-2026:
  - Infrastructure fully moved to Python microservice instead of binance.rs
  - Added ability to configure which exchange to use for requests in config
  - Most error handling changed from String to anyhow::Error
  - Fixed DynamicPercent logic
  - ITicker now has fields that previously belonged to IDayPrice, average_price (reduced API requests)
  - Removed IDayPrice and AveragePrice
  - Renamed ITicker to Ticker, ICandle to Candle and ITime to CircleTime
  - Changed and beautified README

31-1-2026 (preparing for backtests):
  - Added runtime object to config, needed to configure which cycle to choose and which runtime to use (either realtime or backtest)
  - Added CycleError for unique cycle errors
  - Fixed bug with token testing and restarting the cycle with it

1-2-2026:
  - Fixed collect_all issue that collected data from 3 timeframes at once
  - Timeframe is now configured in config
  - Method wait_for_next_interval is now tied to the timeframe
  - Each cycle now works only on one timeframe
  - Added minimal backtests and cosmetics for them
  - Slightly improved error handling

2-2-2026:
  - Fixed backtest bug related to cyclic time
  - Removed unnecessary fn new() from data structures
  - Added ServersActor, which allows controlling requests and server load

3-2-2026:
  - Fixed minor bug in ServersActor logic
  - Added BackgroundCycle
  - Fixed requests to microservice
  - BackgroundCycle now includes server health check updates

5-2-2026:
  - Completely changed dataset and its data collection
  - In model data loading, NaN values are now filtered
  - collect_all is now a CCXTClient method, collect_from_slice is now CollectedData::from_slice(), flat_all is now FlattenedData::from_collected()
  - Added direction accuracy to validation
  - Changed model from RandomForestRegressor to XGBoostRegressor
  - Added AccuracyModel structure
  - Added PredictionActor executor

6-2-2026:
  - Improved backend for PredictionActor
  - Added restart if a server suddenly stops working

9-2-2026:
  - Added Model trait to simplify model creation and enable polymorphism
  - Renamed AccuracyModel to ModelAccuracy
  - Added ModelParams for full polymorphism
  - Returned RandomForest model
  - Added handling of different metrics by single or all at once
  - Added models: Linear, Ridge, DecisionTree, KNN, ExtraTrees

12-2-2026:
  - Performed grid search of hyperparameters for all models I created
  - Grid search failed, all models showed equally poor results
  - Fixed `channel closed` bug in PredictionActor
  - Added IAccount trait
  - Removed old SandboxCycle logic
  - Added minimal backtest in SandboxCycle

09-3-2026:
  - Added SQLStandart to separate queries
  - Removed FlattenedData because it was inconvenient to use indices
  - Replaced FlattenedData with DataMap, which internally uses BTreeMap
  - Changed Ensemble models

14-03-2026:
  - Renamed consts.rs to standart.rs
  - Added Dummy field to SQLStandart
  - Partially implemented new features
  - Completely replaced CollectedData with DataMap
  - Temporarily removed cycle optimizations using Arc

15-03-2026:
  - Fully added new features
  - Added basic Ensemble functionality

30-03-2026:
  - Increased DataMap functionality
  - Removed AddFeatures structure

3-04-2026:
  - Added collect_targets function and changed collect_all

4-04-2026:
  - Added new cycle type "LoaderWM" (LoaderWithModel)
  - Completed collect_targets

6-04-2026:
  - Added classification models
  - Theoretically fixed string formatting bugs
  - Added ccxt_python_service to the CTEx-Rs repository
  - Replaced diff with ratio for error calculation
  - Removed direction accuracy
  - Added generate_accuracy and generate_predictions methods to create initial Ensemble dataset

7-04-2026:
  - Added logging via env_logger
  - Added proper error handling

9-04-2026:
  - Fixed potential bugs when starting model training
  - Added Ensemble model error logging

30-04-2026:
  - Ensemble model finally implemented
  - Fixed several logical bugs

3-05-2026:
  - Added model test structure

18-05-2026:
  - Added ChainActor containing Chain structure used for sequential saving of predictions and targets
  - Added save_plots setting for saving charts

21-05-2026:
  - Fully implemented plot saving
  - Implemented plot saving via REST API

  - **Project recognized as non-working**

27-05-2026:
  - Added documentation
  - **Project released**

31-05-2026:
  - Config divided into 2 files

1-06-2026:
  - Best model configuration test added
  - Logging to file added
  - Project fully translated to English

5-06-2026:
  - Added support for file paths using std::path
  
6-06-2026:
  - Distributed actors across separate files
  
10-06-2026:
  - Replaced Result<\*, anyhow::Error> with anyhow::Result<\*>
