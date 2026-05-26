# Project history

CTEx-Rs is not a finished commercial product, but it represents an important milestone in the development of engineering skills and architectural thinking.

The idea began in February 2025, when interest in crypto markets and machine learning led to a plan for an algorithmic system to trade crypto pairs. The initial implementation was written in Python, and most architectural concepts were developed during active development.

One of the early challenges was making the model less dependent on absolute price levels and more robust across different crypto pairs. This led to a shift from using raw price differences to percentage changes.

In early July, the concept of a cyclical self-learning architecture was introduced: the model should operate as repeating stages separated into distinct processes. This structure became the foundation for further development.

Later work focused on machine learning design and architecture: how to avoid a global singleton model while allowing multiple cycles to run concurrently. As the project grew, Python became a performance bottleneck, and in October the decision was made to transition to Rust.

The Rust migration also changed data and configuration patterns: CSV and JSON were replaced with PostgreSQL and YAML. `smartcore` was chosen for the model layer, and a Python microservice `ccxt_service` was adopted for exchange integration, since available Rust ccxt solutions were not sufficient.

The Rust implementation relies on an asynchronous actor model using `tokio::sync::mpsc` and `oneshot`. This allowed cycle control and state management without global objects.

Later, ensemble modeling was introduced. The ensemble processes data in layers and passes quality metrics to the next stage, while maintaining compatibility with single-model workflows. This was one of the more complex parts of the implementation.

`plotters` was used for result visualization. The project did not reach full production maturity, but it provided valuable experience in Python, Rust, ML, asynchronous systems, and microservice architecture.
