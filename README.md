# ForecrypT : Omni-Back-Test

Forecrypt is a system for large-scale backtesting of forecasting models on cryptocurrency time series. With this application, you can generate predictions for:

- hundreds of cryptocurrencies

- a dozen models

- dozens of configurations per model

As a result of statistical model processing, all forecasts are stored in ClickHouse, where for every hour, every model configuration, and every prediction branch, a set of analytical metrics is computed. These metrics can be used to build hypotheses about the applicability of different model configurations to cryptocurrency series.

## Requirements 

- Python 3.11+

- Docker Desktop with WSL2 backend (used for PostgreSQL and ClickHouse containers)

- pip (Python package installer)

## Setup

- Clone the repository

- Create and activate a virtual environment

- Run: pip install -r requirements.txt

- Create a .env file with PostgreSQL and ClickHouse credentials (optional if hardcoded)

- Run: python main.py (command line) or python nicegui_ui.py (web dashboard)

First-time execution will start containers, mount volumes, apply schema, and initialize user roles.

## Architecture

- **Python**: data fetch, preprocessing, training, forecast generation, evaluation, logging

- **PostgreSQL**: stores historical prices and forecast outputs, including a materialized backtesting view

- **ClickHouse**: stores deduplicated forecast data and multiple metric tables for fast querying and slicing

## Database Structures

### PostgreSQL:

PostgreSQL is used to ingest and store datasets and model forecasts in small batches from Python. It also provides a materialized view that joins historical and forecast tables (allowing to avoid join operations inside ClickHouse).

- **historical_data**: stores raw price history per cryptocurrency, labeled as 'training' or 'historical'

- **forecast_data**: model predictions including step number, forecast value, model metadata, and timestamps

- **backtest_data_mv**: materialized view aligning historical and forecasted rows by timestamp and cryptocurrency

### ClickHouse:

ClickHouse is used to compute and store mass-scale forecast evaluation metrics. Its performance is leveraged for fast input, aggregation, filtering, and ranking of model configurations.

- **external_pg_forecast_data** (PostgreSQL engine): a live connector table that maps directly to the backtest materialized view in PostgreSQL.

- **forecast_data** (ReplacingMergeTree): deduplicated forecast entries, partitioned by month, ordered by timestamp and model

- **pointwise_metrics** (MergeTree): stores per-step metrics for every forecast row

- **aggregated_metrics** (MergeTree): stores grouped metrics per cryptocurrency and model configuration

- **forecast_window_metrics** (MergeTree): rolling window summaries over each prediction horizon

## Forecast Evaluation Metrics

Each forecasted value is evaluated against real historical values using multiple metric sets. These metrics aim to capture accuracy, directional bias, error scaling, and structural deviations.

- **pointwise_metrics**: `abs_error`, `bias_value`, `squared_error`, `ape`, `perc_error`, `log_error`, `rel_error`, `overprediction`, `underprediction`, `zero_crossed`

- **aggregated_metrics**: `mae`, `mse`, `rmse`, `mape`, `bias_value_mean`, `stddev_bias_value`, `overprediction_rate`, `underprediction_rate`, `max_abs_error`, `max_ape`

- **forecast_window_metrics**: `cumulative_mae`, `cumulative_rmse`, `mean_bias_value`, `error_growth_rate`, `relative_step_error`, `is_reversal`, `step_stddev`, `step_rank`

All ratio-based metrics use **EPSILON** as a regularization constant to avoid division by zero.

## System Output and Usage

After execution, the system produces a wide matrix of forecast branches, each associated with full error metadata. These can be used for inspection, ranking, and graphing:

- Query ClickHouse metric tables to sort forecasts by quality criteria

- Use Plotly to visualize and compare results across configurations (through nicegui)

- Analyze model sensitivity to parameter changes

This dataset serves as a base for building automated configuration selection logic.

## Future Extensions

- Add models such as Prophet, LightGBM, Temporal Fusion Transformers, CatBoost, XGBoost, and Ridge Regression

- Develop correlation maps between input model parameters and output metric scores

- Automate discovery of promising configurations using historical performance statistics

- Combine forecast paths from multiple models for robustness and variance reduction

## License
Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International
