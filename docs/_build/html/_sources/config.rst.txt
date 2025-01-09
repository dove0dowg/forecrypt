.. _config:

config
===================================

ForecrypT Configuration Parameters
----------------------------------

This file contains key configuration parameters used by ForecrypT to manage data collection, model training, and forecasting processes.

General Parameters
~~~~~~~~~~~~~~~~~~

- ``CRYPTO_LIST`` (*list*)  
  List of cryptocurrencies to process. Each value represents a cryptocurrency identifier (e.g., ``'BTC'``, ``'ETH'``).

- ``START_DATE`` (*str*)  
  Start date and time for historical data in the format ``yyyy-mm-ddThh:mm:ss``.

- ``FINISH_DATE`` (*str*)  
  End date and time for historical data.


Database Configuration
~~~~~~~~~~~~~~~~~~~~~~

- ``DB_CONFIG`` (*dict*)  
  Parameters for connecting to the PostgreSQL database:

  - ``dbname``: Name of the database.
  - ``user``: Database user.
  - ``password``: User password.
  - ``host``: Database host.
  - ``port``: Port for connection.

  .. note::
     It is recommended to use environment variables instead of hardcoding values:

     - ``FORECRYPT_DB_NAME``
     - ``FORECRYPT_DB_USER``
     - ``FORECRYPT_DB_PASS``
     - ``FORECRYPT_DB_HOST``
     - ``FORECRYPT_DB_PORT``


Model Parameters
~~~~~~~~~~~~~~~~

Each model's parameters are specified in a dictionary within the ``MODEL_PARAMETERS`` variable. Each key represents the model name (e.g., ``'arima'``, ``'ets'``, ``'theta'``). The main model parameters are:

- ``training_dataset_size`` (*int*)  
  [TD] Number of hours of data used for training the model.

- ``model_update_interval`` (*int*)  
  [MU] Time interval (in hours) between model retrainings. If the last retraining was more than this interval ago, the model will retrain.

- ``forecast_dataset_size`` (*int*)  
  [FD] Number of hours of historical data required for forecasting.

- ``forecast_frequency`` (*int*)  
  [FF] Frequency (in hours) of generating forecasts.

- ``forecast_hours`` (*int*)  
  [FH] Number of hours to forecast.

- ``fit_func_name`` (*str*)  
  Path to the function responsible for fitting the model.

- ``specific_parameters`` (*dict*)  
  [SP] Model-specific hyperparameters for training (e.g., ARIMA orders, ETS seasonal periods).

Example configuration for the ARIMA model:

.. code-block:: python

   'arima': {
       'training_dataset_size': 240,
       'model_update_interval': 2880,
       'forecast_dataset_size': 48,
       'forecast_frequency': 120,
       'forecast_hours': 120,
       'fit_func_name': 'model_fits.fit_arima_model',
       'specific_parameters': {
           'order': (24, 3, 8),
           'seasonal_order': (0, 0, 0, 0)
       }
   }


Update Schedule
~~~~~~~~~~~~~~~

- ``HISTORICAL_UPDATE_TIME`` (*dict*)  
  Time for daily updates of historical data. By default, every day at 00:00.

- ``FORECAST_UPDATE_TIME`` (*dict*)  
  Time for hourly forecast updates. By default, every hour at xx:00.


API Parameters
~~~~~~~~~~~~~~

- ``API_KEY`` (*str*)  
  API key for accessing CryptoCompare. Currently, it is not required for free requests but is included for potential future changes.


Additional Parameters
~~~~~~~~~~~~~~~~~~~~~

- ``MODELS_DIRECTORY`` (*str*)  
  Path to the directory for saving and loading models. Used in ``models_processing``.