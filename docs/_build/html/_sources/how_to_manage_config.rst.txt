How to Manage the config.py Configuration?
==========================================

The ``config.py`` file is the main control element of Forecrypt. Proper configuration allows you to automate the forecasting process for multiple cryptocurrencies, models, and time intervals. By adjusting the parameters, you can set up large-scale "batch" forecasting with minimal manual effort.

Refer to the :ref:`config` section for details about the configuration parameters.

---

Key Concepts in config.py
-------------------------

The ``config.py`` file consists of several key sections that determine the behavior of the forecasting system. Below are detailed examples of how to manage and customize its settings.

---

Example 1: Setting Up Basic Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this example, we'll walk through setting up a basic configuration for forecasting two cryptocurrencies: Bitcoin (BTC) and Ethereum (ETH).

1. Open the ``config.py`` file in your preferred code editor.

   .. image:: static/pics/config_main.PNG
      :alt: Screenshot of config.py main section
      :width: 600px

2. Locate the ``CRYPTO_LIST`` parameter and set it as follows:

.. code-block:: python

   CRYPTO_LIST = ['BTC', 'ETH']

3. Set the ``START_DATE`` and ``FINISH_DATE`` parameters to define the range of historical data:

.. code-block:: python

   START_DATE = "2024-10-01T00:00:00"
   FINISH_DATE = "2025-01-01T00:00:00"

.. image:: static/pics/config_db.PNG
   :alt: Screenshot of database connection parameters
   :width: 600px

4. Optionally, adjust the database connection parameters or use environment variables for enhanced security.

   .. image:: static/pics/config_env.PNG
      :alt: Screenshot of environment variables setup
      :width: 600px

5. Start main.py. Database will be filled by historical(+training dataset) and forecast timeseries.

---

Example 2: Advanced Model Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example demonstrates how to configure advanced model parameters for more precise forecasting.

1. Find the ``MODEL_PARAMETERS`` dictionary in the ``config.py`` file.

2. Customize the parameters for each model according to your needs:

   .. image:: static/pics/config_model_params.PNG
      :alt: Screenshot of model parameter setup
      :width: 600px

3. Below are two examples of ETS model forecasts with different configurations:

   **Example A: ETS model with 120-hour forecast frequency**

   .. image:: static/pics/params_ets_ETH_120hFH_120hFF.PNG
      :alt: ETS model parameters (120h forecast frequency)
      :width: 600px

   .. image:: static/pics/ets_ETH_120hFH_120hFF.PNG
      :alt: ETS model forecast graph (120h forecast frequency)
      :width: 600px

   **Example B: ETS model with 720-hour forecast frequency**

   .. image:: static/pics/params_ets_ETH_720hFH_8760hFF.PNG
      :alt: ETS model parameters (720h forecast frequency)
      :width: 600px

   .. image:: static/pics/ets_ETH_720hFH_8760hFF.PNG
      :alt: ETS model forecast graph (720h forecast frequency)
      :width: 600px

---

Summary of Model Parameters
---------------------------

Below is a detailed explanation of the main model parameters used in ``config.py``:

- **model_name** (e.g., ``arima``) — **[MN]** — Name of the model. Used for tagging in logs and the database.
- **training_dataset_size** — **[TD]** — Number of hours of historical data used for training the model. Ensure this value matches the model's requirements for accuracy.
- **model_update_interval** — **[MU]** — Time interval (in hours) between model retrainings. If the last retraining was more than this value ago, the model will retrain.
- **forecast_dataset_size** — **[FD]** — Number of hours of historical data required for forecasting. This dataset is passed to the model for generating predictions.
- **forecast_frequency** — **[FF]** — Frequency (in hours) for generating forecasts. If the last forecast was created more than this value ago, a new forecast will be generated.
- **forecast_hours** — **[FH]** — Total number of hours the model should forecast. This determines the length of the prediction output.
- **fit_func_name** — Path to the function responsible for fitting the model. This function is dynamically imported and executed.
- **specific_parameters** — **[SP]** — Model-specific hyperparameters required for training. These parameters vary between models (e.g., ARIMA orders, ETS seasonal periods, etc.).

---

Conclusion
----------

By carefully managing the ``config.py`` file, you can fully control the forecasting process in Forecrypt. Whether you're working with a single cryptocurrency or managing complex multi-model forecasts, proper configuration is the key to success.
