
Welcome to ForecrypT's Documentation!
======================================

ForecrypT is a powerful cryptocurrency forecasting tool that combines modern algorithms, real-time data, and seamless database integration. Here's what it can do for you:

- Predict cryptocurrency prices using ARIMA, ETS, and Theta models.
- Store historical and forecast data in PostgreSQL.
- (DEV) Automatically update forecasts and retrain models on a schedule.
- (DEV) Vusialize data though Superset
- (DEV) Use forecast efficiency metrics



.. ForecrypT documentation master file, created by
   sphinx-quickstart on Mon Dec 30 12:08:48 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. toctree::
   :maxdepth: 2
   :caption: Modules

   db_utils
   config
   main
   forecasting
   get_data
   model_fits
   models_processing

.. toctree::
   :maxdepth: 2
   :caption: Guides

   how_to_manage_config
   add_custom_model
   custom_ts_source

.. toctree::
   :maxdepth: 2
   :caption: Schemes

   ForecrypT_Python_Internal_Processes
   Systems_of_ForecrypT