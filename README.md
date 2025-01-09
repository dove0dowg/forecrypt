# ForecrypT: Platform for Cryptocurrency Forecasting

**ForecrypT** is a platform for cryptocurrency forecasting, with its primary advantage being flexibility. After configuring the application, users can tailor the forecasting process to their specific needs.

> Docker support is planned for future releases to simplify deployment.

## Overview

ForecrypT is a set of tools written in Python designed to process time series data and store the results in a PostgreSQL database. The application generates hourly forecasts and saves them as database entries. Historical data sroring as well. It is launched via the `main.py` file, and all configurations are managed through the `config.py` file.

[How to manage the `config.py` configuration?](https://dove0dowg.github.io/forecrypt/how_to_manage_config.html)

---

## Platform Flexibility

ForecrypT offers the following capabilities, arranged "from simple to advanced":

1. **Single forecast for one cryptocurrency using one model.**  
   Example: a Bitcoin price forecast based on the ARIMA model.

2. **Forecasts for multiple cryptocurrencies and models.**  
   You can generate forecasts for one or several cryptocurrencies using one or multiple models. Models can be fundamentally different (e.g., ARIMA and ETS) or represent different versions of the same model with varying parameters. All results are saved to a unified database table.

3. **Adjustable forecasting and retraining frequency.**  
   Users can specify how often forecasts should be updated and how frequently models should be retrained on new data. This ensures forecasts remain highly accurate and relevant.

4. **Custom time series.**  
   If necessary, you can modify the `fetch_historical_data` function to process any hourly time series, such as data for metals or stock markets. The application logic remains unchanged in this case.  
   [How to set up a custom time series source?](https://dove0dowg.github.io/forecrypt/custom_ts_source.html)

5. **Adding your own model.**  
   If you have a model not included in the platform, you can integrate it for use or comparison with other models.  
   [How to add a custom model?](https://dove0dowg.github.io/forecrypt/add_custom_model.html)

Project Schemes:
[External Systems](https://dove0dowg.github.io/forecrypt/Systems_of_ForecrypT.html)
[Internal Python Processes](https://dove0dowg.github.io/forecrypt/ForecrypT_Python_Internal_Processes.html)

> **Note:** This project is in an early stage of development. Even the core functionality, such as automating data collection and prediction, is not yet fully implemented. Contributions and suggestions are welcome!

# **Project Goals**

This project automates cryptocurrency forecasting and data collection so you can build a comprehensive knowledge base on the performance of various models (ARIMA, XGBoost, etc.). You decide which datasets to gather and which approaches to test, allowing you to explore and refine forecasts tailored to your specific time series.

In its ready-to-use form, this system provides a robust platform for cryptocurrency forecasting (lol). However, with a precise rework of just one Python function, it can be adapted to any time-series data. This flexibility allows you to leverage the entire pipeline — from data ingestion to model training and evaluation — for a broad range of forecasting scenarios, tailored to your specific needs.
---

## Conclusion

ForecrypT is a flexible tool that adapts easily to your needs. Whether you're forecasting cryptocurrency trends, financial markets, or other time series data, the platform provides a simple yet powerful infrastructure for data analysis.

---

## Documentation

Detailed documentation is available [here](https://dove0dowg.github.io/forecrypt/).

---

## License

This project is licensed under the **Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International Public License (CC BY-NC-SA 4.0)**.  
For more information, see the [LICENSE](LICENSE) file.

---

## **Extended Future Goals**

This project also aspires to achieve **broader objectives**, which would benefit from more contributors:

1. **Robust the application through Docker.**

2. **Write a logic for multi-multi forecasting, based on using main logic with cycle of dynamic config.py input.**

3. **Discover an Algorithm for Predicting Cryptocurrency Trends** *(ha-haa!)*

4. **Develop a Comprehensive "Custom Forecasting" Service**:
   - Allowing users to build **personalized forecasts** based on the above data structure.
   - Essential steps include:
     - **API Integration**
     - **Frontend Development**
     - **Cloud Migration**
---
