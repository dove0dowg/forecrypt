# Forecrypt: Platform for Cryptocurrency Forecasting

**Forecrypt** is a platform for cryptocurrency forecasting, with its primary advantage being flexibility. After configuring the application, users can tailor the forecasting process to their specific needs.

## Overview

Forecrypt is a set of tools written in Python designed to process time series data and store the results in a PostgreSQL database. The application generates hourly forecasts and saves them as database entries. Historical data sroring as well. It is launched via the `main.py` file, and all configurations are managed through the `config.py` file.

[How to manage the `config.py` configuration?](#)

### Requirements
- Python with required libraries
- A PostgreSQL database

> Docker support is planned for future releases to simplify deployment.

---

## Platform Flexibility

Forecrypt offers the following capabilities, arranged "from simple to advanced":

1. **Single forecast for one cryptocurrency using one model.**  
   Example: a Bitcoin price forecast based on the ARIMA model.

2. **Forecasts for multiple cryptocurrencies and models.**  
   You can generate forecasts for one or several cryptocurrencies using one or multiple models. Models can be fundamentally different (e.g., ARIMA and ETS) or represent different versions of the same model with varying parameters. All results are saved to a unified database table.

3. **Adjustable forecasting and retraining frequency.**  
   Users can specify how often forecasts should be updated and how frequently models should be retrained on new data. This ensures forecasts remain highly accurate and relevant.

4. **Custom time series.**  
   If necessary, you can modify the `fetch_historical_data` function to process any hourly time series, such as data for metals or stock markets. The application logic remains unchanged in this case.  
   [How to set up a custom time series source?](#)

5. **Adding your own model.**  
   If you have a model not included in the platform, you can integrate it for use or comparison with other models.  
   [How to add a custom model?](#)


> **Note:** This project is in an early stage of development. Even the core functionality, such as automating data collection and prediction, is not yet fully implemented. Contributions and suggestions are welcome!

# **Project Goals**

The **primary goal** of this project is to evaluate the applicability of predictive models, such as **ARIMA** or **XGBoost**, on cryptocurrency time series data. The project emphasizes automating the collection and analysis of diverse combinations of data with the following structure:

- **Cryptocurrency Name**
- **Prediction Model**
- **Model Parameters**
- **Time Interval**

---

## **Extended Future Goals**

This project also aspires to achieve **broader objectives**, which would benefit from more contributors:

1. **Discover an Algorithm for Predicting Cryptocurrency Trends** *(ha-haa!)*

2. **Develop a Comprehensive "Custom Crypto Forecasting" Service**:
   - Allowing users to build **personalized forecasts** based on the above data structure.
   - Essential steps include:
     - **API Integration**
     - **Frontend Development**
     - **Cloud Migration**
---

## Conclusion

Forecrypt is a flexible tool that adapts easily to your needs. Whether you're forecasting cryptocurrency trends, financial markets, or other time series data, the platform provides a simple yet powerful infrastructure for data analysis.

---

## Documentation

Detailed documentation is available [here](docs/_build/html/index.html).

---

## License

This project is licensed under the **Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International Public License (CC BY-NC-SA 4.0)**.  
For more information, see the [LICENSE](LICENSE) file.

---
