How to Add a Custom Model?
==========================

Adding a custom model to Forecrypt involves integrating your model's fitting and forecasting logic into the existing framework. The goal is to ensure that your model can be trained and produce forecasts in a way compatible with the system.

To achieve this, you need to:

1. **Create a fitting function** that returns a trained model object.
2. **Ensure the trained model provides the required methods** for forecasting.
3. **Register the model in the configuration file** by specifying its fitting function and parameters.

---

Steps to Add a Custom Model
---------------------------

1. Implement the Fitting Function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The fitting function is responsible for training the model and returning a model object that can be used for forecasting. This function should be added to the ``model_fits.py`` module.

The function must:
- Accept a pandas DataFrame as input, with a ``date`` index and a ``price`` column.
- Return a trained model object.

---

2. Ensure Compatibility of the Model Object
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The returned model object must support forecasting. Specifically, it should provide a method to generate forecasts for a specified number of steps.

For example, ``get_forecast`` method.

---

3. Register the Model in ``config.py``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After implementing the fitting function, you need to register the new model in the ``MODEL_PARAMETERS`` section of ``config.py``. This involves specifying the path to the fitting function and defining model-specific parameters.

Example configuration for the custom linear regression model:
