import numpy as np
import pandas as pd
import os
import logging
import sqlalchemy

import sqlalchemy.pool
from xgboost import XGBRegressor
from sklearn.model_selection import RandomizedSearchCV
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore

import mlflow
from functools import reduce
from datetime import timedelta, date

logging.basicConfig(level=logging.INFO, filename="logs/_model.log")

# Import the data
def get_connection() -> sqlalchemy.pool.base._ConnectionFairy:
    host = "localhost"
    port = "5454"
    db = "forex"

    uri = f"postgresql://airflow:airflow@{host}:{port}/{db}"
    con = sqlalchemy.create_engine(uri)

    return con.connect().connection

def get_master() -> pd.DataFrame:
    con = get_connection()
    return pd.read_sql_query("SELECT * FROM ex_rates", con=con)

def prepare_data(
    data: pd.DataFrame,
    input_lags: int,
    output_lags: int
) -> tuple[np.ndarray, np.ndarray]:
    # Prepares a winowed dataset, with back-filled weekend data

    # Backfill all missed data
    df = data.set_index('time_period')\
             .asfreq('d')\
             .bfill()

    # Track the data, feature and output columns
    output = pd.DataFrame()
    feat_cols = []
    output_cols = ['rate']
    
    # Create lag data
    for i in range(1, input_lags+1):
      output[f'rate-{i}'] = df['rate'].shift(-i)
      feat_cols.append(f'rate-{i}')
   
    output['rate'] = df['rate']
   
    # Create lead data
    if output_lags > 0:
        for j in range(1, output_lags):
          output[f'rate+{j}'] = df['rate'].shift(j)
          output_cols.append(f'rate+{j}')

    output.dropna(inplace=True)

    return (output.loc[:, feat_cols].values,
            output.loc[:, output_cols].values)


def prepare_input_prediction(
    data: pd.DataFrame,
    N: int
) -> np.ndarray:
    """
    Prepares input for prediction by getting the last N values
    """
    df = data.set_index('time_period')\
             .asfreq('d')\
             .bfill()

    # Get the last 15 values
    vals = df.reset_index()\
        .sort_values('time_period')\
        .head(N)['rate']\
        .values
    
    return np.array([vals])

    

def train_model(
    data: pd.DataFrame,
    init_params: dict | None = None,
    param_dist: dict | None = None
) -> XGBRegressor:
    """
    Train the model with already set parameters or parameter distribution

    :param data: the data
    :param init_params: model parameters, whihch will be used by the model directly
    :param param_dist: parameter distribution, which will be used by randomized search
    """
    assert((init_params is not None) or 
           (param_dist is not None),
           "Either init_params or param_dist has to be passed")

    X, y = prepare_data(data, 15, 7)
    curr = data['currency'].values[0]

    if init_params is not None:
        xgb = XGBRegressor(**init_params)
        xgb.fit(X, y)

        logging.info(f'Model for currency {curr} has been trained')

        return xgb

    else:
        xgb = XGBRegressor()

        rcv = RandomizedSearchCV(
            estimator = xgb,
            param_distributions = param_dist
        )
        rcv.fit(X, y)

        logging.info(f'Model for currency {curr} has been trained')

        return rcv.best_estimator_

# Train XGBoost models, if not trained
def train_or_get_models(
    data: pd.DataFrame,
    model_directory: str,
    param_dist: dict
) -> dict:
    """ 
    Trains XGBRegressor models, if they are not present in the `model_directory`, otherwise reads them.
    
    :param data: pandas data frame
    :param model_directory: string of directory, in which directories are saved

    :return: A dictionary, whose keys are currencies
    """
    
    df = data.groupby('currency')
    # Read the directory
    model_files = set(os.listdir(model_directory))
    models = {}
    train_ids = []

    for group, data in df:
        model_path = f"model-uni-gbrt-{group}"
        model = XGBRegressor()

        # If we haven;t yet trained the model: train and save
        if model_path not in model_files:
            train_ids.append(group) 
            model = train_model(data, param_dist=param_dist)
            mlflow.xgboost.save_model(model, f"{model_directory}/{model_path}")

            logging.info(f'Model for currency {group} has been saved')

        # If we haven then we just load
        else:
            model = mlflow.xgboost.load_model(f"{model_directory}/{model_path}")
            logging.info(f'Model for currency {group} has been loaded')

        models[group] = model

    return models

def produce_forecast(
    df: pd.api.typing.DataFrameGroupBy,
    models: dict
) -> pd.DataFrame:
    # Produce predictions
    preds = {}
    last = df.obj['time_period'].max() - timedelta(days=15)

    for group, data in df:
        data_last = data[data['time_period'] > last]
        if data_last.shape[0] == 0:
            continue

        X = prepare_input_prediction(data_last, 15)
        preds[group] = models[group].predict(X)

    # For datetime index creation
    start = last + timedelta(days=16) # Max date + 1, since last is max date - 1, we need to add 16

    # Format them into a pandas dataframe
    forecast_tables = []
    for curr, forecast in preds.items():
        forecast_table = pd.DataFrame(
            data = forecast[0],
            index = pd.date_range(start=start, periods=7, freq='d')
        )
        forecast_table['currency'] = curr
        forecast_tables.append(forecast_table)

    # Merge predictions
    output_table = pd.concat(forecast_tables)

    # Add a date of forecast
    output_table['forecast_date'] = date.today()
    output_table['forecast_error'] = None

    return output_table

def init_system():
    data = get_master()

    params = {
      'n_estimators': np.arange(5, 15, step=5),
      'eta': np.logspace(-5, 0, num=5),
      'max_depth': np.arange(5, 20, step=5)
    }

    models = train_or_get_models(
        data,
        "models",
        param_dist=params
    )

    forecast_table = produce_forecast(data.groupby('currency'), models)




if __name__ == "__main__":
    init_system()