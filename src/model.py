import numpy as np
import pandas as pd
import os
import sqlalchemy

import sqlalchemy.pool
from xgboost import XGBRegressor
from sklearn.model_selection import RandomizedSearchCV

# Import the data
def get_connection() -> sqlalchemy.pool.base._ConnectionFairy:
    host = "localost"
    port = "5454"
    db = "forex"

    uri = f"postgresql://airflow:airflow@{host}:{port}/{db}"
    con = sqlalchemy.create_engine(uri)

    return con.connect().connection

def get_master() -> pd.DatatFrame:
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
    for j in range(1, output_lags):
      output[f'rate+{j}'] = df['rate'].shift(j)
      output_cols.append(f'rate+{j}')

    output.dropna(inplace=True)

    return (output.loc[:, feat_cols].values,
            output.loc[:, output_cols].values)
    

def train_model(
    data: pd.DatatFrame,
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

    X, y = prepare_data(data)

    if init_params is not None:
        xgb = XGBRegressor(**init_params)
        xgb.fit(X, y)

        return xgb

    else:
        xgb = XGBRegressor()

        rcv = RandomizedSearchCV(
            estimator = xgb,
            param_distribution = param_dist
        )
        rcv.fit(X, y)

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
            model = train_model(data, param_dist)
            model.save_model(f"{model_directory}/{model_path}")
        # If we haven then we just load
        else:
            model.load_model(f"{model_directory}/{model_path}")

        models[group] = model

    return models
            

if __name__ == "__main__":
    data = get_master()

    params = {
      'n_estimators': np.arange(5, 15, step=5),
      'eta': np.logspace(-5, 0, num=5),
      'max_depth': np.arange(5, 20, step=5)
    }

    models = train_or_get_models(
        data,
        "../models",
        param_dist=params
    )
