import pandas as pd
from xgboost import XGBRegressor

def get_latest() -> pd.DataFrameGroupBy:
  # Uses last updated date from an XCom and access 
  # to the database to get the latest data
  pass

def produce_forecast(
    model: XGBRegressor,
    data: pd.DataFrame
) -> pd.DataFrame:
  pass

def update_forecasts():
  # Get the data 
  df = get_latest()

  # Get models and produce forecasts
  models = {} 

  for group, data in df:
    model_path = f"model-uni-gbrt-{group}"


  # Produce forecasts

  # Save forecasts

  pass

def log_performance():
  # If there were forecasts, evaluate their performance on new data
  pass