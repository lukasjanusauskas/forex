from collect.collect import SDMXCollector

from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

from airflow.models.taskinstance import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas import DataFrame

def get_engine():
  hook = PostgresHook(postgres_conn_id='pg_local')
  return hook.get_sqlalchemy_engine()

def import_data(
  prefix: str,
  ti: TaskInstance,
  source:str,
  resource: str,
  flow_ref: str | list[str],
  params: dict = None,
  n_dims: int | None = None) -> tuple[DataFrame, list]:

  if n_dims is None:
    dims = ti.xcom_pull(task_ids=f'{prefix}_meta', 
                      key=f'{prefix}_dimensions')
    n_dims = len(dims)

  collector = SDMXCollector(source, resource)
  data = collector.get_data(flow_ref,
                           n_args=n_dims,
                           params=params)

  df = SDMXCollector.sample_to_pandas(data, parse_dates=['TIME_PERIOD'])  
  print(df.dtypes)

  return df

def factorize_data(df) -> tuple[DataFrame, dict]:
  df, factors = SDMXCollector.factorize(df)
  return df, factors

def export_dim_tbls(prefix: str, con, tbls: dict):
  for col_name in tbls:
    tbl_name = f'{prefix}_{col_name.lower()}'
    exportable_df = DataFrame(tbls[col_name])
    exportable_df.to_sql(tbl_name, con=con, if_exists='replace')

def import_bop_data(
  source:str,
  resource: str,
  flow_ref: str | list[str],
  ti: TaskInstance,
  params: dict = None):

  df = import_data('bop', ti, source, resource, flow_ref, params)

  df = df[df['FREQ'] == 'Q']
  df['TIME_PERIOD'] = df.loc[:, 'TIME_PERIOD'].astype('datetime64[ns]')

  df, factors = factorize_data(df)

  assert(df.dtypes['TIME_PERIOD'] == 'datetime64[ns]')
  assert(df.shape[0] > 0)

  con = get_engine()
  df.to_sql('balance_of_pay', con, if_exists='replace')

  export_dim_tbls('bop', con, factors)

def import_inr_data(
  source:str,
  resource: str,
  flow_ref: str | list[str],
  ti: TaskInstance,
  params: dict = None):

  df = import_data('inr', ti, source, resource, flow_ref, params)

  # # Because of different date formats that are a cause of 
  # #   different indicators being imported, we have to:

  # # - Filter out the formats, that do not match
  # # - THIS IS A VERY IMPORTANT DATA ASSUMPTION: 
  # #   interest rate indicator has no missing or corupted date formats.
  # #   If one would be present - it would be ommited.  

  # df = df[df['TIME_PERIOD'].str.match(r'\d{4}')]
  # df["TIME_PERIOD"] = df.loc[:, "TIME_PERIOD"].astype("datetime64[ns]")

  df, factors = factorize_data(df)

  con = get_engine()
  df.to_sql('interest_rate', con, if_exists='replace')

  export_dim_tbls('int_rates', con, factors)

def import_exr_data(
  source:str,
  resource: str,
  flow_ref: str | list[str],
  ti: TaskInstance,
  params: dict = None):

  df = import_data('exr', ti, source, resource, flow_ref, params, n_dims=0)
  df, factors = factorize_data(df)

  con = get_engine()
  df.to_sql('exchange_rates', con, if_exists='replace')

  export_dim_tbls('ex_rates', con, factors)
