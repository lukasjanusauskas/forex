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
  source:str,
  resource: str,
  flow_ref: str | list[str],
  n_args: int,
  params: dict = None) -> tuple[DataFrame, list]:

  collector = SDMXCollector(source, resource)
  data = collector.get_data(flow_ref,
                           n_args=n_args,
                           params=params)
  print(collector.make_url(flow_ref,
                           n_args=n_args,
                           params=params))

  df = SDMXCollector.sample_to_pandas(data)  
  df, factors = SDMXCollector.factorize(df)

  print("----------------- DF shape:", df.shape)
  return df, factors

def import_bop_data(
  source:str,
  resource: str,
  flow_ref: str | list[str],
  ti: TaskInstance,
  params: dict = None):

  dims = ti.xcom_pull(task_ids='bop_meta', 
                      key='bop_dimensions')
  n_dims = len(dims)

  df, factors = import_data(source, resource, flow_ref, n_dims, params)

  con = get_engine()
  df.to_sql('balance_of_pay', con, if_exists='replace')

def import_inr_data(
  source:str,
  resource: str,
  flow_ref: str | list[str],
  ti: TaskInstance,
  params: dict = None):

  dims = ti.xcom_pull(task_ids='inr_meta',
                      key='inr_dimensions')
  n_dims = len(dims)

  df, factors = import_data(source, resource, flow_ref, n_dims, params)

  con = get_engine()
  df.to_sql('interest_rate', con, if_exists='replace')

def import_exr_data(
  source:str,
  resource: str,
  flow_ref: str | list[str],
  ti: TaskInstance,
  params: dict = None):

  con = get_engine()

  dims = ti.xcom_pull(task_ids='exr_meta',
                      key='exr_dimensions')

  df, factors = import_data(source, resource, flow_ref, 0, params)

  con = get_engine()
  df.to_sql('ex_rates', con, if_exists='replace')
