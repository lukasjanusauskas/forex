from collect.collect import SDMXCollector

from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

from airflow.models.taskinstance import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook

def get_conn():
  hook = PostgresHook(postgres_conn_id='pg_local')
  return hook.get_conn()

def import_bop_data(
  source:str,
  resource: str,
  flow_ref: str | list[str],
  ti: TaskInstance,
  params: dict = None):

  dims = ti.xcom_pull(task_ids='bop_meta', 
                      key='bop_dimensions')
  n_dims = len(dims)

  collector = SDMXCollector(source, resource)
  data = collector.make_url(flow_ref,
                           n_args=n_dims,
                           params=params)

def import_inr_data(
  source:str,
  resource: str,
  flow_ref: str | list[str],
  ti: TaskInstance,
  params: dict = None):

  dims = ti.xcom_pull(task_ids='inr_meta',
                      key='inr_dimensions')
  n_dims = len(dims)

  collector = SDMXCollector(source, resource)
  data = collector.make_url(flow_ref,
                           n_args=n_dims,
                           params=params)

def import_exr_data(
  source:str,
  resource: str,
  flow_ref: str | list[str],
  ti: TaskInstance,
  params: dict = None):

  con = get_conn()

  dims = ti.xcom_pull(task_ids='exr_meta',
                      key='exr_dimensions')

  collector = SDMXCollector(source, resource)
  data = collector.make_url(flow_ref,
                           params=params)

