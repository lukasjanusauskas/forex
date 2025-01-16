# 
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from meta_imports import import_bop_meta, import_inr_meta, improt_exr_meta
from data_imports import get_update_date

default_args = {
    'owner': 'lukas',
    'retries': 3
}

with DAG(
  dag_id='update_elt_v0',
  default_args=default_args,
  description='FOREX data updating ELT pipeline',
  start_date=datetime(2024, 12, 1),
  schedule_interval='@daily'
) as dag:
  
  get_date = PythonOperator(
    task_id="get_date",
    python_callable=get_update_date
  )
