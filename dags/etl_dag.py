from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'lukas',
    'retries': 3,
    'retry_delay': timedelta(days=1)
}

with DAG(
  dag_id='forex_etl',
  default_args=default_args
  description='The first try at a FOREX data ETL',
  start_date=datetime(2024, 12, 1),
  schedule_interval='@weekly'
) as dag:
  task1 = BashOperator(
  )
