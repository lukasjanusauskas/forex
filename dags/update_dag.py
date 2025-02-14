from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from data_imports import set_update_date, import_exr_updates

default_args = {
    'owner': 'lukas',
    'retries': 3
}

with DAG(
  dag_id='update_elt_v1_01',
  default_args=default_args,
  description='FOREX data updating ELT pipeline',
  start_date=datetime(2024, 12, 1),
  schedule_interval='@once'
) as dag:
  
  get_date = PythonOperator(
    task_id="get_date",
    python_callable=set_update_date
  )

  import_updates = PythonOperator(
    task_id="import_updates",
    python_callable=import_exr_updates,
    op_kwargs={
      "source": "data-api.ecb.europa.eu",
      "resource": "service",
      "flow_ref": "EXR",
      "params": {"format": "csvdata"}
    }
  )

  insert_updates = SQLExecuteQueryOperator(
    task_id="insert_updates",
    conn_id='pg_local',
    sql='sql/update_exr.sql'
  )

  # update_forecast = PythonOperator(
  #   task_id="update_forecast",
  #   python_callable=update_forecasts
  # )

  # log_perf = PythonOperator(
  #   task_id="update_forecast",
  #   python_callable=log_performance
  # )

  # DAG definition
  (
    get_date              # Get the date of the 
    >> import_updates     # Import the updates in their raw format and put them into DB
    >> insert_updates     # Clean and insert the data into the ex_rates table
    # >> update_forecast    # Update the forecasts and errors of previous forecasts
    # >> log_perf           # Log the performance to MLFlow``
  )
  
