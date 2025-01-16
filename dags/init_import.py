from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from meta_imports import import_bop_meta, import_inr_meta, improt_exr_meta
from data_imports import import_bop_data, import_inr_data, import_exr_data

default_args = {
    'owner': 'lukas',
    'retries': 0
}

with DAG(
  dag_id='forex_elt_v1_20',
  default_args=default_args,
  description='FOREX data ELT pipeline',
  start_date=datetime(2024, 12, 1),
  schedule_interval='@once'
) as dag:

  # Definition of metadata import tasks
  bop_meta = PythonOperator(
    task_id = "bop_meta",
    python_callable = import_bop_meta,
    op_kwargs = {
      'source': 'sdmx.oecd.org', 
      'resource': 'public/rest',
      'flow_ref': ['OECD.SDD.TPS', 'DSD_BOP@DF_BOP/', ''],
      'dataflow': 'dataflow'
    }
  )

  inr_meta = PythonOperator(
    task_id = "inr_meta",
    python_callable = import_inr_meta,
    op_kwargs = {
      'source': 'sdmx.oecd.org', 
      'resource': 'public/rest',
      'flow_ref': ["OECD.SDD.STES", "DSD_KEI@DF_KEI", "4.0"],
      'dataflow': 'dataflow'
    }
  )

  exr_meta = PythonOperator(
    task_id = "exr_meta",
    python_callable = improt_exr_meta,
    op_kwargs = {
      'source': 'data-api.ecb.europa.eu', 
      'resource': 'service',
      'flow_ref': ['ECB', 'ECB_EXR1', '1.0'],
      'dataflow': 'datastructure'
    }
  )

  # Definition of data import tasks
  bop_import = PythonOperator(
    task_id = "import_bop",
    python_callable = import_bop_data,
    op_kwargs = {
      "source": "sdmx.oecd.org",
      "resource": "public/rest",
      "flow_ref": ["OECD.SDD.TPS", "DSD_BOP@DF_BOP", ""],
      "params": {
        "format": "csv",
        "startPeriod": "2024-Q1"
      }
    }
  )

  inr_import = PythonOperator(
    task_id = "import_inr",
    python_callable = import_inr_data,
    op_kwargs = {
      "source": "sdmx.oecd.org",
      "resource": "public/rest",
      "flow_ref": ["OECD.SDD.STES", "DSD_KEI@DF_KEI", "4.0"],
      "params": {
        "format": "csv",
        "startPeriod": "2021-Q1"
      }
    }
  )

  exr_import = PythonOperator(
    task_id = "import_exr",
    python_callable = import_exr_data,
    op_kwargs = {
      "source": "data-api.ecb.europa.eu",
      "resource": "service",
      "flow_ref": "EXR",
      "params": {
        "format": "csvdata",
        "startPeriod": "2024-04-01"
      }
    }
  )

  bop_clean = SQLExecuteQueryOperator(
    task_id = "bop_clean",
    conn_id='pg_local',
    sql="sql/clean_bop.sql"
  )

  inr_clean = SQLExecuteQueryOperator(
    task_id = "inr_clean",
    conn_id='pg_local',
    sql="sql/clean_inr.sql"
  )

  exr_clean = SQLExecuteQueryOperator(
    task_id = "exr_clean",
    conn_id='pg_local',
    sql="sql/clean_exr.sql"
  )

  # make_star = SQLExecuteQueryOperator(
  #   task_id = "make_star",
  #   conn_id='pg_local',
  #   sql="sql/star_schema.sql"
  # )

  # dimension_tbls = SQLExecuteQueryOperator(
  #   task_id = "dimension_tbls",
  #   conn_id='pg_local',
  #   sql="sql/dimension_tbls.sql"
  # )

  # drop_useless = SQLExecuteQueryOperator(
  #   task_id = "drop_useless",
  #   conn_id='pg_local',
  #   sql="sql/drop_redundand.sql"
  # )

  (
  # DAG definition
  # ELT pipelines for each individual source
  [bop_meta >> bop_import >> bop_clean,   # Balance of pay
   inr_meta >> inr_import >> inr_clean,   # Interest rates
   exr_meta >> exr_import >> exr_clean]   # Exchange rates

  #  >> make_star                           # Make a star schema from a relational DB 
  #  >> dimension_tbls                      # Set up dimension tables
  #  >> drop_useless                        # Drop useless tables
  )
