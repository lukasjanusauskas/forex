from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from meta_imports import import_bop_meta, import_inr_meta, improt_exr_meta
from data_imports import import_bop_data


default_args = {
    'owner': 'lukas',
    'retries': 3,
    'retry_delay': timedelta(days=1)
}

with DAG(
  dag_id='forex_etl_v0_03',
  default_args=default_args,
  description='The first try at a FOREX data ETL',
  start_date=datetime(2024, 12, 1),
  schedule_interval='@weekly'
) as dag:

  # Definition of metadata import tasks
  bop_meta = PythonOperator(
    task_id = "bop_meta",
    python_callable = import_bop_meta,
    op_kwargs = {
      'source': 'sdmx.oecd.org', 
      'resource': 'public/rest',
      'flowref': ['OECD.SDD.TPS', 'DSD_BOP@DF_BOP/', ''],
      'dataflow': 'dataflow'
    }
  )

  inr_meta = PythonOperator(
    task_id = "inr_meta",
    python_callable = import_inr_meta,
    op_kwargs = {
      'source': 'sdmx.oecd.org', 
      'resource': 'public/rest',
      'flowref': ["OECD.SDD.STES", "DSD_KEI@DF_KEI", "4.0"],
      'dataflow': 'dataflow'
    }
  )

  exr_meta = PythonOperator(
    task_id = "exr_meta",
    python_callable = improt_exr_meta,
    op_kwargs = {
      'source': 'data-api.ecb.europa.eu', 
      'resource': 'service',
      'flowref': ['ECB', 'ECB_EXR1', '1.0'],
      'dataflow': 'datastructure'
    }
  )

  # Definition of data import tasks
  bop_import = PythonOperator(
    task_id = "import_bop",
    python_callable = import_bop_data
  )

  inr_import = PythonOperator(
    task_id = "import_bop",
    python_callable = import_bop_data
  )

  exr_import = PythonOperator(
    task_id = "import_bop",
    python_callable = import_bop_data
  )

  # DAG definition
  [bop_meta >> bop_import, 
   inr_meta >> inr_import,
   exr_meta >> exr_import] 
