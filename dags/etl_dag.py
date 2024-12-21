from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from collect.collect import SDMXCollector

def import_bop_data():
  # collector = SDMXCollector("sdmx.oecd.org/public", "rest")

  # n_args = 8
  # flow_ref = ["OECD.SDD.TPS", "DSD_BOP@DF_BOP", ""]

  # sample = collector.get(flow_ref, n_args=n_args, params={"format": "csv", "startPeriod": "2023-Q3"})

  # df = collector.sample_to_pandas(sample, parse_dates=["TIME_PERIOD"])
  # df.drop("DATAFLOW", axis=1, inplace=True)

  # df, factor_array = collector.factorize(df)

  # return df.head()
  pass

def import_inr_data():
  pass

def import_ex_data():
  pass

default_args = {
    'owner': 'lukas',
    'retries': 3,
    'retry_delay': timedelta(days=1)
}

with DAG(
  dag_id='forex_etl',
  default_args=default_args,
  description='The first try at a FOREX data ETL',
  start_date=datetime(2024, 12, 1),
  schedule_interval='@weekly'
) as dag:

  import_bop = PythonOperator(
    task_id="bop_importer",
    python_callable=import_bop_data
  )

  import_inr = PythonOperator(
    task_id="inr_importer",
    python_callable=import_inr_data
  )

  import_ex = PythonOperator(
    task_id="exr_importer",
    python_callable=import_ex_data
  )

  task1 = BashOperator(
    task_id='first_task',
    bash_command='echo Pavyko'
  )

  [import_bop, import_inr, import_ex] >> task1
