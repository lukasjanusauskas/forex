from collect.collect import SDMXCollector
from airflow.models.taskinstance import TaskInstance

def import_bop_data(ti: TaskInstance):
  dims = ti.xcom_pull(task_ids='bop_meta', key='bop_dimensions')
  print(dims)

def import_inr_data(ti: TaskInstance):
  dims = ti.xcom_pull(task_ids='inr_meta', key='inr_dimensions')
  print(dims)

def import_exr_data(ti: TaskInstance):
  dims = ti.xcom_pull(task_ids='exr_meta', key='exr_dimensions')
  print(dims)