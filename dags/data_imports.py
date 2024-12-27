from collect.collect import SDMXCollector
from airflow.models.taskinstance import TaskInstance

def import_bop_data(ti: TaskInstance):
  dims = ti.xcom_pull(task_ids='bop_meta', key='bop_dimensions')
  n_args = len(dims)
  print(dims)

def import_inr_data(ti: TaskInstance):
  dims = ti.xcom_pull(task_ids='inr_meta', key='inr_dimensions')

  print(dims)

def import_exr_data(
  source:str,
  resource: str,
  flow_ref: str | list[str],
  ti: TaskInstance,
  params: dict = None):

  dims = ti.xcom_pull(task_ids='exr_meta', key='exr_dimensions')

  collector = SDMXCollector(source, resource)
  data = collector.make_url(flow_ref, params=params)
  print(data)