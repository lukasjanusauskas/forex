from collect.metadata import OECDMetaCollector, ECBMetaCollector
from airflow.models.taskinstance import TaskInstance

def import_bop_meta(source: str,
                    resource: str,
                    flow_ref: list[str] | str,
                    dataflow: str,
                    ti: TaskInstance):

  collector = OECDMetaCollector(source, resource)  
  metadata = collector.get_metadata(flow_ref, dataflow)

  ti.xcom_push(key='bop_dimensions',
               value=metadata.dimensions)

def import_inr_meta(source: str,
                    resource: str,
                    flow_ref: list[str] | str,
                    dataflow: str,
                    ti: TaskInstance):

  collector = OECDMetaCollector(source, resource)  
  metadata = collector.get_metadata(flow_ref, dataflow)

  ti.xcom_push(key='inr_dimensions',
               value=metadata.dimensions)

def improt_exr_meta(source: str,
                    resource: str,
                    flow_ref: list[str] | str,
                    dataflow: str,
                    ti: TaskInstance):

  collector = ECBMetaCollector(source, resource)  
  metadata = collector.get_metadata(flow_ref, dataflow)

  ti.xcom_push(key='ecb_dimensions',
               value=metadata.dimensions)
