from collect.metadata import OECDMetaCollector, ECBMetaCollector
from airflow.models.taskinstance import TaskInstance

from data_imports import get_engine

import pandas as pd

def export_meta(codelists: dict, prefix: str):
  con = get_engine()

  for key, code_dict in codelists:
    # Name the table {shortened name of the table}_{name of the variable}
    tbl_name = f"{prefix}_{key.lower()}_codelist"

    # Convert dictionary to dataframe
    tbl = pd.DataFrame(code_dict)
    tbl.to_sql(name=tbl_name, con=con, if_exists="replace")

def import_bop_meta(source: str,
                    resource: str,
                    flow_ref: list[str] | str,
                    dataflow: str,
                    ti: TaskInstance):

  collector = OECDMetaCollector(source, resource)  
  metadata = collector.get_metadata(flow_ref, dataflow)

  ti.xcom_push(key='bop_dimensions',
               value=metadata.dimensions)

  export_meta(metadata.codelists, prefix="bop")


def import_inr_meta(source: str,
                    resource: str,
                    flow_ref: list[str] | str,
                    dataflow: str,
                    ti: TaskInstance):

  collector = OECDMetaCollector(source, resource)  
  metadata = collector.get_metadata(flow_ref, dataflow)

  ti.xcom_push(key='inr_dimensions',
               value=metadata.dimensions)

  export_meta(metadata.codelists, prefix="inr")

def improt_exr_meta(source: str,
                    resource: str,
                    flow_ref: list[str] | str,
                    dataflow: str,
                    ti: TaskInstance):

  collector = ECBMetaCollector(source, resource)  
  metadata = collector.get_metadata(flow_ref, dataflow)

  ti.xcom_push(key='ecb_dimensions',
               value=metadata.dimensions)

  export_meta(metadata.codelists, prefix="exr")
