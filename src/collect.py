# Define classes, that will help programmatically import SDMX data.

import logging
import requests


class CollectorException(Exception):
  pass


class SDMXCollector:
  def __init__(self,
               source: str,
               n_dimensions: int) -> None:
    
    self.source = source
    self.n_dimensions = n_dimensions

  def __repr__(self) -> str:
    key = self.__create_key()

    flow_ref = self.__create_flowref()
    url = f"http://{self.source}/{self.agency}/data/{flow_ref}/{key}?{self.params}"

    return url

  def __create_key(self, arg_list: list[str]) -> str:
    assert(len(arg_list) == self.n_dimensions,
           "Your argument list length does not match the number of arguments the collector key requires")
    return '.'.join(arg_list)

  def __create_flowref(self,
                       agency: str,
                       dataflow: str,
                       dataflow_version: str | None) -> str:
    
    return f"{agency},{dataflow},{dataflow_version}"

  def get(self, **kwargs) -> str:
    self.params = kwargs
    
    output = requests.get(self.__repr__())
    
    if output.status_code() == 404:
      raise CollectorException(f"The collector can no longer access data:\n {output.content}")

    return output.content
