# Define classes, that will help programmatically import SDMX data.

import requests


class CollectorException(Exception):
  pass


class SDMXCollector:
  def __init__(self,
               source: str,
               resource: str,
               params: dict) -> None:
    
    self.source = source
    self.resource = resource
    self.params = "&".join([f"{key}={val}" 
                            for key, val in params.items()])


  def make_url(self,
               flow_ref: list[str] | str,
               arg_list: list[str] | dict) -> str:

    if isinstance(arg_list, dict):
      arg_list = arg_list.items()

    if isinstance(flow_ref, list):
      flow_ref = self.__create_flowref(flow_ref)

    key = '.'.join(arg_list)
    url = f"https://{self.source}/{self.resource}/data/{flow_ref}/{key}?{self.params}"

    return url

  def __parse_error(self, error_html: str) -> str:
    return error_html

  def __create_flowref(self, agency: str,
                       dataflow: str,
                       dataflow_version: str) -> str:

    return f"{agency},{dataflow},{dataflow_version}"


  def get(self,
          flow_ref: list[str] | str,
          arg_list: list[str] | dict) -> str:

    url = self.make_url(flow_ref, arg_list)
    output = requests.get(url)
    
    if output.status_code != 200 and output.content:
      error_output = self.__parse_error(output.content)
      raise CollectorException(f"The collector can't access data:\n{error_output}")

    elif output.status_code != 200:
      raise CollectorException(f"Unknown error. Error code: {output.status_code()}")

    return output.content
