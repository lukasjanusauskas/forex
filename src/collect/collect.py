# Define classes, that will help programmatically import SDMX data.

import requests


class CollectorException(Exception):
  pass


class SDMXCollector:
  def __init__(self,
               source: str,
               resource: str) -> None:
    
    self.source = source
    self.resource = resource

  def make_url(self,
               flow_ref: list[str] | str,
               arg_list: list[str] | None = None,
               n_args: int | None = None,
               params: dict | None = None) -> str:

    if isinstance(flow_ref, list):
      flow_ref = self.__create_flowref(*flow_ref)

    url = f"https://{self.source}/{self.resource}/data/{flow_ref}"

    key = self.__set_key(arg_list, n_args)
    if key:
      url += f"/{key}"

    if params:
      params = "&".join([f"{key}={val}" 
                         for key, val in params.items()])
      url += f"?{params}"

    return url

  def __parse_error(self, error_html: str) -> str:
    return error_html.decode()

  def __create_flowref(self, agency: str,
                       dataflow: str,
                       dataflow_version: str) -> str:

    return f"{agency},{dataflow},{dataflow_version}"

  def __set_key(self,
                arg_list: list[str] | None,
                n_args: int | None):
    key = None
    if arg_list:
      key = '.'.join(arg_list)
    elif n_args:
      key = '.' * (n_args - 1)

    return key
    

  def get(self,
          flow_ref: list[str] | str,
          arg_list: list[str] | None = None,
          n_args: int | None = None,
          params: dict | None = None) -> str:

    url = self.make_url(flow_ref, arg_list, n_args, params)
    output = requests.get(url)
    
    if output.status_code != 200 and output.content:
      error_output = self.__parse_error(output.content)
      raise CollectorException(f"The collector can't access data:\n{error_output}")

    elif output.status_code != 200:
      raise CollectorException(f"Unknown error. Error code: {output.status_code()}")

    return output.content.decode()
