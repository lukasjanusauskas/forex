# Define classes, that will help programmatically import SDMX data.

import requests
import pandas as pd
from io import StringIO
from lxml import etree


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
    # Makes the url for data retrieval

    if isinstance(flow_ref, list):
      flow_ref = self.__create_flowref(delimeter=",", *flow_ref)

    # Defines url following conventional SDMX rules
    url = f"https://{self.source}/{self.resource}/data/{flow_ref}"

    # Key defines a query. 
    # Query arguments go in between dots, 
    # position of query fields follow the dimensions, we can get from metadata. 
    key = self.__set_key(arg_list, n_args)
    if key:
      url += f"/{key}"

    # params define additional parameters
    if params:
      params = "&".join([f"{key}={val}" 
                         for key, val in params.items()])
      url += f"?{params}"

    return url

  def make_url_meta(self,
                    flow_ref: list[str] | str,
                    dataflow: str = "dataflow") -> str:
    # Makes the url for data retrieval

    if isinstance(flow_ref, list):
      flow_ref = self.__create_flowref(delimeter=",", *flow_ref)

    url = f"https://{self.source}/{self.resource}/{dataflow}/{flow_ref}?references=all"

    return url


  def __parse_error(self, error_html: str) -> str:
    return error_html.decode()

  def __create_flowref(self, agency: str,
                       dataflow: str,
                       dataflow_version: str,
                       delimeter=",") -> str:
    # Generate a flowRef part to of SDMX API. 

    return f"{delimeter}".join([agency, dataflow, dataflow_version])

  def __set_key(self,
                arg_list: list[str] | None,
                n_args: int | None):
    key = None
    if arg_list:
      key = '.'.join(arg_list)
    elif n_args:
      key = '.' * (n_args - 1)

    return key
    

  def get_data(self,
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


  def get_metadata(self,
          flow_ref: list[str] | str,
          dataflow: str = "dataflow"):
    # Gets metadata which is a pair of dimension list and code list dictionary

    # Make the request
    url = self.make_url_meta(flow_ref, dataflow)
    res = requests.get(url)

    # Deal with errors
    if res.status_code != 200 and res.content:
      error_output = self.__parse_error(res.content)
      raise CollectorException(f"The collector can't access metadata:\n{error_output}")

    elif res.status_code != 200:
      raise CollectorException(f"Unknown error. Error code: {res.status_code()}")

    # Parse the request
    root = etree.fromstring(str(res.text)[40:]) # lxml needs to ignore the <?xml ...> tag

    structure_tag = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}"
    common_tag = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}"
    # Get dimensions
    dimensions = [el.get('id') for el in root.iter(f'{structure_tag}Dimension')
                               if el.get('id') is not None]

    # Get code lists
    code_lists = {}
    for codelist in root.iter(f'{structure_tag}Codelist'):
      codes = [e.get('id') 
               for e in codelist.iter(f'{structure_tag}Code')]

      names = [e.text 
               for e in codelist.iter(f'{common_tag}Name')]

      code_lists[codelist] = {code : name for code, name 
                                           in zip(codes, names)}

    return dimensions, code_lists

  @staticmethod
  def sample_to_pandas(sample, 
                       parse_dates: list[str] = None):

    df = pd.read_csv(StringIO(sample),
              parse_dates=parse_dates,
              engine="pyarrow")

    return df

  @staticmethod
  def factorize(df: pd.DataFrame):
    obj_cols = df.keys()[df.dtypes == "object"]
    factor_array = []

    for col in obj_cols:
      # Factorizes object columns
      indices, factors = pd.factorize(df[col])
      df.loc[:, col] = indices

      # Adds to a list of dimension tables
      # In this stage they are Python tuples
      # later they will be turned into pandas Dataframes and exported to Postgres
      factor_array.append( (col, factors) )

      return df, factor_array
