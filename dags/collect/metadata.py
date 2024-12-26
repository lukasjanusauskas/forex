# Define classes for metadata collection
import pandas as pd
from collect import CollectorException
import requests
from lxml import etree

class MetaData:
  def __init__(self, 
               dimensions: list[str],
               codelists: dict):
    
    self.dimensions = dimensions
    self.codelists = codelists

  def codelist_to_df() -> pd.DataFrame:
    pass
  

class MetaDataCollector:
  DEFAULT_NAMESPACE = {
    'structure': '{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}',
    'common': '{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}'
  }

  def __init__(self, source: str, resource: str,
               namespace: dict = DEFAULT_NAMESPACE):

    self.source = source
    self.resource = resource
    self.namespace = namespace
    
  def get_metadata(self,
                   flow_ref: list[str] | str,
                   dataflow: str,
                   handle_xml = lambda x: x) -> MetaData:
    # Retrieves metadata and returns a MetaData object

    url = self.make_url(flow_ref, dataflow)
    res = requests.get(url)
   
    self.__handle_status(res, url)
    
    xml_data = handle_xml(res.text)
    meta = etree.fromstring(xml_data)

    dimensions = self.__get_dimensions(meta)
    codelists = self.__get_codelist(meta)

    return MetaData(dimensions, codelists)

  def make_url(self,
               flow_ref: list[str] | str,
               dataflow: str = "dataflow") -> str:

    if isinstance(flow_ref, list):
      flow_ref = self.__create_flowref(delimeter="/", *flow_ref)

    url = f"https://{self.source}/{self.resource}/{dataflow}/{flow_ref}?references=all"

    return url

  def __create_flowref(self, agency: str,
                       dataflow: str,
                       dataflow_version: str,
                       delimeter=",") -> str:
    # Generate a flowRef part to of SDMX API. 

    return f"{delimeter}".join([agency, dataflow, dataflow_version])

  def __get_dimensions(self, meta: etree.Element) -> list[str]:
    # Get dimensions
    structure = self.namespace['structure']

    # Iterator over dimension tags
    dimension_iter = meta.iter(f'{structure}Dimension')

    # Get ids for each dimensions
    dimensions = [el.get('id') for el in dimension_iter
                               if el.get('id') is not None]

    return dimensions

  def __get_codelist(self, meta: etree.Element) -> dict:
    # Unpack namespaces
    structure, common = self.namespace['structure'], self.namespace['common']

    code_lists = {}
    for codelist in meta.iter(f'{structure}Codelist'):
          # Get codes and coresponding names
          codes = [e.get('id') for e in codelist.iter(f'{structure}Code')]
          names = [e.text for e in codelist.iter(f'{common}Name')]

          code_lists[codelist] = {code : name for code, name 
                                               in zip(codes, names)}

    return code_lists

  def __handle_status(self, res: requests.Response, url: str):
    status_code = res.status_code

    # Handle the response status code
    if status_code == 404:
      print(f"""There is something wrong with the URL 
                {url} or there were changes made to the API""")
    elif int(status_code / 10) == 5:
      raise CollectorException("Internal server error")
    else:
      raise CollectorException(f"""HTTP error code: {status_code}\n URL: {url}""")


class OECDMetaCollector(MetaDataCollector):
  def __get_codelist(self, meta: etree.Element) -> dict:
    # Unpack namespaces
    structure, common = self.namespace['structure'], self.namespace['common']

    code_lists = {}
    for codelist in meta.iter(f'{structure}Codelist'):
          # Get codes and coresponding names
          codes = [e.get('id') for e in codelist.iter(f'{structure}Code')]
          names = [e.text for e in codelist.iter(f'{common}Name')]

          code_lists[codelist] = {code : name for code, name 
                                               in zip(codes, names[::2])}

    return code_lists

class ECBMetaCollector(MetaDataCollector):
  def __get_codelist(self, meta: etree.Element) -> dict:
    # Unpack namespaces
    structure, common = self.namespace['structure'], self.namespace['common']

    code_lists = {}
    for codelist in meta.iter(f'{structure}Codelist'):
          # Get codes and coresponding names
          codes = [e.get('id') for e in codelist.iter(f'{structure}Code')]
          names = [e.text for e in codelist.iter(f'{common}Name')]

          code_lists[codelist] = {code : name for code, name 
                                               in zip(codes, names[1::])}

    return code_lists