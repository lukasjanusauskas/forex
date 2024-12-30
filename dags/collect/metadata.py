# Define classes for metadata collection
import pandas as pd
import requests
from lxml import etree

class CollectorException(Exception):
  pass


class MetaData:
  def __init__(self, 
               dimensions: list[str],
               codelists: dict):
    
    self.dimensions = dimensions
    self.codelists = codelists
  
  def export_codelists() -> dict[str, pd.DataFrame]:
    pass

class MetaDataCollector:
  DEFAULT_NAMESPACE = {
    'structure': '{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}',
    'common': '{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}',
    'xml': '{http://www.w3.org/XML/1998/namespace}'
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
   
    self._handle_status(res, url)
    
    xml_data = handle_xml(res.content)
    meta = etree.fromstring(xml_data)

    dimensions = self._get_dimensions(meta)
    codelists = self._get_codelist(meta)

    return MetaData(dimensions, codelists)

  def make_url(self,
               flow_ref: list[str] | str,
               dataflow: str = "dataflow") -> str:

    if isinstance(flow_ref, list):
      flow_ref = self._create_flowref(delimeter="/", *flow_ref)

    url = f"https://{self.source}/{self.resource}/{dataflow}/{flow_ref}?references=all"

    return url

  def _create_flowref(self, agency: str,
                       dataflow: str,
                       dataflow_version: str = None,
                       delimeter: str = "/") -> str:
    # Generate a flowRef part to of SDMX API. 

    if dataflow_version:
      return f"{delimeter}".join([agency, dataflow, dataflow_version]) 
    else:
      return f"{delimeter}".join([agency, dataflow]) 

  def _get_dimensions(self, meta: etree.Element) -> list[str]:
    # Get dimensions
    structure = self.namespace['structure']

    # Iterator over dimension tags
    dimension_iter = meta.iter(f'{structure}Dimension')

    # Get ids for each dimensions
    dimensions = [el.get('id') for el in dimension_iter
                               if el.get('id') is not None]

    return dimensions

  def _get_codelist(self, meta: etree.Element) -> dict:
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

  def _handle_status(self, res: requests.Response, url: str):
    status_code = res.status_code

    # If the status code is 200: all good
    if status_code == 200:
       return

    # Handle the response status code
    if status_code == 404:
      print(f"""There is something wrong with the URL 
                {url} or there were changes made to the API""")
    elif int(status_code / 10) == 5:
      raise CollectorException(f"Internal server error {status_code}")
    else:
      raise CollectorException(f"""HTTP error code: {status_code}\n URL: {url}""")


class OECDMetaCollector(MetaDataCollector):
  def get_metadata(self,
                   flow_ref: list[str] | str,
                   dataflow: str,
                   handle_xml = lambda x: x) -> MetaData:
    # Retrieves metadata and returns a MetaData object

    url = self.make_url(flow_ref, dataflow)
    res = requests.get(url)
   
    self._handle_status(res, url)
    
    xml_data = handle_xml(res.content)
    try:
      meta = etree.fromstring(xml_data)
    except etree.XMLSyntaxError:
      print(xml_data)

    dimensions = self._get_dimensions(meta)
    codelists = self._get_codelist(meta)

    return MetaData(dimensions, codelists)

  def _get_codelist(self, meta: etree.Element) -> dict:
    # Unpack namespaces
    structure, common, xml_nspace = (self.namespace['structure'],
                                     self.namespace['common'],
                                     self.namespace['xml'])

    code_lists = {}
    for codelist in meta.iter(f'{structure}Codelist'):
          codes = [e.get('id') for e in codelist.iter(f'{structure}Code')]
          names = [e.text for e in codelist.iter(f'{common}Name')
                      if e.attrib[f'{xml_nspace}lang'] == "en"]

          code_lists[codelist.get('id').lower()] = {
                'code': codes,
                'value': names[1:]
          }

    return code_lists

class ECBMetaCollector(MetaDataCollector):
  def get_metadata(self,
                   flow_ref: list[str] | str,
                   dataflow: str,
                   handle_xml = lambda x: x) -> MetaData:
    # Retrieves metadata and returns a MetaData object

    url = self.make_url(flow_ref, dataflow)
    res = requests.get(url)
   
    self._handle_status(res, url)
    
    xml_data = handle_xml(res.content)
    meta = etree.fromstring(xml_data)

    dimensions = self._get_dimensions(meta)
    codelists = self._get_codelist(meta)

    return MetaData(dimensions, codelists)

  def _get_codelist(self, meta: etree.Element) -> dict:
    # Unpack namespaces
    structure, common = self.namespace['structure'], self.namespace['common']

    code_lists = {}
    for codelist in meta.iter(f'{structure}Codelist'):
          # Get codes and coresponding names
          codes = [e.get('id') for e in codelist.iter(f'{structure}Code')]
          names = [e.text for e in codelist.iter(f'{common}Name')]

          code_lists[codelist.get('id').lower()] = {'code': codes, 'name': names[1::]}

    return code_lists

if __name__ == "__main__":
  """https://sdmx.oecd.org/public/rest/dataflow/OECD.SDD.TPS/DSD_BOP@DF_BOP/?references=all"""
  collector = OECDMetaCollector('sdmx.oecd.org', 'public/rest')  
  metadata = collector.get_metadata(['OECD.SDD.TPS', 'DSD_BOP@DF_BOP/', ''],
                                     'dataflow')

  print(metadata.dimensions)
  print(metadata.codelists)