import unittest
from collect import SDMXCollector 
from metadata import MetaDataCollector


class CollectorTest(unittest.TestCase):

  def test_url(self):
    collector = SDMXCollector("data-api.ecb.europa.eu", "service")

    result = collector.make_url("EXR", 
                                arg_list=["D", "USD", "EUR", "SP00", "A"],
                                params={"startPeriod": "2009-12-31"})

    output = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?startPeriod=2009-12-31"

    self.assertEqual(result, output)

    result = collector.make_url("EXR", 
                                arg_list=["D", "USD", "EUR", "SP00", "A"],
                                params={"startPeriod": "2009-12-31", "format": "csvdata"})
    output = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?startPeriod=2009-12-31&format=csvdata"

    self.assertEqual(result, output)

    result = collector.make_url("EXR", n_args=7)
    output = "https://data-api.ecb.europa.eu/service/data/EXR/......"

    self.assertEqual(result, output)

  def test_metadata_url(self):
    namespace = {
      'structure': '{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}',
      'common': '{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}'
    }

    collector = MetaDataCollector('data-api.ecb.europa.eu', 'service')  

    result = collector.make_url(['ECB', 'ECB_EXR1', '1.0'], 'datastructure')
    output = 'https://data-api.ecb.europa.eu/service/datastructure/ECB/ECB_EXR1/1.0?references=all'

    self.assertEqual(result, output)

    self.assertEqual(namespace['structure'], collector.namespace['structure'])
    self.assertEqual(namespace['common'], collector.namespace['common'])


if __name__ == "__main__":
  unittest.main()