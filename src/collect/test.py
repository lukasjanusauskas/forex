import unittest
from collect import SDMXCollector 


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

  def test_get(self):
    collector = SDMXCollector("data-api.ecb.europa.eu", "service")

    output = collector.get("EXR", ["D", "USD", "EUR", "SP00", "A"],
                           {"startPeriod": "2023-12-31", "format": "csvdata"})
    self.assertGreater(len(output), 0)


if __name__ == "__main__":
  unittest.main()