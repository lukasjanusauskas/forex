import unittest
from collect import SDMXCollector 


class CollectorTest(unittest.TestCase):

  def test_url(self):
    collector = SDMXCollector("data-api.ecb.europa.eu", 
                              "service",
                              {"startPeriod": "2009-12-31"})

    result = collector.make_url("EXR", ["D", "USD", "EUR", "SP00", "A"])
    output = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?startPeriod=2009-12-31"

    self.assertEqual(result, output)

    collector.params = {"startPeriod": "2009-12-31", "format": "csvdata"}

    result = collector.make_url("EXR", ["D", "USD", "EUR", "SP00", "A"])
    output = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?startPeriod=2009-12-31&format=csvdata"

  def test_get(self):
    collector = SDMXCollector("data-api.ecb.europa.eu", 
                              "service",
                              {"startPeriod": "2023-12-31", "format": "csvdata"})

    output = collector.get("EXR", ["D", "USD", "EUR", "SP00", "A"])
    self.assertGreater(len(output), 0)


if __name__ == "__main__":
  unittest.main()