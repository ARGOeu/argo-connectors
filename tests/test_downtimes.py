import datetime
import unittest

from argo_connectors.config.customer import Customer
from argo_connectors.config.glob import Global
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.log import Logger
from argo_connectors.parse.gocdb_downtimes import ParseDowntimes


CUSTOMER_NAME = 'CUSTOMERFOO'


class ParseGocdbDowntimes(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-gocdb_downtimes.xml', encoding='utf-8') as feed_file:
            self.downtimes = feed_file.read()
        self.maxDiff = None
        _ = Global('downtimes-gocdb-connector.py')
        cust = Customer('downtimes-gocdb-connector.py')
        cust.custopts['TopoUIDServiceEndpoints'] = True
        logger = Logger(f'{__name__}.{__class__.__name__}')
        logger.customer = CUSTOMER_NAME
        self.logger = logger
        self.start = datetime.datetime(2023, 2, 21)
        self.end = datetime.datetime(2023, 2, 23)

    def test_parseGocdbDowntimes(self):
        flat_downtimes = ParseDowntimes(self.downtimes, self.start, self.end)
        downtimes = flat_downtimes.get_data()
        expected = [{'hostname': 'mock2.foo2_567890G0', 'service': 'foo2-BDII', 'start_time': '2023-02-21T00:00:00Z', 'end_time': '2023-02-23T00:00:00Z'},
                    {'hostname': 'mock3.foo3_11111G0', 'service': 'foo3-AUU', 'start_time': '2023-02-21T00:00:00Z', 'end_time': '2023-02-23T00:00:00Z'}]
        self.assertEqual(downtimes, expected)
        self.assertEqual(len(downtimes), 2)
        first_schedule = downtimes[0]
        self.assertEqual(first_schedule['hostname'], 'mock2.foo2_567890G0')
        self.assertEqual(self.start, datetime.datetime(2023, 2, 21))
        self.assertEqual(self.end, datetime.datetime(2023, 2, 23))

    def test_fail_parseGocdbDowntimes(self):
        with self.assertRaises(ConnectorParseError) as cm:
            flat_downtimes = ParseDowntimes('Foo_data', self.start, self.end, False)
            downtimes = flat_downtimes.get_data()

        excep = cm.exception
        self.assertTrue('XMLSyntaxError' in excep.msg)


if __name__ == '__main__':
    unittest.main()
