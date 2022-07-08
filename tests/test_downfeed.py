import datetime
import mock
import unittest

from argo_connectors.log import Logger
from argo_connectors.parse.flat_downtimes import ParseDowntimes
from argo_connectors.exceptions import ConnectorParseError

CUSTOMER_NAME = 'CUSTOMERFOO'


class ParseCsvDowntimes(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-downtimes.csv', encoding='utf-8') as feed_file:
            self.downtimes = feed_file.read()
        self.maxDiff = None
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.logger = logger

    def test_parseDowntimes(self):
        date_2_21_2022 = datetime.datetime(2022, 2, 21)
        self.flat_downtimes = ParseDowntimes(self.logger, self.downtimes, date_2_21_2022, False)
        downtimes = self.flat_downtimes.get_data()

    def test_failedParseDowntimes(self):
        date_2_21_2022 = datetime.datetime(2022, 2, 21)
        with self.assertRaises(ConnectorParseError) as cm:
            self.flat_downtimes = ParseDowntimes(self.logger, 'DUMMY DATA', date_2_21_2022, False)
            downtimes = self.flat_downtimes.get_data()

        excep = cm.exception
        self.assertTrue('CSV feed' in excep.msg)
        self.assertTrue(CUSTOMER_NAME in excep.msg)

if __name__ == '__main__':
    unittest.main()
