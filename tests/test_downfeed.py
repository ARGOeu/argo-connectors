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
        flat_downtimes = ParseDowntimes(self.logger, self.downtimes, date_2_21_2022, True)
        downtimes = flat_downtimes.get_data()
        self.assertEqual(len(downtimes), 16)
        first_schedule = downtimes[0]
        self.assertEqual(first_schedule['hostname'], 'atmo-flud.neanias.eu_neanias_5')
        start_time = datetime.datetime.strptime(first_schedule['start_time'], '%Y-%m-%dT%H:%M:00Z')
        end_time = datetime.datetime.strptime(first_schedule['end_time'], '%Y-%m-%dT%H:%M:00Z')
        self.assertEqual(start_time, datetime.datetime(2022, 2, 21, 8, 0))
        self.assertEqual(end_time, datetime.datetime(2022, 2, 21, 23, 59))

        date_2_22_2022 = datetime.datetime(2022, 2, 22)
        flat_downtimes = ParseDowntimes(self.logger, self.downtimes, date_2_22_2022, True)
        downtimes = flat_downtimes.get_data()
        self.assertEqual(len(downtimes), 16)
        first_schedule = downtimes[0]
        self.assertEqual(first_schedule['hostname'], 'atmo-flud.neanias.eu_neanias_5')
        start_time = datetime.datetime.strptime(first_schedule['start_time'], '%Y-%m-%dT%H:%M:00Z')
        end_time = datetime.datetime.strptime(first_schedule['end_time'], '%Y-%m-%dT%H:%M:00Z')
        self.assertEqual(start_time, datetime.datetime(2022, 2, 22, 0, 0))
        self.assertEqual(end_time, datetime.datetime(2022, 2, 22, 19, 0))

        date_3_1_2022 = datetime.datetime(2022, 3, 1)
        flat_downtimes = ParseDowntimes(self.logger, self.downtimes, date_3_1_2022, True)
        downtimes = flat_downtimes.get_data()
        self.assertEqual(len(downtimes), 16)
        first_schedule = downtimes[0]
        self.assertEqual(first_schedule['hostname'], 'atmo-flud.neanias.eu_neanias_5')
        start_time = datetime.datetime.strptime(first_schedule['start_time'], '%Y-%m-%dT%H:%M:00Z')
        end_time = datetime.datetime.strptime(first_schedule['end_time'], '%Y-%m-%dT%H:%M:00Z')
        self.assertEqual(start_time, datetime.datetime(2022, 3, 1, 8, 0))
        self.assertEqual(end_time, datetime.datetime(2022, 3, 1, 23, 59))

        date_3_2_2022 = datetime.datetime(2022, 3, 2)
        flat_downtimes = ParseDowntimes(self.logger, self.downtimes, date_3_2_2022, True)
        downtimes = flat_downtimes.get_data()
        self.assertEqual(len(downtimes), 16)
        first_schedule = downtimes[0]
        self.assertEqual(first_schedule['hostname'], 'atmo-flud.neanias.eu_neanias_5')
        start_time = datetime.datetime.strptime(first_schedule['start_time'], '%Y-%m-%dT%H:%M:00Z')
        end_time = datetime.datetime.strptime(first_schedule['end_time'], '%Y-%m-%dT%H:%M:00Z')
        self.assertEqual(start_time, datetime.datetime(2022, 3, 2, 0, 0))
        self.assertEqual(end_time, datetime.datetime(2022, 3, 2, 23, 59))

        date_3_4_2022 = datetime.datetime(2022, 3, 4)
        flat_downtimes = ParseDowntimes(self.logger, self.downtimes, date_3_4_2022, True)
        downtimes = flat_downtimes.get_data()
        self.assertEqual(len(downtimes), 16)
        first_schedule = downtimes[0]
        self.assertEqual(first_schedule['hostname'], 'atmo-flud.neanias.eu_neanias_5')
        start_time = datetime.datetime.strptime(first_schedule['start_time'], '%Y-%m-%dT%H:%M:00Z')
        end_time = datetime.datetime.strptime(first_schedule['end_time'], '%Y-%m-%dT%H:%M:00Z')
        self.assertEqual(start_time, datetime.datetime(2022, 3, 4, 0, 0))
        self.assertEqual(end_time, datetime.datetime(2022, 3, 4, 19, 0))

    def test_failedParseDowntimes(self):
        date_2_21_2022 = datetime.datetime(2022, 2, 21)
        with self.assertRaises(ConnectorParseError) as cm:
            flat_downtimes = ParseDowntimes(self.logger, 'DUMMY DATA', date_2_21_2022, False)
            downtimes = flat_downtimes.get_data()

        excep = cm.exception
        self.assertTrue('CSV feed' in excep.msg)
        self.assertTrue(CUSTOMER_NAME in excep.msg)

if __name__ == '__main__':
    unittest.main()
