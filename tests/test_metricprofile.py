import unittest
from unittest import mock

from argo_connectors.parse.webapi_metricprofile import ParseMetricProfiles
from argo_connectors.exceptions import ConnectorParseError


class ParseMetricProfile(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-metricprofile.json', encoding='utf-8') as feed_file:
            self.metricprofile_data = feed_file.read()
        logger = mock.Mock()
        logger.customer = 'mock_customer'
        logger.job = 'mock_job'
        self.logger = logger
        self.target_profiles = ['MOCK_DATA']
        
    def test_parseMetricProfile(self):
        flat_metricprofile = ParseMetricProfiles(
            self.logger, self.metricprofile_data, self.target_profiles
        )
        metricprofile = flat_metricprofile.get_data()
        expected = [{'profile': 'MOCK_DATA', 'metric': 'foo.access.login-certificate', 'service': 'foo.access.unity'}, 
                    {'profile': 'MOCK_DATA', 'metric': 'foo.access.login-local', 'service': 'foo.access.unity'}, 
                    {'profile': 'MOCK_DATA', 'metric': 'generic.tcp.connect', 'service': 'foo.access.unity'}, 
                    {'profile': 'MOCK_DATA', 'metric': 'mock.safe.irod-crud', 'service': 'safe.irods'}]
        self.assertEqual(metricprofile, expected)
        self.assertEqual(len(metricprofile), 4)


    def test_fail_parseMetricProfile(self):
        with self.assertRaises(ConnectorParseError) as cm:
            flat_metricprofile = ParseMetricProfiles(
                self.logger, 'foo_data', self.target_profiles
            )
            metricprofile = flat_metricprofile.get_data()
            
        excep = cm.exception
    
        self.assertTrue('JSONDecodeError' in excep.msg)
        self.assertTrue('mock_customer' in excep.msg)
        
        
    def test_no_target_profiles(self):
        with self.assertRaises(SystemExit) as cm:        
            flat_metricprofile = ParseMetricProfiles(
                self.logger, self.metricprofile_data, []
            )
            metricprofile = flat_metricprofile.get_data()
        
        self.assertEqual(cm.exception.code, 1)
        expected_message = 'Customer:mock_customer Job:mock_job: No profiles  were found!'
        self.logger.error.assert_called_once_with(expected_message)
