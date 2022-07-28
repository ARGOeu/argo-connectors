import unittest

import mock

from argo_connectors.log import Logger
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.gocdb_servicetypes import ParseGocdbServiceTypes
from argo_connectors.parse.flat_servicetypes import ParseFlatServiceTypes
from argo_connectors.parse.base import ParseHelpers

CUSTOMER_NAME = 'CUSTOMERFOO'


class ParseGocdb(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_types_gocdb.xml', encoding='utf-8') as feed_file:
            service_types = feed_file.read()
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.services_gocdb = ParseGocdbServiceTypes(logger, service_types)
        self.maxDiff = None

    def test_GocdbFeedParse(self):
        service_types = self.services_gocdb.get_data()
        self.assertEqual(service_types, [
            {
                'description': 'Horizon is the canonical implementation of OpenStack’s '
                    'Dashboard, which provides a web based user interface to '
                    'OpenStack services',
                'name': 'org.openstack.horizon'
            },
            {
                'description': 'The primary endpoint for an OpenStack Cloud. Provides '
                    'identity and an endpoint catalog for other OpenStack '
                    'services',
                'name': 'org.openstack.keystone'
            },
            {
                'description': 'OpenStack Nova provides VM management services',
                'name': 'org.openstack.nova'
            },
            {
                'description': '',
                'name': 'service.type.empty.desc'
            }
        ])


class ParseFlat(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-topo.csv', encoding='utf-8') as feed_file:
            service_types = feed_file.read()
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.logger = logger
        self.service_types = service_types
        self.services_flat = ParseFlatServiceTypes(logger, service_types, is_csv=True)
        self.maxDiff = None

    def test_FlatFeedParse(self):
        service_types = self.services_flat.get_data()
        self.assertEqual(service_types, [
            {
                'description': 'tenant project Data sharing service',
                'name': 'nextcloud'
            },
            {
                'description': 'tenant project SSO service',
                'name': 'aai'
            },
            {
                'description': 'NEANIAS project SPACE-VIS ViaLactea service',
                'name': 'WebService'
            },
        ])

    @mock.patch.object(ParseHelpers, 'csv_to_json')
    def test_FailedFeedParse(self, mocked_csv2json):
        mocked_csv2json.return_value = ['FAILED_DATA1', 'FAILED_DATA2']
        services_flat = ParseFlatServiceTypes(self.logger, self.service_types, is_csv=True)
        with self.assertRaises(ConnectorParseError) as cm:
            data = services_flat.get_data()
        excep = cm.exception
        self.assertTrue('CSV feed' in excep.msg)
        self.assertTrue(CUSTOMER_NAME in excep.msg)
