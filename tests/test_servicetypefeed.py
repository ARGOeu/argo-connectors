import unittest

import mock

from argo_connectors.log import Logger
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.webapi_servicetypes import ParseWebApiServiceTypes
from argo_connectors.parse.gocdb_servicetypes import ParseGocdbServiceTypes
from argo_connectors.parse.flat_servicetypes import ParseFlatServiceTypes
from argo_connectors.parse.base import ParseHelpers

CUSTOMER_NAME = 'CUSTOMERFOO'


class ParseWebApi(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_types_webapi.json', encoding='utf-8') as feed_file:
            service_types = feed_file.read()
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.logger = logger
        self.maxDiff = None
        self.services_webapi = ParseWebApiServiceTypes(logger, service_types)

    def test_WebApiFeedParse(self):
        service_types = self.services_webapi.get_data()
        self.assertEqual(service_types,
            [
                {
                    'description': '[Central service] gLite metadata catalogue. This service '
                    'only needs to be installed on the request of a VO. ',
                    'name': 'AMGA',
                    'tags': ['topology']
                },
                {
                    'description': '[Site service] This is a "dummy" Service Type to enable the '
                    'monitoring tests for APEL accounting. All EGEE sites must '
                    'have one instance of this Service Type, associated with a '
                    'CE. ',
                    'name': 'APEL',
                    'tags': ['topology']
                },
                {
                    'description': '[Site service] The Compute Element within the ARC middleware '
                    'stack. ',
                    'name': 'ARC-CE',
                    'tags': ['topology']
                },
                {
                    'description': '[Site service] The LCG Compute Element. Currently the '
                    'standard CE within the gLite middleware stack. Soon to be '
                    'replaced by the CREAM CE. ',
                    'name': 'CE',
                    'tags': ['topology']
                },
                {
                    'description': '[Site service] The CREAM Compute Element is the new CE '
                    'within the gLite middleware stack. ',
                    'name': 'CREAM-CE',
                    'tags': ['topology']
                },
                {
                    'description': 'FroNTier N-Tier data distribution system. '
                    'http://frontier.cern.ch/',
                    'name': 'CUSTOM.ch.cern.frontier.FroNTier',
                    'tags': ['topology']
                },
                {
                    'description': 'Service for collecting accounting data from NGI_PL grid ',
                    'name': 'CUSTOM.pl.plgrid.BAT.agent',
                    'tags': ['topology']
                },
                {
                    'description': 'Bazaar Site Admin Toolkit from NGI_PL grid',
                    'name': 'CUSTOM.pl.plgrid.BazaarSAT',
                    'tags': ['topology']
                },
                {
                    'description': 'Generic request tracker',
                    'name': 'CUSTOM.RequestTracker',
                    'tags': ['topology']
                },
                {
                    'description': 'Generic user portal ',
                    'name': 'CUSTOM.UserPortal',
                    'tags': ['topology']
                },
                {
                    'description': 'Service type created from POEM',
                    'name': 'Service.Type.One',
                    'tags': ['poem']
                },
                {
                    'description': 'Service type created from POEM earlier without tag',
                    'name': 'Service.Type.Three',
                    'tags': ['poem']
                },
                {
                    'description': 'Service type created from POEM',
                    'name': 'Service.Type.Two',
                    'tags': ['poem']
                }
            ]
        )

    def test_WebApiTagPoem(self):
        service_types_poem = self.services_webapi.get_data(tag='poem')
        self.assertEqual(service_types_poem,
            [
                {
                    'description': 'Service type created from POEM',
                    'name': 'Service.Type.One',
                    'tags': ['poem']
                },
                {
                    'description': 'Service type created from POEM earlier without tag',
                    'name': 'Service.Type.Three',
                    'tags': ['poem']
                },
                {
                    'description': 'Service type created from POEM',
                    'name': 'Service.Type.Two',
                    'tags': ['poem']
                }
            ]
        )

    def test_FailedWebApiFeedParse(self):
        with self.assertRaises(ConnectorParseError) as cm:
            fail_services_webapi = ParseWebApiServiceTypes(
                self.logger, 'FAILED_DATA')

        excep = cm.exception
        self.assertTrue('JSON feed' in excep.msg)
        self.assertTrue('JSONDecodeError' in excep.msg)


class ParseGocdb(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_types_gocdb.xml', encoding='utf-8') as feed_file:
            service_types = feed_file.read()
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.services_gocdb = ParseGocdbServiceTypes(logger, service_types)
        self.maxDiff = None
        self.fail_services_gocdb = ParseGocdbServiceTypes(
            logger, 'FAILED_DATA')

    def test_GocdbFeedParse(self):
        service_types = self.services_gocdb.get_data()
        self.assertEqual(service_types, 
            [
                {
                    'description': 'Horizon is the canonical implementation of OpenStack’s '
                    'Dashboard, which provides a web based user interface to '
                    'OpenStack services',
                    'name': 'org.openstack.horizon',
                    'tags': ['topology']
                },
                {
                    'description': 'The primary endpoint for an OpenStack Cloud. Provides '
                    'identity and an endpoint catalog for other OpenStack '
                    'services',
                    'name': 'org.openstack.keystone',
                    'tags': ['topology']
                },
                {
                    'description': 'OpenStack Nova provides VM management services',
                    'name': 'org.openstack.nova',
                    'tags': ['topology']
                },
                {
                    'description': '',
                    'name': 'service.type.empty.desc',
                    'tags': ['topology']
                },
                {
                    'description': '',
                    'name': 'SERVICE.TYPE.UPPERCASE',
                    'tags': ['topology']
                }
            ]
        )

    def test_FailedGocdbFeedParse(self):
        with self.assertRaises(ConnectorParseError) as cm:
            failed_service_types = self.fail_services_gocdb.get_data()

        excep = cm.exception
        self.assertTrue('Error parsing service types feed' in excep.msg)
        self.assertTrue('XMLSyntaxError' in excep.msg)


class ParseFlat(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-topo.csv', encoding='utf-8') as feed_file:
            service_types = feed_file.read()
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.logger = logger
        self.service_types = service_types
        self.services_flat = ParseFlatServiceTypes(
            logger, service_types, is_csv=True)
        self.maxDiff = None

    def test_FlatFeedParse(self):
        service_types = self.services_flat.get_data()
        self.assertEqual(service_types, 
            [
                {
                    'description': 'tenant project Data sharing service',
                    'name': 'nextcloud',
                    'tags': ['topology']
                },
                {
                    'description': 'tenant project SSO service',
                    'name': 'aai',
                    'tags': ['topology']
                },
                {
                    'description': 'NEANIAS project SPACE-VIS ViaLactea service',
                    'name': 'WebService',
                    'tags': ['topology']
                },
            ]
        )

    @mock.patch.object(ParseHelpers, 'csv_to_json')
    def test_FailedFeedParse(self, mocked_csv2json):
        mocked_csv2json.return_value = ['FAILED_DATA1', 'FAILED_DATA2']
        services_flat = ParseFlatServiceTypes(
            self.logger, self.service_types, is_csv=True)
        with self.assertRaises(ConnectorParseError) as cm:
            data = services_flat.get_data()
        excep = cm.exception
        self.assertTrue('CSV feed' in excep.msg)
        self.assertTrue(CUSTOMER_NAME in excep.msg)
