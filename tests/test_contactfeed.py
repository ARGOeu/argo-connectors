import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_contacts import ParseSiteContacts, ParseSitesWithContacts, \
    ParseRocContacts, ParseServiceEndpointContacts, \
    ParseServiceGroupRoles, ParseServiceGroupWithContacts, ConnectorParseError
from argo_egi_connectors.parse.gocdb_topology import ParseServiceEndpoints
from argo_egi_connectors.parse.flat_topology import ParseContacts


logger = Logger('test_contactfeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


class ParseRocContactTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-roc_contacts.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        parse_roc_contacts = ParseRocContacts(logger, self.content)
        self.roc_contacts = parse_roc_contacts.get_contacts()

    def test_formatContacts(self):
        self.assertEqual(self.roc_contacts[0],
            {
                'name': 'CERN',
                'contacts': [
                    {
                        'certdn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=Name1/CN=11111/CN=Name1 Surname1',
                        'email': 'Name1.Surname1@example.com',
                        'forename': 'Name1',
                        'role': 'NGI Operations Manager',
                        'surname': 'Surname1'
                    },
                    {
                        'certdn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=Name2/CN=111111/CN=Name2 Surname2',
                        'email': 'Name2.Surname2@example.com',
                        'forename': 'Name2',
                        'role': 'NGI Security Officer',
                        'surname': 'Surname2'
                    },
                    {
                        'certdn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=Name3/CN=222222/CN=Name3 Surname3',
                        'email': 'Name3.Surname3@example.com',
                        'forename': 'Name3',
                        'role': 'NGI Operations Deputy Manager',
                        'surname': 'Surname3'
                    }
                ]
            }
        )


class ParseSitesContactTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-site_contacts.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        parse_sites_contacts = ParseSiteContacts(logger, self.content)
        self.site_contacts = parse_sites_contacts.get_contacts()

    def test_lenContacts(self):
        self.assertEqual(len(self.site_contacts), 2)
        site_1 = len(self.site_contacts[0]['contacts'])
        site_2 = len(self.site_contacts[1]['contacts'])
        self.assertEqual(10, site_1 + site_2)

    def test_malformedContacts(self):
        self.assertRaises(ConnectorParseError, ParseSiteContacts, logger, 'wrong mocked data')

    def test_formatContacts(self):
        self.assertEqual(self.site_contacts[0],
            {
                'name': 'Site1',
                'contacts': [
                    {
                        'certdn': '/C=HR/O=CROGRID/O=SRCE/CN=Name1 Surname1',
                        'email': 'Name1.Surname1@email.hr',
                        'forename': 'Name1',
                        'role': 'Site Security Officer',
                        'surname': 'Surname1'
                    },
                    {
                        'certdn': '/C=HR/O=CROGRID/O=SRCE/CN=Name1 Surname1',
                        'email': 'Name1.Surname1@email.hr',
                        'forename': 'Name1',
                        'role': 'Site Operations Manager',
                        'surname': 'Surname1'
                    },
                    {
                        'certdn': '/C=HR/O=CROGRID/O=SRCE/CN=Name2 Surname2',
                        'email': 'Name2.Surname2@email.hr',
                        'forename': 'Name2',
                        'role': 'Site Operations Manager',
                        'surname': 'Surname2'
                    }
                ],
            }
        )
        # contact without surname
        self.assertEqual(
            self.site_contacts[1]['contacts'][6],
                {
                    'certdn': '/C=HR/O=CROGRID/O=SRCE/CN=Name3 Surname3',
                    'email': 'Name3.Surname3@email.hr',
                    'forename': 'Name3',
                    'role': 'Site Administrator',
                    'surname': ''
                }
        )


class ParseSitesWithContactTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-sites_with_contacts.xml') as feed_file:
            self.content = feed_file.read()
        self.maxDiff = None
        logger.customer = CUSTOMER_NAME
        parse_sites_contacts = ParseSitesWithContacts(logger, self.content)
        self.site_contacts = parse_sites_contacts.get_contacts()

    def test_formatContacts(self):
        self.assertEqual(self.site_contacts[0],
            {
                'name': 'INFN',
                'contacts': ['name1.surname1@ba.infn.it']
            }
        )


class ParseServiceEndpointsWithContactsTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_endpoint_with_contacts.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME

        serviceendpoint_contacts = ParseServiceEndpointContacts(logger, self.content)
        self.serviceendpoint_contacts = serviceendpoint_contacts.get_contacts()

        with open('tests/sample-service_endpoint.xml') as feed_file:
            self.content = feed_file.read()
        serviceendpoint_nocontacts = ParseServiceEndpointContacts(logger, self.content)
        self.serviceendpoint_nocontacts = serviceendpoint_nocontacts.get_contacts()

    def test_formatContacts(self):
        self.assertEqual(self.serviceendpoint_contacts[0],
            {
                'contacts': ['contact@email.com'],
                'name': 'some.fqdn.com+service.type'
            }
        )
        self.assertEqual(self.serviceendpoint_contacts[2],
            {
                'contacts': ['contact1@email.com', 'contact2@email.com',
                             'contact3@email.com'],
                'name': 'some.fqdn2.com+service.type2'
            }
        )

    def test_formatNoContacts(self):
        self.assertEqual(self.serviceendpoint_nocontacts, [])


class ParseServiceGroupRolesTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_group_contacts.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME

        servicegroup_contacts = ParseServiceGroupRoles(logger, self.content)
        self.servicegroup_contacts = servicegroup_contacts.get_contacts()

    def test_formatContacts(self):
        self.assertEqual(self.servicegroup_contacts[0],
            {
                'contacts': ['grid-admin@example.com'],
                'name': 'GROUP1'
            }
        )


class ParseServiceGroupWithContactsTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_group_with_contacts.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME

        servicegroup_contacts = ParseServiceGroupWithContacts(logger, self.content)
        self.servicegroup_contacts = servicegroup_contacts.get_contacts()

    def test_formatContacts(self):
        self.assertEqual(self.servicegroup_contacts[0],
            {
                'contacts': ['name1.surname1@email.com'],
                'name': 'B2FIND-Askeladden'
            }
        )


class ParseCsvServiceEndpointsWithContacts(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-topo.csv') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME

        self.contacts = ParseContacts(logger, self.content, uidservtype=True, is_csv=True).get_contacts()

    def test_FormatContacts(self):
        self.assertEqual(self.contacts,
            [
                {
                    'contacts': ['name.surname@country.com'],
                    'name': 'files.dev.tenant.eu_tenant_1+nextcloud'
                },
                {
                    'contacts': ['name.surname@country.com'],
                    'name': 'files.tenant.eu_tenant_2+nextcloud'
                },
                {
                    'contacts': ['name.surname@country.com'],
                    'name': 'sso.tenant.eu_tenant_3+aai'
                }
            ]
        )


if __name__ == '__main__':
    unittest.main()
