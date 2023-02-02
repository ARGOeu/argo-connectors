import unittest

from argo_connectors.log import Logger
from argo_connectors.parse.gocdb_contacts import ParseSiteContacts, ParseSitesWithContacts, \
    ParseRocContacts, ParseServiceEndpointContacts, \
    ParseServiceGroupRoles, ParseServiceGroupWithContacts, ConnectorParseError
from argo_connectors.parse.gocdb_topology import ParseServiceEndpoints
from argo_connectors.parse.provider_topology import ParseTopo
from argo_connectors.parse.flat_contacts import ParseContacts as ParseFlatContacts
from argo_connectors.parse.provider_contacts import ParseResourcesContacts, ParseProvidersContacts


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
        self.assertEqual(self.roc_contacts, {
            'CERN': [
                {
                    'certdn': '/DC=ch/DC=cern/OU=Organic '
                              'Units/OU=Users/CN=Name1/CN=11111/CN=Name1 Surname1',
                    'email': 'Name1.Surname1@example.com',
                    'forename': 'Name1',
                    'role': 'NGI Operations Manager',
                    'surname': 'Surname1'
                },
                {
                    'certdn': '/DC=ch/DC=cern/OU=Organic '
                              'Units/OU=Users/CN=Name2/CN=111111/CN=Name2 Surname2',
                    'email': 'Name2.Surname2@example.com',
                    'forename': 'Name2',
                    'role': 'NGI Security Officer',
                    'surname': 'Surname2'},
                {
                    'certdn': '/DC=ch/DC=cern/OU=Organic '
                              'Units/OU=Users/CN=Name3/CN=222222/CN=Name3 Surname3',
                    'email': 'Name3.Surname3@example.com',
                    'forename': 'Name3',
                    'role': 'NGI Operations Deputy Manager',
                    'surname': 'Surname3'
                }
            ],
            'EGI.eu': [
                {
                    'certdn': '/C=HR/O=edu/OU=srce/CN=Name1 Surname1',
                    'email': 'Name1.Surname1@email.com',
                    'forename': 'Name1',
                    'role': 'Regional Staff (ROD)',
                    'surname': 'Surname1'
                },
                {
                    'certdn': '1111111@egi.eu',
                    'email': 'Name2.Surname2@email.com',
                    'forename': 'Name2',
                    'role': 'Regional Staff (ROD)',
                    'surname': 'Surname2'
                }
            ]
        })


class ParseSitesContactTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-site_contacts.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        parse_sites_contacts = ParseSiteContacts(logger, self.content)
        self.site_contacts = parse_sites_contacts.get_contacts()

    def test_lenContacts(self):
        self.assertEqual(len(self.site_contacts.items()), 2)
        site_1 = len(self.site_contacts['Site1'])
        site_2 = len(self.site_contacts['Site2'])
        self.assertEqual(10, site_1 + site_2)

    def test_malformedContacts(self):
        self.assertRaises(ConnectorParseError, ParseSiteContacts, logger, 'wrong mocked data')

    def test_formatContacts(self):
        self.assertEqual(self.site_contacts['Site1'],
            [
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
            ]
        )
        # contact without surname
        self.assertEqual(
            self.site_contacts['Site2'][6],
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
        self.assertEqual(self.site_contacts,
            {
                'INFN': ['name1.surname1@ba.infn.it'],
                'DATACITE': ['name2.surname2@email.com']
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
        self.assertEqual(self.serviceendpoint_contacts,
            {
                'some.fqdn.com+service.type': ['contact@email.com'],
                'some.fqdn1.com+service.type1': ['contact1@email.com'],
                'some.fqdn2.com+service.type2': ['contact1@email.com', 'contact2@email.com', 'contact3@email.com']
            }

        )
    def test_formatNoContacts(self):
        self.assertEqual(self.serviceendpoint_nocontacts, {})


class ParseServiceGroupRolesTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_group_contacts.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME

        servicegroup_contacts = ParseServiceGroupRoles(logger, self.content)
        self.servicegroup_contacts = servicegroup_contacts.get_contacts()

    def test_formatContacts(self):
        self.assertEqual(self.servicegroup_contacts,
            {
                'GROUP1': ['grid-admin@example.com'],
                'GROUP2': ['grid-meteo@example1.com']
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
        self.assertEqual(self.servicegroup_contacts,
            {
                'B2FIND-Askeladden': ['name1.surname1@email.com'],
                'B2FIND-UHH': ['name2.surname2@email.com']
            }
        )


class ParseCsvServiceEndpointsWithContacts(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-topo.csv') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME

        self.contacts = ParseFlatContacts(logger, self.content, uidservendp=True, is_csv=True).get_contacts()

    def test_FormatContacts(self):
        self.assertEqual(self.contacts,
            {
                'files.dev.tenant.eu_tenant_1+nextcloud': ['name.surname@country.com'],
                'files.tenant.eu_tenant_2+nextcloud': ['name.surname@country.com'],
                'sso.tenant.eu_tenant_3+aai': ['name.surname@country.com'],
                'ia2-vialactea.oats.inaf.it_neanias_4+WebService': ['name.surname@garr.it']
            }
        )


class ParseEoscContacts(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-private-resource.json', encoding='utf-8') as feed_file:
            self.resources = feed_file.read()
        with open('tests/sample-private-provider.json', encoding='utf-8') as feed_file:
            self.providers = feed_file.read()
        logger.customer = CUSTOMER_NAME
        self.maxDiff = None

        self.resources_contacts = ParseResourcesContacts(logger,
                                                         self.resources).get_contacts()
        self.providers_contacts = ParseProvidersContacts(logger,
                                                         self.providers).get_contacts()

    def test_formatResourcesContacts(self):
        self.assertEqual(self.resources_contacts,
            {
                '3dbionotes.cnb.csic.es+srce.3dbionotes': ['kzailac@srce.hr'],
                'poem.devel.argo.grnet.gr+srce.poem': ['Emir.Imamagic@srce.hr'],
                'webodv-egi-ace.cloud.ba.infn.it+srce.webodv': ['Emir.Imamagic@srce.hr'],
                'www.openaire.eu+openaire.validator': ['info@openaire.eu'],
                'www.srce.unizg.hr+srce.srceweb': ['Emir.Imamagic@srce.hr']
            }
        )

    def test_formatProvidersContacts(self):
        self.assertEqual(self.providers_contacts,
            [
                {
                    'contacts': ['office@srce.hr'], 'name': 'SRCE'
                },
                {
                    'contacts': ['info@openaire.eu'], 'name': 'OpenAIRE'
                }
            ]
        )


if __name__ == '__main__':
    unittest.main()
