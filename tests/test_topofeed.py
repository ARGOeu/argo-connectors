import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites
from argo_egi_connectors.parse.flat_topology import ParseFlatEndpoints
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.mesh.contacts import attach_contacts_topodata

logger = Logger('test_topofeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'

# Help function - check if any of endpoints contains extensions
# Used for checking if pass_extensions is working properly
def endpoints_have_extension(group_endpoints):
    for gren in group_endpoints:
        for key in gren['tags'].keys():
            if key.startswith('info_ext_'):
                return True
    return False

# Returns element of group_endpoints with given group_name or None
def get_group(group_endpoints, group_name):
    for group in group_endpoints:
        if group['group'] == group_name:
            return group

    return None


class ParseServiceEndpointsTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_endpoint.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        parse_service_endpoints = ParseServiceEndpoints(logger, self.content, CUSTOMER_NAME)
        self.group_endpoints = parse_service_endpoints.get_group_endpoints()

        parse_service_endpoints_ext = ParseServiceEndpoints(logger, self.content, 'CUSTOMERFOO', uid=True, pass_extensions=True)
        self.group_endpoints_ext = parse_service_endpoints_ext.get_group_endpoints()

    def test_LenEndpoints(self):
        self.assertEqual(len(self.group_endpoints), 3) # Parsed correct number of endpoint groups

    def test_DataEndpoints(self):
        self.assertEqual(self.group_endpoints[0],
            {
                'group': 'AZ-IFAN',
                'hostname': 'ce.physics.science.az',
                'service': 'CREAM-CE',
                'tags': {'info_HOSTDN': '/DC=ORG/DC=SEE-GRID/O=Hosts/O=Institute of Physics of ANAS/CN=ce.physics.science.az',
                'info_ID': '1555G0',
                'info_URL': 'ce.physics.science.az:8443/cream-pbs-ops',
                'info_service_endpoint_URL': 'ce.physics.science.az:8443/cream-pbs-ops',
                'monitored': '1',
                'production': '1',
                'scope': 'EGI, wlcg, atlas'},
                'type': 'SITES'
            },
            {
                'group': 'RAL-LCG2',
                'hostname': 'arc-ce01.gridpp.rl.ac.uk',
                'service': 'gLite-APEL',
                'tags': {'info_HOSTDN': '/C=UK/O=eScience/OU=CLRC/L=RAL/CN=arc-ce01.gridpp.rl.ac.uk',
                    'info_ID': '782G0',
                    'monitored': '1',
                    'production': '1',
                    'scope': 'EGI, wlcg, tier1, alice, atlas, cms, lhcb'},
                'type': 'SITES'
            }

        )

    def test_HaveExtensions(self):
        # Assert pass_extensions=False is working
        self.assertFalse(endpoints_have_extension(self.group_endpoints))

    def test_EnabledExtensions(self):
        # Assert pass_extensions=True is working
        self.assertTrue(endpoints_have_extension(self.group_endpoints_ext))

    def test_SuffixUid(self):
        # Assert uid=True is working
        temp_group = get_group(self.group_endpoints_ext, 'GSI-LCG2')
        self.assertIsNotNone(temp_group)
        self.assertEqual(temp_group['hostname'], 'grid13.gsi.de_14G0')

        temp_group = get_group(self.group_endpoints_ext, 'RAL-LCG2')
        self.assertIsNotNone(temp_group)
        self.assertEqual(temp_group['hostname'], 'arc-ce01.gridpp.rl.ac.uk_782G0')

        temp_group = get_group(self.group_endpoints_ext, 'AZ-IFAN')
        self.assertIsNotNone(temp_group)
        self.assertEqual(temp_group['hostname'], 'ce.physics.science.az_1555G0')

    def test_ConnectorParseErrorException(self):
        # Assert proper exception is thrown if empty xml is given to the function
        with self.assertRaises(ConnectorParseError) as cm:
            ParseServiceEndpoints(logger, '', 'CUSTOMERFOO', uid=True, pass_extensions=True)
        excep = cm.exception
        self.assertTrue('XML feed' in excep.msg)
        self.assertTrue('ExpatError' in excep.msg)


class MeshSitesAndContacts(unittest.TestCase):
    def setUp(self):
        logger.customer = CUSTOMER_NAME
        self.maxDiff = None
        self.sample_sites_data = [
            {
                'group': 'iris.ac.uk',
                'subgroup': 'dirac-durham',
                'tags': {
                    'certification': 'Certified', 'infrastructure':
                    'Production', 'scope': 'iris.ac.uk'
                },
                'type': 'NGI'
            },
            {
                'group': 'Russia',
                'subgroup': 'RU-SARFTI',
                'tags': {
                    'certification': 'Certified', 'infrastructure':
                    'Production', 'scope': 'EGI'
                },
                'type': 'NGI'
            },
        ]
        self.sample_sites_contacts = [
            {
                'contacts': [
                    {
                        'certdn': 'certdn1-dirac-durham',
                        'email': 'name1.surname1@durham.ac.uk',
                        'forename': 'Name1',
                        'role': 'Site Administrator',
                        'surname': 'Surname1'
                    },
                    {
                        'certdn': 'certdn2-dirac-durham',
                        'email': 'name2.surname2@durham.ac.uk',
                        'forename': 'Name2',
                        'role': 'Site Operations Manager',
                        'surname': 'Surname2'
                    }
                ],
                'name': 'dirac-durham'
            },
            {
                'contacts': [
                    {
                        'certdn': 'certdn1-ru-sarfti',
                        'email': 'name1.surname1@gmail.com',
                        'forename': 'Name1',
                        'role': 'Site Administrator',
                        'surname': 'Surname1'
                    },
                    {
                        'certdn': 'certdn2-ru-sarfti',
                        'email': 'name2.surname2@gmail.com',
                        'forename': 'Name2',
                        'role': 'Site Administrator',
                        'surname': 'Surname2'
                    },
                ],
                'name': 'RU-SARFTI'
            },
        ]

    def test_SitesAndContacts(self):
        attach_contacts_topodata(logger, self.sample_sites_contacts, self.sample_sites_data)
        self.assertEqual(self.sample_sites_data[0],
            {
                'group': 'iris.ac.uk',
                'notifications': {'contacts': ['name1.surname1@durham.ac.uk',
                                               'name2.surname2@durham.ac.uk'],
                                  'enabled': True},
                'subgroup': 'dirac-durham',
                'tags': {'certification': 'Certified', 'infrastructure':
                         'Production', 'scope': 'iris.ac.uk'},
                'type': 'NGI'
            }
        )
        self.assertEqual(self.sample_sites_data[1],
            {
                'group': 'Russia',
                'notifications': {'contacts': ['name1.surname1@gmail.com',
                                               'name2.surname2@gmail.com'],
                                  'enabled': True},
                'subgroup': 'RU-SARFTI',
                'tags': {'certification': 'Certified', 'infrastructure':
                         'Production', 'scope': 'EGI'},
                'type': 'NGI'
            }
        )


class MeshServiceGroupsAndContacts(unittest.TestCase):
    def setUp(self):
        logger.customer = CUSTOMER_NAME
        self.maxDiff = None
        self.sample_servicegroups_data = [
            {
                'group': 'EGI',
                'subgroup': 'NGI_ARMGRID_SERVICES',
                'tags': {
                    'monitored': '1',
                    'scope': 'EGI'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'EGI',
                'subgroup': 'NGI_CYGRID_SERVICES',
                'tags': {
                    'monitored': '1',
                    'scope': 'EGI'
                },
                'type': 'PROJECT'
            },
        ]
        self.sample_servicegroup_contacts = [
            {
                'contacts': ['Name1.Surname1@email.com', 'Name2.Surname2@email.com'],
                'name': 'NGI_ARMGRID_SERVICES'
            },
            {
                'contacts': ['Name3.Surname3@email.com', 'Name4.Surname4@email.com'],
                'name': 'NGI_CYGRID_SERVICES'
            },

        ]

    def test_ServiceGroupsAndContacts(self):
        attach_contacts_topodata(logger, self.sample_servicegroup_contacts,
                                 self.sample_servicegroups_data)
        self.assertEqual(self.sample_servicegroups_data[0],
            {
                'group': 'EGI',
                'subgroup': 'NGI_ARMGRID_SERVICES',
                'notifications': {
                    'contacts': ['Name1.Surname1@email.com', 'Name2.Surname2@email.com'],
                    'enabled': True
                },
                'tags': {
                    'monitored': '1',
                    'scope': 'EGI'
                },
                'type': 'PROJECT'
            }
        )


class MeshServiceEndpointsAndContacts(unittest.TestCase):
    def setUp(self):
        logger.customer = CUSTOMER_NAME
        self.maxDiff = None
        self.sample_serviceendpoints_data = [
            {
                'group': 'GROUP1',
                'hostname': 'fqdn1.com',
                'service': 'service1',
                'tags': {
                    'monitored': '1',
                    'production': '0',
                    'scope': ''
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'GROUP2',
                'hostname': 'fqdn2.com',
                'service': 'service2',
                'tags': {
                    'monitored': '1',
                    'production': '0',
                    'scope': ''
                },
                'type': 'SERVICEGROUPS'
            }
        ]
        self.sample_serviceendpoints_contacts = [
            {
                'contacts': ['Name1.Surname1@email.com', 'Name2.Surname2@email.com'],
                'name': 'fqdn1.com+service1'
            },
            {
                'contacts': ['Name3.Surname3@email.com', 'Name4.Surname4@email.com'],
                'name': 'fqdn2.com+service2'
            }
        ]

    def test_ServiceEndpointsAndContacts(self):
        attach_contacts_topodata(logger, self.sample_serviceendpoints_contacts,
                                 self.sample_serviceendpoints_data)
        self.assertEqual(self.sample_serviceendpoints_data[0],
            {
                'group': 'GROUP1',
                'hostname': 'fqdn1.com',
                'service': 'service1',
                'notifications': {
                    'contacts': ['Name1.Surname1@email.com', 'Name2.Surname2@email.com'],
                    'enabled': True
                },
                'tags': {
                    'monitored': '1',
                    'production': '0',
                    'scope': ''
                },
                'type': 'SERVICEGROUPS'
            }
        )
        self.assertEqual(self.sample_serviceendpoints_data[1],
            {
                'group': 'GROUP2',
                'hostname': 'fqdn2.com',
                'service': 'service2',
                'notifications': {
                    'contacts': ['Name3.Surname3@email.com', 'Name4.Surname4@email.com'],
                    'enabled': True
                },
                'tags': {
                    'monitored': '1',
                    'production': '0',
                    'scope': ''
                },
                'type': 'SERVICEGROUPS'
            }
        )


class ParseServiceEndpointsAndServiceGroupsCsv(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-topo.csv') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME

        self.topology = ParseFlatEndpoints(logger, self.content, CUSTOMER_NAME,
                                           uidservtype=True,
                                           fetchtype='ServiceGroups',
                                           scope=CUSTOMER_NAME, is_csv=True)

    def test_CsvTopology(self):
        group_groups = self.topology.get_groupgroups()
        self.assertEqual(group_groups,
            [
                {
                    'group': 'CUSTOMERFOO',
                    'subgroup': 'NextCloud',
                    'tags': {'monitored': '1', 'scope': 'CUSTOMERFOO'},
                    'type': 'PROJECT'
                },
                {
                    'group': 'CUSTOMERFOO',
                    'subgroup': 'AAI',
                    'tags': {'monitored': '1', 'scope': 'CUSTOMERFOO'},
                    'type': 'PROJECT'
                }
            ]
        )
        group_endpoints = self.topology.get_groupendpoints()
        self.assertEqual(group_endpoints,
            [
                {
                    'group': 'NextCloud',
                    'hostname': 'files.dev.tenant.eu_tenant_1',
                    'service': 'nextcloud',
                    'tags': {'hostname': 'files.dev.tenant.eu', 'info_ID':
                             'tenant_1', 'info_URL':
                             'https://files.dev.tenant.eu', 'monitored': '1',
                             'scope': 'CUSTOMERFOO'},
                    'type': 'SERVICEGROUPS'
                },
                {
                    'group': 'NextCloud',
                    'hostname': 'files.tenant.eu_tenant_2',
                    'service': 'nextcloud',
                    'tags': {'hostname': 'files.tenant.eu', 'info_ID':
                             'tenant_2', 'info_URL': 'https://files.tenant.eu',
                             'monitored': '1', 'scope': 'CUSTOMERFOO'},
                    'type': 'SERVICEGROUPS'
                },
                {
                    'group': 'AAI',
                    'hostname': 'sso.tenant.eu_tenant_3',
                    'service': 'aai',
                    'tags': {'hostname': 'sso.tenant.eu', 'info_ID': 'tenant_3',
                            'info_URL': 'https://sso.tenant.eu', 'monitored': '1',
                            'scope': 'CUSTOMERFOO'},
                    'type': 'SERVICEGROUPS'
                }
            ]
        )

    def test_FailedCsvTopology(self):
        with self.assertRaises(ConnectorParseError) as cm:
            self.failed_topology = ParseFlatEndpoints(logger, 'RUBBISH_DATA',
                                                    CUSTOMER_NAME,
                                                    uidservtype=True,
                                                    fetchtype='ServiceGroups',
                                                    scope=CUSTOMER_NAME,
                                                    is_csv=True)
        excep = cm.exception
        self.assertTrue('CSV feed' in excep.msg)


class ParseServiceEndpointsAndServiceGroupsJson(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-topo.json') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME

        self.topology = ParseFlatEndpoints(logger, self.content, CUSTOMER_NAME,
                                           uidservtype=True,
                                           fetchtype='ServiceGroups',
                                           scope=CUSTOMER_NAME, is_csv=False)

    def test_JsonTopology(self):
        group_groups = self.topology.get_groupgroups()
        self.assertEqual(group_groups,
            [
                {
                    'group': 'CUSTOMERFOO',
                    'subgroup': 'Open Telekom Cloud',
                    'tags': {
                        'monitored': '1', 'scope': 'CUSTOMERFOO'
                    },
                    'type': 'PROJECT'
                },
                {
                    'group': 'CUSTOMERFOO',
                    'subgroup': 'PaaS Orchestrator ',
                    'tags': {'monitored': '1', 'scope': 'CUSTOMERFOO'},
                    'type': 'PROJECT'
                }
            ]
        )
        group_endpoints = self.topology.get_groupendpoints()
        self.assertEqual(group_endpoints,
            [
                {
                    'group': 'Open Telekom Cloud',
                    'hostname': 'open-telekom-cloud.com_227',
                    'service': 'eu.eosc.portal.services.url',
                    'tags': {
                        'hostname': 'open-telekom-cloud.com',
                        'info_ID': '227',
                        'info_URL': 'https://open-telekom-cloud.com/en',
                        'monitored': '1',
                        'scope': 'CUSTOMERFOO'
                    },
                    'type': 'SERVICEGROUPS'
                },
                {
                    'group': 'PaaS Orchestrator ',
                    'hostname': 'indigo-paas.cloud.ba.infn.it_243',
                    'service': 'eu.eosc.portal.services.url',
                    'tags': {
                        'hostname': 'indigo-paas.cloud.ba.infn.it',
                        'info_ID': '243',
                        'info_URL': 'https://indigo-paas.cloud.ba.infn.it',
                        'monitored': '1',
                        'scope': 'CUSTOMERFOO'
                    },
                    'type': 'SERVICEGROUPS'
                }
            ]
        )

    def test_FailedJsonTopology(self):
        with self.assertRaises(ConnectorParseError) as cm:
            self.failed_topology = ParseFlatEndpoints(logger, 'RUBBISH_DATA', CUSTOMER_NAME,
                                                    uidservtype=True,
                                                    fetchtype='ServiceGroups',
                                                    scope=CUSTOMER_NAME,
                                                    is_csv=False)
        excep = cm.exception
        self.assertTrue('JSON feed' in excep.msg)
        self.assertTrue('JSONDecodeError' in excep.msg)


class ParseServiceEndpointsBiomed(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_endpoint_biomed.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        parse_service_endpoints = ParseServiceEndpoints(logger, self.content, CUSTOMER_NAME)
        self.group_endpoints = parse_service_endpoints.get_group_endpoints()


    def test_BiomedEndpoints(self):
        self.assertEqual(self.group_endpoints,
            [
                {
                    'group': 'HG-02-IASA',
                    'hostname': 'cream-ce01.marie.hellasgrid.gr',
                    'service': 'APEL',
                    'tags': {
                        'info_ID': '451G0',
                        'monitored': '1',
                        'production': '1',
                        'scope': 'EGI'
                    },
                    'type': 'SITES'},
                {
                    'group': 'TR-10-ULAKBIM',
                    'hostname': 'kalkan1.ulakbim.gov.tr',
                    'service': 'APEL',
                    'tags': {
                        'info_HOSTDN': '/C=TR/O=TRGrid/OU=TUBITAK-ULAKBIM/CN=kalkan1.ulakbim.gov.tr',
                        'info_ID': '375G0',
                        'monitored': '1',
                        'production': '1',
                        'scope': 'EGI, wlcg, tier2, atlas'
                    },
                    'type': 'SITES'
                }
            ]
        )


class ParseSitesBiomed(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-sites_biomed.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        parse_sites = ParseSites(logger, self.content, CUSTOMER_NAME)
        self.group_groups = parse_sites.get_group_groups()

    def test_BiomedSites(self):
        self.assertEqual(self.group_groups,
            [
                {
                    'group': 'NGI_FRANCE',
                    'subgroup': 'AUVERGRID',
                    'tags': {
                        'certification': '', 'infrastructure': '', 'scope': ''
                    },
                    'type': 'NGI'
                },
                {
                    'group': 'NGI_IT',
                    'subgroup': 'CNR-ILC-PISA',
                    'tags': {
                        'certification': '', 'infrastructure': '', 'scope': ''
                    },
                    'type': 'NGI'
                }
            ]
        )

if __name__ == '__main__':
    unittest.main()
