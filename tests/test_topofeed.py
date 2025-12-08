import unittest

from argo_connectors.config.customer import Customer
from argo_connectors.config.glob import Global
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.log import Logger
from argo_connectors.mesh.contacts import attach_contacts_topodata
from argo_connectors.parse.flat_topology import ParseFlatEndpoints
from argo_connectors.parse.gocdb_topology import ParseServiceEndpoints, ParseSites
from argo_connectors.parse.lot1sc_topology import ParseLot1ScEndpoints
from argo_connectors.parse.provider_topology import ParseTopo, ParseExtensions, buildmap_id2groupname


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
        glob = Global('topology-gocdb-connector.py')
        cust = Customer('topology-gocdb-connector.py')
        cust.custopts['TopoType'] = 'GOCDB'
        cust.custopts['TopoFetchType'] = 'Sites'
        cust.custopts['TopoUIDServiceEndpoints'] = True
        cust.custopts['TopoFeedServiceEndpointsExtensions'] = False
        logger = Logger(f'{__name__}.{__class__.__name__}')
        logger.customer = CUSTOMER_NAME

        with open('tests/sample-service_endpoint.xml') as feed_file:
            self.content = feed_file.read()

        glob.options()['GeneralPassExtensions'.lower()] = False
        parse_service_endpoints = ParseServiceEndpoints(self.content)
        self.group_endpoints = parse_service_endpoints.get_group_endpoints()

        glob.options()['GeneralPassExtensions'.lower()] = True
        parse_service_endpoints_ext = ParseServiceEndpoints(self.content)
        self.group_endpoints_ext = parse_service_endpoints_ext.get_group_endpoints()

        self.maxDiff = None

    def test_LenEndpoints(self):
        self.assertEqual(len(self.group_endpoints), 4)  # Parsed correct number of endpoint groups

    def test_DataEndpoints(self):
        self.assertEqual(self.group_endpoints[0], {
            'group': 'AZ-IFAN',
            'hostname': 'ce.physics.science.az_1555G0',
            'service': 'CREAM-CE',
            'tags': {'info_HOSTDN': '/DC=ORG/DC=SEE-GRID/O=Hosts/O=Institute of Physics of ANAS/CN=ce.physics.science.az',
                     'hostname': 'ce.physics.science.az',
                     'info_ID': '1555G0',
                     'info_URL': 'ce.physics.science.az:8443/cream-pbs-ops',
                     'info_service_endpoint_URL': 'ce.physics.science.az:8443/cream-pbs-ops',
                     'monitored': '1',
                     'production': '1',
                     'scope': 'EGI, wlcg, atlas'},
            'type': 'SITES'
        }, {
            'group': 'RAL-LCG2',
            'hostname': 'arc-ce01.gridpp.rl.ac.uk_782G0',
            'service': 'gLite-APEL',
            'tags': {'info_HOSTDN': '/C=UK/O=eScience/OU=CLRC/L=RAL/CN=arc-ce01.gridpp.rl.ac.uk',
                     'info_ID': '782G0',
                     'monitored': '1',
                     'hostname': 'arc-ce01.gridpp.rl.ac.uk',
                     'production': '1',
                     'scope': 'EGI, wlcg, tier1, alice, atlas, cms, lhcb'},
            'type': 'SITES'
        })

    def test_HaveExtensions(self):
        # Assert pass_extensions=False is working
        self.assertFalse(endpoints_have_extension(self.group_endpoints))

    def test_EnabledExtensions(self):
        # Assert pass_extensions=True is working
        self.assertTrue(endpoints_have_extension(self.group_endpoints_ext))
        self.assertEqual(self.group_endpoints_ext, [
            {
                'group': 'AZ-IFAN',
                'hostname': 'ce.physics.science.az_1555G0',
                'service': 'CREAM-CE',
                'tags': {
                    'info_HOSTDN': '/DC=ORG/DC=SEE-GRID/O=Hosts/O=Institute of Physics '
                                   'of ANAS/CN=ce.physics.science.az',
                    'info_ID': '1555G0',
                    'info_URL': 'ce.physics.science.az:8443/cream-pbs-ops',
                    'info_service_endpoint_URL': 'ce.physics.science.az:8443/cream-pbs-ops',
                    'monitored': '1',
                    'production': '1',
                    'hostname': 'ce.physics.science.az',
                    'scope': 'EGI, wlcg, atlas'
                },
                'type': 'SITES'
            },
            {
                'group': 'GRIDOPS-CheckIn',
                'hostname': 'aai.egi.eu_9502G0',
                'service': 'egi.aai.oidc',
                'tags': {
                    'info_ID': '9502G0',
                    'info_URL': 'https://aai.egi.eu/auth/realms/egi',
                    'info_ext_ARGO_OIDC_AUTHORISATION_ENDPOINT': '/auth/realms/egi/protocol/openid-connect/auth',
                    'info_ext_ARGO_OIDC_PROVIDER_CONFIGURATION': '/auth/realms/egi/.well-known/openid-configuration',
                    'info_service_endpoint_URL': 'https://aai.egi.eu/auth/realms/egi/.well-known/openid-configuration, '
                                                 'https://aai.egi.eu/auth/realms/egi/protocol/openid-connect/auth',
                    'monitored': '1',
                    'production': '1',
                    'hostname': 'aai.egi.eu',
                    'scope': 'EGI'
                },
                'type': 'SITES'},
            {
                'group': 'GSI-LCG2',
                'hostname': 'grid13.gsi.de_14G0',
                'service': 'CE',
                'tags': {
                    'info_ID': '14G0',
                    'hostname': 'grid13.gsi.de',
                    'monitored': '0',
                    'production': '0',
                    'scope': 'EGI, wlcg, tier2, alice'
                },
                'type': 'SITES'
            },
            {
                'group': 'RAL-LCG2',
                'hostname': 'arc-ce01.gridpp.rl.ac.uk_782G0',
                'service': 'gLite-APEL',
                'tags': {
                    'info_HOSTDN': '/C=UK/O=eScience/OU=CLRC/L=RAL/CN=arc-ce01.gridpp.rl.ac.uk',
                    'info_ID': '782G0',
                    'info_ext_InformationSystem': 'https://www.gridpp.rl.ac.uk/RAL-LCG2/RAL-LCG2_CE.json',
                    'hostname': 'arc-ce01.gridpp.rl.ac.uk',
                    'monitored': '1',
                    'production': '1',
                    'scope': 'EGI, wlcg, tier1, alice, atlas, cms, lhcb'
                },
                'type': 'SITES'
            }
        ])

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
            ParseServiceEndpoints('')
        excep = cm.exception
        self.assertTrue('endpoint feed' in excep.msg)
        self.assertTrue('XMLSyntaxError' in excep.msg)


class MeshSitesAndContacts(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.notification_flag = True
        self.sample_sites_data = [
            {
                'group': 'iris.ac.uk',
                'subgroup': 'dirac-durham',
                'notifications': {
                    'enabled': False
                },
                'tags': {
                    'certification': 'Certified', 'infrastructure':
                    'Production', 'scope': 'iris.ac.uk'
                },
                'type': 'NGI'
            },
            {
                'group': 'Russia',
                'subgroup': 'RU-SARFTI',
                'notifications': {
                    'enabled': True
                },
                'tags': {
                    'certification': 'Certified', 'infrastructure':
                    'Production', 'scope': 'EGI'
                },
                'type': 'NGI'
            },
        ]
        self.sample_sites_contacts = {
            'dirac-durham': [
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
            'RU-SARFTI': [
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
                }
            ]
        }

    def test_SitesAndContacts(self):
        attach_contacts_topodata(self.sample_sites_contacts,
                                 self.sample_sites_data,
                                 self.notification_flag)
        self.assertEqual(self.sample_sites_data[0], {
            'group': 'iris.ac.uk',
            'notifications': {'contacts': ['name1.surname1@durham.ac.uk',
                                           'name2.surname2@durham.ac.uk'],
                              'enabled': False},
            'subgroup': 'dirac-durham',
            'tags': {'certification': 'Certified', 'infrastructure':
                     'Production', 'scope': 'iris.ac.uk'},
            'type': 'NGI'
        })
        self.assertEqual(self.sample_sites_data[1], {
            'group': 'Russia',
            'notifications': {'contacts': ['name1.surname1@gmail.com',
                                           'name2.surname2@gmail.com'],
                              'enabled': True},
            'subgroup': 'RU-SARFTI',
            'tags': {'certification': 'Certified', 'infrastructure':
                     'Production', 'scope': 'EGI'},
            'type': 'NGI'
        })

    def test_SitesAndContactsNoHonorNotificationFlag(self):
        attach_contacts_topodata(self.sample_sites_contacts,
                                 self.sample_sites_data,
                                 False)
        self.assertEqual(self.sample_sites_data[0], {
            'group': 'iris.ac.uk',
            'notifications': {'contacts': ['name1.surname1@durham.ac.uk',
                                           'name2.surname2@durham.ac.uk'],
                              'enabled': True},
            'subgroup': 'dirac-durham',
            'tags': {'certification': 'Certified', 'infrastructure':
                     'Production', 'scope': 'iris.ac.uk'},
            'type': 'NGI'
        })
        self.assertEqual(self.sample_sites_data[1], {
            'group': 'Russia',
            'notifications': {'contacts': ['name1.surname1@gmail.com',
                                           'name2.surname2@gmail.com'],
                              'enabled': True},
            'subgroup': 'RU-SARFTI',
            'tags': {'certification': 'Certified', 'infrastructure':
                     'Production', 'scope': 'EGI'},
            'type': 'NGI'
        })


class MeshServiceGroupsAndContacts(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.notification_flag = True
        self.sample_servicegroups_data = [
            {
                'group': 'EGI',
                'subgroup': 'NGI_ARMGRID_SERVICES',
                'notifications': {
                    'enabled': True
                },
                'tags': {
                    'monitored': '1',
                    'scope': 'EGI'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'EGI',
                'subgroup': 'NGI_CYGRID_SERVICES',
                'notifications': {
                    'enabled': False
                },
                'tags': {
                    'monitored': '1',
                    'scope': 'EGI'
                },
                'type': 'PROJECT'
            },
        ]
        self.sample_servicegroup_contacts = {
            'NGI_ARMGRID_SERVICES': ['Name1.Surname1@email.com', 'Name2.Surname2@email.com'],
            'NGI_CYGRID_SERVICES': ['Name3.Surname3@email.com', 'Name4.Surname4@email.com'],
        }

    def test_ServiceGroupsAndContacts(self):
        attach_contacts_topodata(self.sample_servicegroup_contacts,
                                 self.sample_servicegroups_data, self.notification_flag)
        self.assertEqual(self.sample_servicegroups_data[0], {
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
        })

    def test_ServiceGroupsAndContactsNoHonorNotificationFlag(self):
        attach_contacts_topodata(self.sample_servicegroup_contacts,
                                 self.sample_servicegroups_data, False)
        self.assertEqual(self.sample_servicegroups_data[1], {
            'group': 'EGI',
            'subgroup': 'NGI_CYGRID_SERVICES',
            'notifications': {
                'contacts': ['Name3.Surname3@email.com', 'Name4.Surname4@email.com'],
                'enabled': True
            },
            'tags': {
                'monitored': '1',
                'scope': 'EGI'
            },
            'type': 'PROJECT'
        })


class MeshServiceEndpointsAndContacts(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.notfication_flag = True
        self.sample_serviceendpoints_data = [
            {
                'group': 'GROUP1',
                'hostname': 'fqdn1.com',
                'service': 'service1',
                'notifications': {
                    'enabled': True
                },
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
                'notifications': {
                    'enabled': True
                },
                'tags': {
                    'monitored': '1',
                    'production': '0',
                    'scope': ''
                },
                'type': 'SERVICEGROUPS'
            }
        ]
        self.sample_serviceendpoints_contacts = {
            'fqdn1.com+service1': ['Name1.Surname1@email.com', 'Name2.Surname2@email.com'],
            'fqdn2.com+service2': ['Name3.Surname3@email.com', 'Name4.Surname4@email.com']
        }

    def test_ServiceEndpointsAndContacts(self):
        attach_contacts_topodata(self.sample_serviceendpoints_contacts,
                                 self.sample_serviceendpoints_data, self.notfication_flag)
        self.assertEqual(self.sample_serviceendpoints_data[0], {
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
        })
        self.assertEqual(self.sample_serviceendpoints_data[1], {
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
        })


class ParseServiceEndpointsAndServiceGroupsCsv(unittest.TestCase):
    def setUp(self):
        _ = Global('topology-csv-connector.py')
        cust = Customer('topology-csv-connector.py')
        cust.custopts['TopoType'] = 'CSV'
        cust.custopts['TopoFetchType'] = 'ServiceGroups'
        cust.custopts['TopoUIDServiceEndpoints'] = True
        cust.custopts['Name'] = 'CUSTOMERFOO'
        logger = Logger(f'{__name__}.{__class__.__name__}')
        logger.customer = 'CUSTOMERFOO'
        with open('tests/sample-topo.csv') as feed_file:
            self.content = feed_file.read()

        self.topology = ParseFlatEndpoints(self.content, is_csv=True)
        self.maxDiff = None

    def test_CsvTopology(self):
        group_groups = self.topology.get_groupgroups()

        self.assertEqual(group_groups, [
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
            },
            {
                'group': 'CUSTOMERFOO',
                'subgroup': 'NEANIAS-Space',
                'tags': {'monitored': '1', 'scope': 'CUSTOMERFOO'},
                'type': 'PROJECT'
            }
        ])
        group_endpoints = self.topology.get_groupendpoints()
        self.assertEqual(group_endpoints, [
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
            },
            {
                'group': 'NEANIAS-Space',
                'hostname': 'ia2-vialactea.oats.inaf.it_neanias_4',
                'service': 'WebService',
                'tags': {'hostname': 'ia2-vialactea.oats.inaf.it',
                         'info_ID': 'neanias_4', 'info_URL':
                         'http://ia2-vialactea.oats.inaf.it:8080/vlkb/availability',
                         'monitored': '1', 'scope': 'CUSTOMERFOO'},
                'type': 'SERVICEGROUPS'
            }
        ])

    def test_FailedCsvTopology(self):
        with self.assertRaises(ConnectorParseError) as cm:
            self.failed_topology = ParseFlatEndpoints('FAILED_DATA',
                                                      is_csv=True)
        excep = cm.exception
        self.assertTrue('CSV feed' in excep.msg)


class ParseServiceEndpointsAndServiceGroupsJson(unittest.TestCase):
    def setUp(self):
        _ = Global('topology-csv-connector.py')
        cust = Customer('topology-csv-connector.py')
        cust.custopts['TopoType'] = 'CSV'
        cust.custopts['TopoFetchType'] = 'ServiceGroups'
        cust.custopts['TopoUIDServiceEndpoints'] = True
        cust.custopts['Name'] = 'CUSTOMERFOO'
        logger = Logger(f'{__name__}.{__class__.__name__}')
        logger.customer = 'CUSTOMERFOO'
        with open('tests/sample-topo.json') as feed_file:
            self.content = feed_file.read()

        self.topology = ParseFlatEndpoints(self.content,
                                           is_csv=False)

    def test_JsonTopology(self):
        group_groups = self.topology.get_groupgroups()
        self.assertEqual(group_groups, [
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
        ])
        group_endpoints = self.topology.get_groupendpoints()
        self.assertEqual(group_endpoints, [
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
        ])

    def test_FailedJsonTopology(self):
        with self.assertRaises(ConnectorParseError) as cm:
            self.failed_topology = ParseFlatEndpoints('FAILED_DATA',
                                                      is_csv=False)
        excep = cm.exception
        self.assertTrue('JSON feed' in excep.msg)
        self.assertTrue('JSONDecodeError' in excep.msg)


class ParseServiceEndpointsBiomed(unittest.TestCase):
    def setUp(self):
        glob = Global('topology-gocdb-connector.py')
        glob.options()['GeneralPassExtensions'.lower()] = False
        cust = Customer('topology-gocdb-connector.py')
        cust.custopts['TopoType'] = 'CSV'
        cust.custopts['TopoFetchType'] = 'Sites'
        cust.custopts['TopoUIDServiceEndpoints'] = False
        cust.custopts['Name'] = 'CUSTOMERFOO'
        logger = Logger(f'{__name__}.{__class__.__name__}')
        logger.customer = 'CUSTOMERFOO'
        with open('tests/sample-service_endpoint_biomed.xml') as feed_file:
            self.content = feed_file.read()
        parse_service_endpoints = ParseServiceEndpoints(self.content)
        self.maxDiff = None
        self.group_endpoints = parse_service_endpoints.get_group_endpoints()

    def test_BiomedEndpoints(self):
        self.assertEqual(self.group_endpoints, [
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
        ])


class ParseSitesBiomed(unittest.TestCase):
    def setUp(self):
        glob = Global('topology-gocdb-connector.py')
        glob.options()['GeneralPassExtensions'.lower()] = False
        cust = Customer('topology-gocdb-connector.py')
        cust.custopts['TopoType'] = 'CSV'
        cust.custopts['TopoFetchType'] = 'Sites'
        cust.custopts['TopoUIDServiceEndpoints'] = True
        cust.custopts['Name'] = 'CUSTOMERFOO'
        logger = Logger(f'{__name__}.{__class__.__name__}')
        logger.customer = 'CUSTOMERFOO'
        with open('tests/sample-sites_biomed.xml') as feed_file:
            self.content = feed_file.read()
        parse_sites = ParseSites(self.content)
        self.group_groups = parse_sites.get_group_groups()

    def test_BiomedSites(self):
        self.assertEqual(self.group_groups, [
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
        ])


class ParseSitesTest(unittest.TestCase):
    def setUp(self):
        glob = Global('topology-gocdb-connector.py')
        glob.options()['GeneralPassExtensions'.lower()] = False
        cust = Customer('topology-gocdb-connector.py')
        cust.custopts['TopoType'] = 'CSV'
        cust.custopts['TopoFetchType'] = 'Sites'
        cust.custopts['TopoUIDServiceEndpoints'] = True
        cust.custopts['Name'] = 'CUSTOMERFOO'
        cust.custopts['HonorNotificationFlag'] = True
        logger = Logger(f'{__name__}.{__class__.__name__}')
        logger.customer = 'CUSTOMERFOO'
        with open('tests/sample-site.xml') as feed_file:
            self.content = feed_file.read()
        self.notification_flag = True
        parse_sites = ParseSites(self.content)
        self.group_groups = parse_sites.get_group_groups()
        self.maxDiff = None

    def test_EgiSites(self):
        self.assertEqual(self.group_groups, [
            {
                'group': 'NGI_CZ',
                'subgroup': 'prague_cesnet_lcg2_cert',
                'notifications': {
                    'contacts': [],
                    'enabled': False
                },
                'tags': {
                    'certification': 'Closed',
                    'infrastructure': 'Production',
                    'scope': 'EGI'
                },
                'type': 'NGI'
            },
            {
                'group': 'NGI_SK',
                'subgroup': 'TU-Kosice',
                'notifications': {
                    'contacts': [],
                    'enabled': True
                },
                'tags': {
                    'certification': 'Certified',
                    'infrastructure': 'Production',
                    'scope': 'EGI'
                },
                'type': 'NGI'
            },
            {
                'group': 'NGI_SK',
                'subgroup': 'IISAS-Bratislava',
                'notifications': {
                    'contacts': [],
                    'enabled': True
                },
                'tags': {
                    'certification': 'Certified',
                    'infrastructure': 'Production',
                    'scope': 'EGI'
                },
                'type': 'NGI'
            }]
        )


class ParseEoscProvider(unittest.TestCase):
    def setUp(self):
        _ = Global('topology-provider-connector.py')
        cust = Customer('topology-provider-connector.py')
        cust.custopts['TopoType'] = 'EOSC'
        cust.custopts['TopoFetchType'] = 'ServiceGroups'
        cust.custopts['TopoUIDServiceEndpoints'] = True
        logger = Logger(f'{__name__}.{__class__.__name__}')
        logger.customer = CUSTOMER_NAME
        with open('tests/sample-public-service.json', encoding='utf-8') as feed_file:
            resources = feed_file.read()
        with open('tests/sample-public-provider.json', encoding='utf-8') as feed_file:
            providers = feed_file.read()
        with open('tests/sample-provider-configurationtemplateinstance.json', encoding='utf-8') as feed_file:
            resource_extensions = feed_file.read()
        eosc_topo = ParseTopo(providers, resources)
        self.group_groups = eosc_topo.get_group_groups()
        self.group_endpoints = eosc_topo.get_group_endpoints()
        self.id_groupname = buildmap_id2groupname(self.group_endpoints)
        fakemap_idgroupnames = {
            '21-T15999-uxIE5y': '3rd-Party Data Security Assessment',
            '21-T15999-xVQZOZ': 'Italian SuperComputing Resource Allocation - ISCRA'
        }
        eosc_topo_extensions = ParseExtensions(resource_extensions, fakemap_idgroupnames)
        self.extensions = eosc_topo_extensions.get_extensions()
        self.maxDiff = None

    def test_groupGroups(self):
        self.assertEqual(self.group_groups, [
            {
                'group': 'CINECA',
                'subgroup': ' Italian SuperComputing Resource Allocation - ISCRA',
                'tags': {
                    'info_projectid': '21-T15999-llB2t3',
                    'provider_tags': 'High Performance Computing'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'ICTLC',
                'subgroup': '3rd-Party Data Security Assessment',
                'tags': {
                    'info_projectid': '21-T15999-kKG7EL'
                },
                'type': 'PROJECT'
            }
        ])

    def test_serviceExtensions(self):
        self.assertEqual(self.extensions, [
            {
                'group': '3rd-Party Data Security Assessment',
                'hostname': 'example.com_cti-8wEjbu',
                'service': 'eu.eosc.generic.json',
                'tags': {
                    'hostname': 'example.com',
                    'info_ID': 'cti-8wEjbu',
                    'info_URL': 'https://example.com',
                    'info_groupname': '3rd-Party Data Security Assessment'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Italian SuperComputing Resource Allocation - ISCRA',
                'hostname': 'www.google.com_cti-qykSlW',
                'service': 'eu.eosc.argo.mon',
                'tags': {
                    'hostname': 'www.google.com',
                    'info_ID': 'cti-qykSlW',
                    'info_URL': 'https://www.google.com',
                    'info_groupname': 'Italian SuperComputing Resource Allocation - '
                                      'ISCRA'
                },
                'type': 'SERVICEGROUPS'
            }
        ])

    def test_groupEndpoints(self):
        self.assertEqual(self.group_endpoints, [
            {
                'group': '3rd-Party Data Security Assessment',
                'hostname': 'ictlc.com_21-T15999-uxIE5y',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'ictlc.com',
                    'info_ID': '21-T15999-uxIE5y',
                    'info_URL': 'https://ictlc.com/ICTLC_2023_3rd-Party%20Data%20Security%20Assessment.pdf',
                    'info_groupname': '3rd-Party Data Security Assessment',
                    'service_tags': 'Cybersecurity, Supply Chain, Supply Chain '
                    'Management, GDPR, Audit, Data Breach, ENISA'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': ' Italian SuperComputing Resource Allocation - ISCRA',
                'hostname': 'www.hpc.cineca.it_21-T15999-xVQZOZ',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'www.hpc.cineca.it',
                    'info_ID': '21-T15999-xVQZOZ',
                    'info_URL': 'https://www.hpc.cineca.it/services/iscra',
                    'info_groupname': 'Italian SuperComputing Resource Allocation - '
                                      'ISCRA'
                },
                'type': 'SERVICEGROUPS'
            }
        ])

    def test_idGroupname(self):
        self.assertEqual(self.id_groupname, {
            '21-T15999-uxIE5y': '3rd-Party Data Security Assessment',
            '21-T15999-xVQZOZ': 'Italian SuperComputing Resource Allocation - ISCRA'
        })

    def test_meshContactsProviders(self):
        sample_resources_contacts = {
            'ictlc.com+21-T15999-uxIE5y': ['foo@bar.com']
        }

        attach_contacts_topodata(sample_resources_contacts, self.group_endpoints)
        self.assertEqual(self.group_endpoints[0], {
            'group': '3rd-Party Data Security Assessment',
            'hostname': 'ictlc.com_21-T15999-uxIE5y',
            'service': 'eu.eosc.portal.services.url',
            'tags': {
                'hostname': 'ictlc.com',
                'info_ID': '21-T15999-uxIE5y',
                'info_URL': 'https://ictlc.com/ICTLC_2023_3rd-Party%20Data%20Security%20Assessment.pdf',
                'info_groupname': '3rd-Party Data Security Assessment',
                'service_tags': 'Cybersecurity, Supply Chain, Supply Chain '
                'Management, GDPR, Audit, Data Breach, ENISA'
            },
            'type': 'SERVICEGROUPS',
            'notifications': {'contacts': ['foo@bar.com'], 'enabled': True},
        })

    def test_FailedEoscProviderTopology(self):
        with self.assertRaises(ConnectorParseError) as cm:
            eosc_topo = ParseTopo('FAILED_DATA', 'FAILED_DATA')
            self.group_groups = eosc_topo.get_group_groups()
            self.group_endpoints = eosc_topo.get_group_endpoints()
        excep = cm.exception
        self.assertTrue('JSON feed' in excep.msg)
        self.assertTrue('JSONDecodeError' in excep.msg)


class ParseLot1ServiceCatalogueTopology(unittest.TestCase):
    def setUp(self):
        _ = Global('topology-lot1sc-connector.py')
        cust = Customer('topology-lot1sc-connector.py')
        cust.custopts['TopoType'] = 'LOT1SC'
        cust.custopts['TopoFetchType'] = 'ServiceGroups'
        cust.custopts['TopoUIDServiceEndpoints'] = True
        logger = Logger(f'{__name__}.{__class__.__name__}')
        logger.customer = CUSTOMER_NAME
        with open('tests/sample-lot1sc.json', encoding='utf-8') as feed_file:
            providers_endpoints = feed_file.read()
        self.maxDiff = None
        lot1sc_topo = ParseLot1ScEndpoints(providers_endpoints, 2)
        self.group_groups = lot1sc_topo.get_group_groups()
        self.group_endpoints = lot1sc_topo.get_group_endpoints()

    def test_groupGroups(self):
        self.assertEqual(self.group_groups, [
            {
                'group': 'Phenomenal',
                'subgroup': 'Test service for datasource 4-6',
                'type': 'PROJECT',
                'tags': {
                    'tier': 2
                }
            },
            {
                'group': 'European Commission',
                'subgroup': 'Virtual Machines',
                'type': 'PROJECT',
                'tags': {
                    'tier': 2
                }
            },
            {
                'group': 'Test 20-11 14.41',
                'subgroup': 'Test Interactive Notebooks',
                'tags': {
                    'tier': 2
                },
                'type': 'PROJECT'},
            {
                'group': 'Test 20-11 14.41',
                'subgroup': 'Test File Sync and Share',
                'tags': {
                    'tier': 2
                },
                'type': 'PROJECT'
            },
            {
                'group': 'Test 20-11 14.41',
                'subgroup': 'Test-HPC',
                'tags': {
                    'tier': 2
                },
                'type': 'PROJECT'
            },
            {
                'group': 'Test 20-11 14.41',
                'subgroup': 'Test Cloud Container Platform',
                'tags': {
                    'tier': 2
                },
                'type': 'PROJECT'
            }
        ])

    def test_groupEndpoints(self):
        self.assertEqual(self.group_endpoints, [
            {
                'group': 'Test service for datasource 4-6',
                'hostname': 'testUrlEndpoint.com_f17e599f-095d-373e-bcfe-4fb017acf5d5',
                'service': 'service.type.1',
                'tags': {
                    'hostname': 'testUrlEndpoint.com',
                    'info_ID': 'f17e599f-095d-373e-bcfe-4fb017acf5d5',
                    'info_URL': 'https://testUrlEndpoint.com',
                    'service_name': 'FTS web console',
                    'site_name': 'PSNC',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Virtual Machines',
                'hostname': 'test.claudius.cloud.psnc.pl_bdb09405-f227-3cd6-b8fd-c19c52a3a354',
                'service': 'service.type.1',
                'tags': {
                    'hostname': 'test.claudius.cloud.psnc.pl',
                    'info_ID': 'bdb09405-f227-3cd6-b8fd-c19c52a3a354',
                    'info_URL': 'https://test.claudius.cloud.psnc.pl/',
                    'service_name': 'OpenStack Horizon Dashboard',
                    'site_name': 'PSNC', 'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Virtual Machines',
                'hostname': 'test.claudius.cloud.psnc.pl_749deefd-e633-385a-805a-3fd64b80dbe4',
                'service': 'service.type.2',
                'tags': {
                    'hostname': 'test.claudius.cloud.psnc.pl',
                    'info_ID': '749deefd-e633-385a-805a-3fd64b80dbe4',
                    'info_URL': 'https://test.claudius.cloud.psnc.pl/',
                    'service_name': 'OpenStack Horizon Dashboard',
                    'site_name': 'PSNC', 'tier': 2
                },
                'type': 'SERVICEGROUPS'},
            {
                'group': 'Virtual Machines',
                'hostname': 'test.claudius.cloud.psnc.pl_7caacfa4-5bca-34fc-80eb-386e0579b0e9',
                'service': 'service.type.2',
                'tags': {
                    'hostname': 'test.claudius.cloud.psnc.pl',
                    'info_ID': '7caacfa4-5bca-34fc-80eb-386e0579b0e9',
                    'info_URL':
                    'https://test.claudius.cloud.psnc.pl:5000',
                    'service_name': 'OpenStack API', 'site_name': 'PSNC',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'},
            {
                'group': 'Virtual Machines',
                'hostname': 'test.claudius.cloud.psnc.pl_8cbe072d-1975-3ca3-bc74-fa2591c99b07',
                'service': 'service.type.1',
                'tags': {
                    'hostname': 'test.claudius.cloud.psnc.pl',
                    'info_ID': '8cbe072d-1975-3ca3-bc74-fa2591c99b07',
                    'info_URL': 'https://test.claudius.cloud.psnc.pl:5000/v3/auth/OS-FEDERATION/identity_providers/testing.eosc-federation.eu_openid/protocols/openid/websso',
                    'service_name': 'OpenStack Horizon Dashboard GUI Redirection',
                    'site_name': 'PSNC',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Virtual Machines',
                'hostname': 'openstack.testing.safedc.services_bdb09405-f227-3cd6-b8fd-c19c52a3a354',
                'service': 'service.type.1',
                'tags': {
                    'hostname': 'openstack.testing.safedc.services',
                    'info_ID': 'bdb09405-f227-3cd6-b8fd-c19c52a3a354',
                    'info_URL': 'https://openstack.testing.safedc.services/',
                    'service_name': 'OpenStack Horizon Dashboard',
                    'site_name': 'Safespring',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Virtual Machines',
                'hostname': 'openstack.testing.safedc.services_dd81e46a-6d98-3258-be69-2929d162dc18',
                'service': 'service.type.3',
                'tags': {
                    'hostname': 'openstack.testing.safedc.services',
                    'info_ID': 'dd81e46a-6d98-3258-be69-2929d162dc18',
                    'info_URL': 'https://openstack.testing.safedc.services/',
                    'service_name': 'OpenStack Horizon Dashboard',
                    'site_name': 'Safespring',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Virtual Machines',
                'hostname': 'openstack.testing.safedc.services_8e57a57f-c6fb-3dea-b586-2871f908776c',
                'service': 'service.type.2',
                'tags': {
                    'hostname': 'openstack.testing.safedc.services',
                    'info_ID': '8e57a57f-c6fb-3dea-b586-2871f908776c',
                    'info_URL': 'https://openstack.testing.safedc.services:5000/identity/v3/auth/OS-FEDERATION/identity_providers/proxy.testing.eosc-federation.eu/protocols/openid/websso',
                    'service_name': 'OpenStack Horizon Dashboard GUI Redirection',
                    'site_name': 'Safespring',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test Interactive Notebooks',
                'hostname': 'notebooks-tst.cloud.cesnet.cz_ad3e3f67-36a3-3d7e-9f0f-d0cccad9d221',
                'service': 'eu.eosc.interactive_notebooks.gui',
                'tags': {
                    'hostname': 'notebooks-tst.cloud.cesnet.cz',
                    'info_ID': 'ad3e3f67-36a3-3d7e-9f0f-d0cccad9d221',
                    'info_URL': 'https://notebooks-tst.cloud.cesnet.cz/',
                    'service_name': 'Notebooks GUI',
                    'site_name': 'CESNET',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test Interactive Notebooks',
                'hostname': 'notebooks-tst.cloud.cesnet.cz_cf3160b8-e297-32ba-9c87-014c17f02594',
                'service': 'eu.eosc.interactive_notebooks.api',
                'tags': {
                    'hostname': 'notebooks-tst.cloud.cesnet.cz',
                    'info_ID': 'cf3160b8-e297-32ba-9c87-014c17f02594',
                    'info_URL': 'https://notebooks-tst.cloud.cesnet.cz/services/jwt',
                    'service_name': 'Notebooks API',
                    'site_name': 'CESNET',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test Interactive Notebooks',
                'hostname': 'notebooks-tst.cloud.cesnet.cz_5b3dd363-b3bc-3b93-bb99-f64570603f39',
                'service': 'eu.eosc.interactive_notebooks.gui',
                'tags': {
                    'hostname': 'notebooks-tst.cloud.cesnet.cz',
                    'info_ID': '5b3dd363-b3bc-3b93-bb99-f64570603f39',
                    'info_URL': 'https://notebooks-tst.cloud.cesnet.cz/hub/oauth_login',
                    'service_name': 'Notebooks GUI Redirection',
                    'site_name': 'CESNET',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test File Sync and Share',
                'hostname': 'ocis-testing.apps.bst2-test.paas.psnc.pl_cd201cab-7792-3024-b1ed-96c48fec1e43',
                'service': 'eu.eosc.storage.gui',
                'tags': {
                    'hostname': 'ocis-testing.apps.bst2-test.paas.psnc.pl',
                    'info_ID': 'cd201cab-7792-3024-b1ed-96c48fec1e43',
                    'info_URL': 'https://ocis-testing.apps.bst2-test.paas.psnc.pl/',
                    'service_name': 'OwnCloud GUI',
                    'site_name': 'PSNC',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test File Sync and Share',
                'hostname': 'ocis-testing.apps.bst2-test.paas.psnc.pl_26d1d2a9-0e55-31c1-b83d-1678db3ebeaa',
                'service': 'eu.eosc.storage.gui',
                'tags': {
                    'hostname': 'ocis-testing.apps.bst2-test.paas.psnc.pl',
                    'info_ID': '26d1d2a9-0e55-31c1-b83d-1678db3ebeaa',
                    'info_URL': 'https://ocis-testing.apps.bst2-test.paas.psnc.pl/',
                    'service_name': 'OwnCloud GUI Redirect',
                    'site_name': 'PSNC',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test-HPC',
                'hostname': 'www.hpc.grnet.gr_45b9f71a-742d-3176-86c8-523e258bbc7c',
                'service': 'eu.eosc.hpc.access',
                'tags': {
                    'hostname': 'www.hpc.grnet.gr',
                    'info_ID': '45b9f71a-742d-3176-86c8-523e258bbc7c',
                    'info_URL': 'https://www.hpc.grnet.gr/en/',
                    'service_name': 'HPC API',
                    'site_name': 'GRNET',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test Cloud Container Platform',
                'hostname': 'console.apps.dcw1-test.paas.psnc.pl_ff7d6732-d073-34c1-9d4d-7d76e7b7b134',
                'service': 'eu.eosc.container_platform.gui',
                'tags': {
                    'hostname': 'console.apps.dcw1-test.paas.psnc.pl',
                    'info_ID': 'ff7d6732-d073-34c1-9d4d-7d76e7b7b134',
                    'info_URL': 'https://console.apps.dcw1-test.paas.psnc.pl/',
                    'service_name': 'OKD Console',
                    'site_name': 'PSNC',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test Cloud Container Platform',
                'hostname': 'api.dcw1-test.paas.psnc.pl_cdca6940-8a67-3015-97c8-da262b9b3d15',
                'service': 'eu.eosc.container_platform.api',
                'tags': {
                    'hostname': 'api.dcw1-test.paas.psnc.pl',
                    'info_ID': 'cdca6940-8a67-3015-97c8-da262b9b3d15',
                    'info_URL': 'https://api.dcw1-test.paas.psnc.pl:6443',
                    'service_name': 'OKD API',
                    'site_name': 'PSNC',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test Cloud Container Platform',
                'hostname': 'dcw1-test.paas.psnc.pl_931f9020-0ff6-3640-bc7c-2be26af5e917',
                'service': 'eu.eosc.container_platform.gui',
                'tags': {
                    'hostname': 'dcw1-test.paas.psnc.pl',
                    'info_ID': '931f9020-0ff6-3640-bc7c-2be26af5e917',
                    'info_URL': 'https://dcw1-test.paas.psnc.pl/',
                    'service_name': 'OKD Console GUI Redirection',
                    'site_name': 'PSNC',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test Cloud Container Platform',
                'hostname': 'oauth-openshift.apps.dcw1-test.paas.psnc.pl_26f6dd63-e34d-37e9-8f96-256621c6ae9a',
                'service': 'eu.eosc.container_platform.api',
                'tags': {
                    'hostname': 'oauth-openshift.apps.dcw1-test.paas.psnc.pl',
                    'info_ID': '26f6dd63-e34d-37e9-8f96-256621c6ae9a',
                    'info_URL': 'https://oauth-openshift.apps.dcw1-test.paas.psnc.pl/',
                    'service_name': 'OKD OIDC Server',
                    'site_name': 'PSNC',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test Cloud Container Platform',
                'hostname': 'console-openshift-console.apps.okd01.saft.in_ff7d6732-d073-34c1-9d4d-7d76e7b7b134',
                'service': 'eu.eosc.container_platform.gui',
                'tags': {
                    'hostname': 'console-openshift-console.apps.okd01.saft.in',
                    'info_ID': 'ff7d6732-d073-34c1-9d4d-7d76e7b7b134',
                    'info_URL': 'https://console-openshift-console.apps.okd01.saft.in/',
                    'service_name': 'OKD Console',
                    'site_name': 'Safespring',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test Cloud Container Platform',
                'hostname': 'api.okd01.saft.in_cdca6940-8a67-3015-97c8-da262b9b3d15',
                'service': 'eu.eosc.container_platform.api',
                'tags': {
                    'hostname': 'api.okd01.saft.in',
                    'info_ID': 'cdca6940-8a67-3015-97c8-da262b9b3d15',
                    'info_URL': 'https://api.okd01.saft.in:6443',
                    'service_name': 'OKD API',
                    'site_name': 'Safespring',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test Cloud Container Platform',
                'hostname': 'console-openshift-console.apps.eosc-test.safedc.services_931f9020-0ff6-3640-bc7c-2be26af5e917',
                'service': 'eu.eosc.container_platform.gui',
                'tags': {
                    'hostname': 'console-openshift-console.apps.eosc-test.safedc.services',
                    'info_ID': '931f9020-0ff6-3640-bc7c-2be26af5e917',
                    'info_URL': 'https://console-openshift-console.apps.eosc-test.safedc.services/',
                    'service_name': 'OKD Console GUI Redirection',
                    'site_name': 'Safespring',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'Test Cloud Container Platform',
                'hostname': 'oauth-openshift.apps.eosc-test.safedc.services_26f6dd63-e34d-37e9-8f96-256621c6ae9a',
                'service': 'eu.eosc.container_platform.api',
                'tags': {
                    'hostname': 'oauth-openshift.apps.eosc-test.safedc.services',
                    'info_ID': '26f6dd63-e34d-37e9-8f96-256621c6ae9a',
                    'info_URL': 'https://oauth-openshift.apps.eosc-test.safedc.services/',
                    'service_name': 'OKD OIDC Server',
                    'site_name': 'Safespring',
                    'tier': 2
                },
                'type': 'SERVICEGROUPS'
            }
        ])

    def test_FailedParseLot1ScTopology(self):
        with self.assertRaises(ConnectorParseError) as cm:
            lot1sc_topo = ParseLot1ScEndpoints('FAILED_DATA', 'FAILED_DATA', False)
            self.group_groups = lot1sc_topo.get_group_groups()
            self.group_endpoints = lot1sc_topo.get_group_endpoints()
        excep = cm.exception
        self.assertTrue('JSONDecodeError' in excep.msg)


if __name__ == '__main__':
    unittest.main()
