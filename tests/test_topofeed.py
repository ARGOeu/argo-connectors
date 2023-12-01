import unittest

from argo_connectors.log import Logger
from argo_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites
from argo_connectors.parse.flat_topology import ParseFlatEndpoints
from argo_connectors.parse.provider_topology import ParseTopo, ParseExtensions, buildmap_id2groupname
from argo_connectors.parse.agora_topology import ParseAgoraTopo
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.mesh.contacts import attach_contacts_topodata

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
        self.maxDiff = None

        parse_service_endpoints_ext = ParseServiceEndpoints(logger, self.content, 'CUSTOMERFOO', uid=True, pass_extensions=True)
        self.group_endpoints_ext = parse_service_endpoints_ext.get_group_endpoints()

    def test_LenEndpoints(self):
        self.assertEqual(len(self.group_endpoints), 4) # Parsed correct number of endpoint groups

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
            ParseServiceEndpoints(logger, '', 'CUSTOMERFOO', uid=True, pass_extensions=True)
        excep = cm.exception
        self.assertTrue('endpoint feed' in excep.msg)
        self.assertTrue('XMLSyntaxError' in excep.msg)


class MeshSitesAndContacts(unittest.TestCase):
    def setUp(self):
        logger.customer = CUSTOMER_NAME
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
        attach_contacts_topodata(logger, self.sample_sites_contacts,
                                 self.sample_sites_data,
                                 self.notification_flag)
        self.assertEqual(self.sample_sites_data[0],
            {
                'group': 'iris.ac.uk',
                'notifications': {'contacts': ['name1.surname1@durham.ac.uk',
                                               'name2.surname2@durham.ac.uk'],
                                  'enabled': False},
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

    def test_SitesAndContactsNoHonorNotificationFlag(self):
        attach_contacts_topodata(logger, self.sample_sites_contacts,
                                 self.sample_sites_data,
                                 False)
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
        attach_contacts_topodata(logger, self.sample_servicegroup_contacts,
                                 self.sample_servicegroups_data, self.notification_flag)
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

    def test_ServiceGroupsAndContactsNoHonorNotificationFlag(self):
        attach_contacts_topodata(logger, self.sample_servicegroup_contacts,
                                 self.sample_servicegroups_data, False)
        self.assertEqual(self.sample_servicegroups_data[1],
            {
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
            }
        )


class MeshServiceEndpointsAndContacts(unittest.TestCase):
    def setUp(self):
        logger.customer = CUSTOMER_NAME
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
        attach_contacts_topodata(logger, self.sample_serviceendpoints_contacts,
                                 self.sample_serviceendpoints_data, self.notfication_flag)
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
                                           uidservendp=True,
                                           fetchtype='ServiceGroups',
                                           scope=CUSTOMER_NAME, is_csv=True)
        self.maxDiff = None

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
                },
                {
                    'group': 'CUSTOMERFOO',
                    'subgroup': 'NEANIAS-Space',
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
            ]
        )

    def test_FailedCsvTopology(self):
        with self.assertRaises(ConnectorParseError) as cm:
            self.failed_topology = ParseFlatEndpoints(logger, 'FAILED_DATA',
                                                    CUSTOMER_NAME,
                                                    uidservendp=True,
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
                                           uidservendp=True,
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
            self.failed_topology = ParseFlatEndpoints(logger, 'FAILED_DATA', CUSTOMER_NAME,
                                                    uidservendp=True,
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
        self.notification_flag = False
        parse_sites = ParseSites(logger, self.content, CUSTOMER_NAME, False,
                                 False, self.notification_flag)
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


class ParseSitesTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-site.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        self.notification_flag = True
        parse_sites = ParseSites(logger, self.content, CUSTOMER_NAME, False,
                                 False, self.notification_flag)
        self.group_groups = parse_sites.get_group_groups()
        self.maxDiff = None

    def test_EgiSites(self):
        self.assertEqual(self.group_groups,
            [
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
                }
            ]
        )


class ParseEoscProvider(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-private-resource.json', encoding='utf-8') as feed_file:
            resources = feed_file.read()
        with open('tests/sample-private-provider.json', encoding='utf-8') as feed_file:
            providers = feed_file.read()
        with open('tests/sample-resourcefeed_extensions.json', encoding='utf-8') as feed_file:
            resource_extensions = feed_file.read()
        logger.customer = CUSTOMER_NAME
        eosc_topo = ParseTopo(logger, providers, resources, True, CUSTOMER_NAME)
        self.group_groups = eosc_topo.get_group_groups()
        self.group_endpoints = eosc_topo.get_group_endpoints()
        self.id_groupname = buildmap_id2groupname(self.group_endpoints)
        fakemap_idgroupnames = {
            'openaire.validator': 'OpenAIRE Validator',
            'srce.3dbionotes': '3DBionotes-WS-TEST',
            'srce.poem': 'POEM',
            'srce.srceweb': 'SRCE Web',
            'srce.webodv': 'WebODV - Online extraction, analysis and visualization of '
                            'SeaDataNet and Argo data'
        }
        eosc_topo_extensions = ParseExtensions(logger, resource_extensions, fakemap_idgroupnames, True, CUSTOMER_NAME)
        self.extensions = eosc_topo_extensions.get_extensions()
        self.maxDiff = None

    def test_groupGroups(self):
        self.assertEqual(self.group_groups, [
            {
                'group': 'srce',
                'subgroup': 'srce.3dbionotes',
                'tags': {
                    'info_projectname': 'SRCE'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'srce',
                'subgroup': 'srce.poem',
                'tags': {
                    'info_projectname': 'SRCE'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'srce',
                'subgroup': 'srce.srceweb',
                'tags': {
                    'info_projectname': 'SRCE'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'srce',
                'subgroup': 'srce.webodv',
                'tags': {
                    'info_projectname': 'SRCE'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'openaire',
                'subgroup': 'openaire.validator',
                'tags': {
                    'info_projectname': 'OpenAIRE',
                    'provider_tags': 'Open Science'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'riga_stradins_university',
                'subgroup': 'riga_stradins_university.rsu_dataverse',
                'tags': {
                    'info_projectname': 'RIga Stradins University',
                    'provider_tags': 'university, Latvia, Health, Medicine'
                },
                'type': 'PROJECT'
            }
        ])

    def test_meshContactsProviders(self):
        sample_resources_contacts = {
            '3dbionotes.cnb.csic.es+srce.3dbionotes': ['Emir.Imamagic@srce.hr']
        }

        attach_contacts_topodata(logger, sample_resources_contacts, self.group_endpoints)
        self.assertEqual(self.group_endpoints[0],
            {
                'group': 'srce.3dbionotes',
                'hostname': '3dbionotes.cnb.csic.es_srce.3dbionotes',
                'notifications': {
                    'contacts': ['Emir.Imamagic@srce.hr'],
                    'enabled': True
                },
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': '3dbionotes.cnb.csic.es',
                    'info_ID': 'srce.3dbionotes',
                    'info_URL': 'https://3dbionotes.cnb.csic.es/',
                    'info_groupname': '3DBionotes-WS-TEST'
                },
                'type': 'SERVICEGROUPS'
            }
        )

    def test_groupEndpoints(self):
        self.assertEqual(self.group_endpoints, [
            {
                'group': 'srce.3dbionotes',
                'hostname': '3dbionotes.cnb.csic.es_srce.3dbionotes',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': '3dbionotes.cnb.csic.es',
                    'info_ID': 'srce.3dbionotes',
                    'info_URL': 'https://3dbionotes.cnb.csic.es/',
                    'info_groupname': '3DBionotes-WS-TEST'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'srce.poem',
                'hostname': 'poem.devel.argo.grnet.gr_srce.poem',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'poem.devel.argo.grnet.gr',
                    'info_ID': 'srce.poem',
                    'info_URL': 'https://poem.devel.argo.grnet.gr',
                    'info_groupname': 'POEM'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'srce.srceweb',
                'hostname': 'www.srce.unizg.hr_srce.srceweb',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'www.srce.unizg.hr',
                    'info_ID': 'srce.srceweb',
                    'info_URL': 'https://www.srce.unizg.hr/',
                    'info_groupname': 'SRCE Web'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'srce.webodv',
                'hostname': 'webodv-egi-ace.cloud.ba.infn.it_srce.webodv',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'webodv-egi-ace.cloud.ba.infn.it',
                    'info_ID': 'srce.webodv',
                    'info_URL': 'http://webodv-egi-ace.cloud.ba.infn.it/',
                    'info_groupname': 'WebODV - Online extraction, analysis and '
                                        'visualization of SeaDataNet and Argo data'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'openaire.validator',
                'hostname': 'www.openaire.eu_openaire.validator',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'www.openaire.eu',
                    'info_ID': 'openaire.validator',
                    'info_URL': 'https://www.openaire.eu/validator',
                    'info_groupname': 'OpenAIRE Validator',
                    'service_tags': 'OAI-PMH protocol, horizontalService'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'riga_stradins_university.rsu_dataverse',
                'hostname': 'dataverse.rsu.lv_riga_stradins_university.rsu_dataverse',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'dataverse.rsu.lv',
                    'info_ID': 'riga_stradins_university.rsu_dataverse',
                    'info_URL': 'https://dataverse.rsu.lv/',
                    'info_groupname': 'RSU Dataverse',
                    'service_tags': 'e-INFRA non-commercial, horizontalService'
                },
                'type': 'SERVICEGROUPS'
            }
        ])

    def test_idGroupname(self):
        self.assertEqual(self.id_groupname, {
            'riga_stradins_university.rsu_dataverse': 'RSU Dataverse',
            'openaire.validator': 'OpenAIRE Validator',
            'srce.3dbionotes': '3DBionotes-WS-TEST',
            'srce.poem': 'POEM',
            'srce.srceweb': 'SRCE Web',
            'srce.webodv': 'WebODV - Online extraction, analysis and visualization of '
                            'SeaDataNet and Argo data'
        })

    def test_FailedEoscProviderTopology(self):
        logger.customer = CUSTOMER_NAME
        with self.assertRaises(ConnectorParseError) as cm:
            eosc_topo = ParseTopo(logger, 'FAILED_DATA', 'FAILED_DATA', True, CUSTOMER_NAME)
            self.group_groups = eosc_topo.get_group_groups()
            self.group_endpoints = eosc_topo.get_group_endpoints()
        excep = cm.exception
        self.assertTrue('JSON feed' in excep.msg)
        self.assertTrue('JSONDecodeError' in excep.msg)

    def test_serviceExtensions(self):
        self.assertEqual(self.extensions, [
            {
                'group': 'openaire.validator',
                'hostname': 'argo.grnet.gr_4429aede-129a-4a2d-9788-198a96912bc1',
                'service': 'eu.eosc.portal',
                'tags': {
                    'hostname': 'argo.grnet.gr',
                    'info_ID': '4429aede-129a-4a2d-9788-198a96912bc1',
                    'info_URL': 'argo.grnet.gr',
                    'info_groupname': 'OpenAIRE Validator',
                    'info_monitored_by': 'asdf'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'srce.poem',
                'hostname': 'eosc.poem.devel.argo.grnet.gr_c302082a-b0e3-4735-9e1f-b93053e4aa27',
                'service': 'eu.eosc.argo.poem',
                'tags': {
                    'hostname': 'eosc.poem.devel.argo.grnet.gr',
                    'info_ID': 'c302082a-b0e3-4735-9e1f-b93053e4aa27',
                    'info_URL': 'https://eosc.poem.devel.argo.grnet.gr',
                    'info_groupname': 'POEM',
                    'info_monitored_by': 'monitored_by-eosc'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'srce.poem',
                'hostname': 'eosc.poem.devel.argo.grnet.gr_c302082a-b0e3-4735-9e1f-b93053e4aa28_2fcf95f1-858b-311a-97aa-52d7e1fe66eb',
                'service': 'eu.eosc.argo.poem',
                'tags': {
                    'hostname': 'eosc.poem.devel.argo.grnet.gr',
                    'info_ID': 'c302082a-b0e3-4735-9e1f-b93053e4aa28_2fcf95f1-858b-311a-97aa-52d7e1fe66eb',
                    'info_URL': 'https://eosc.poem.devel.argo.grnet.gr/different/url/path',
                    'info_groupname': 'POEM',
                    'info_monitored_by': 'monitored_by-eosc'
                },
                'type': 'SERVICEGROUPS'
            }
        ])


class ParseAgoraTopology(unittest.TestCase):
    def setUp(self):
        with open('tests/agora_resource_sample.json', encoding='utf-8') as feed_file:
            resources = feed_file.read()
        with open('tests/agora_provider_sample.json', encoding='utf-8') as feed_file:
            providers = feed_file.read()
        logger.customer = CUSTOMER_NAME
        self.maxDiff = None
        agora_topo = ParseAgoraTopo(logger, providers, resources, False)
        self.group_groups = agora_topo.get_group_groups()
        self.group_endpoints = agora_topo.get_group_endpoints()

    def test_groupGroups(self):
        self.assertEqual(self.group_groups, [
                {
                "group": "NI4OS Providers",
                "type": "PROVIDERS",
                "subgroup": "UoB_IBISS",
                "tags": {
                    "info_ext_catalog_id": "02dc5b9a-99ba-4924-ab80-aa51b9c86b1e",
                    "info_ext_catalog_type": "provider",
                    "info_ext_catalog_url": "https://catalogue.ni4os.eu/?_=/providers/02dc5b9a-99ba-4924-ab80-aa51b9c86b1e",
                    "info_ext_name": "Institute for Biological Research Sinisa Stankovic, University of Belgrade"
                }
            },
            {
                "group": "NI4OS Providers",
                "type": "PROVIDERS",
                "subgroup": "UNIOS-EFOS",
                "tags": {
                    "info_ext_catalog_id": "0a6361a4-dfb4-4acd-af16-05b57c7a80d4",
                    "info_ext_catalog_type": "provider",
                    "info_ext_catalog_url": "https://catalogue.ni4os.eu/?_=/providers/0a6361a4-dfb4-4acd-af16-05b57c7a80d4",
                    "info_ext_name": "J.J. Strossmayer University of Osijek, Faculty of Economics in Osijek"
                    }
                }
            ]
        )

    def test_groupEndpoints(self):
        self.assertEqual(self.group_endpoints, [
                {
                "group": "UoB-RCUB",
                "type": "SERVICEGROUPS",
                "service": "catalog.service.entry",
                "hostname": "agora.ni4os.eu_uob_nardus",
                "tags": {
                    "hostname": "agora.ni4os.eu",
                    "info_ID": "uob_nardus",
                    "info_ext_catalog_id": "01426fe3-8783-47f2-97e6-757bcd70e1be",
                    "info_ext_catalog_type": "resource",
                    "info_ext_catalog_url": "https://catalogue.ni4os.eu/?_=/resources/01426fe3-8783-47f2-97e6-757bcd70e1be",
                    "info_ext_name": "Repository of Faculty of Science, University of Zagreb"
                }
            },
            {
                "group": "SRCE",
                "type": "SERVICEGROUPS",
                "service": "catalog.service.entry",
                "hostname": "agora.ni4os.eu_uob_nardus",
                "tags": {
                    "hostname": "agora.ni4os.eu",
                    "info_ID": "uob_nardus",
                    "info_ext_catalog_id": "01426fe3-8783-47f2-97e6-757bcd70e1be",
                    "info_ext_catalog_type": "resource",
                    "info_ext_catalog_url": "https://catalogue.ni4os.eu/?_=/resources/01426fe3-8783-47f2-97e6-757bcd70e1be",
                    "info_ext_name": "Repository of Faculty of Science, University of Zagreb"
                }
            },
            {
                "group": "CING",
                "type": "SERVICEGROUPS",
                "service": "catalog.service.entry",
                "hostname": "agora.ni4os.eu_melgene_cy",
                "tags": {
                    "hostname": "agora.ni4os.eu",
                    "info_ID": "melgene_cy",
                    "info_ext_catalog_id": "04b06b6f-e3a1-490b-94ea-8a1ab0309213",
                    "info_ext_catalog_type": "resource",
                    "info_ext_catalog_url": "https://catalogue.ni4os.eu/?_=/resources/04b06b6f-e3a1-490b-94ea-8a1ab0309213",
                    "info_ext_name": "MelGene"
                }
            },
            {
                "group": "UoB_IBISS",
                "type": "SERVICEGROUPS",
                "service": "catalog.provider.entry",
                "hostname": "agora.ni4os.eu_uob_ibiss",
                "tags": {
                    "hostname": "agora.ni4os.eu",
                    "info_ID": "uob_ibiss",
                    "info_ext_catalog_id": "02dc5b9a-99ba-4924-ab80-aa51b9c86b1e",
                    "info_ext_catalog_type": "provider",
                    "info_ext_catalog_url": "https://catalogue.ni4os.eu/?_=/providers/02dc5b9a-99ba-4924-ab80-aa51b9c86b1e",
                    "info_ext_name": "Institute for Biological Research Sinisa Stankovic, University of Belgrade"
                }
            },
            {
                "group": "UNIOS-EFOS",
                "type": "SERVICEGROUPS",
                "service": "catalog.provider.entry",
                "hostname": "agora.ni4os.eu_unios-efos",
                "tags": {
                    "hostname": "agora.ni4os.eu",
                    "info_ID": "unios-efos",
                    "info_ext_catalog_id": "0a6361a4-dfb4-4acd-af16-05b57c7a80d4",
                    "info_ext_catalog_type": "provider",
                    "info_ext_catalog_url": "https://catalogue.ni4os.eu/?_=/providers/0a6361a4-dfb4-4acd-af16-05b57c7a80d4",
                    "info_ext_name": "J.J. Strossmayer University of Osijek, Faculty of Economics in Osijek"
                }
                }
            ]
        )

    def test_FailedParseAgoraTopology(self):
        with self.assertRaises(ConnectorParseError) as cm:
            agora_topo = ParseAgoraTopo(logger, 'FAILED_DATA', 'FAILED_DATA', False)
            self.group_groups = agora_topo.get_group_groups()
            self.group_endpoints = agora_topo.get_group_endpoints()
        excep = cm.exception
        self.assertTrue('Providers feed' in excep.msg)
        self.assertTrue('JSONDecodeError' in excep.msg)


if __name__ == '__main__':
    unittest.main()
