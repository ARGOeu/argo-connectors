import unittest

from argo_connectors.log import Logger
from argo_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites
from argo_connectors.parse.flat_topology import ParseFlatEndpoints
from argo_connectors.parse.provider_topology import ParseTopo, ParseExtensions, ParseResourcesExtras, buildmap_id2groupname
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
                    'scope': 'EGI'
                },
                'type': 'SITES'},
            {
                'group': 'GSI-LCG2',
                'hostname': 'grid13.gsi.de_14G0',
                'service': 'CE',
                'tags': {
                    'info_ID': '14G0',
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
        self.sample_servicegroup_contacts = {
            'NGI_ARMGRID_SERVICES': ['Name1.Surname1@email.com', 'Name2.Surname2@email.com'],
            'NGI_CYGRID_SERVICES': ['Name3.Surname3@email.com', 'Name4.Surname4@email.com'],
        }

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
        self.sample_serviceendpoints_contacts = {
            'fqdn1.com+service1': ['Name1.Surname1@email.com', 'Name2.Surname2@email.com'],
            'fqdn2.com+service2': ['Name3.Surname3@email.com', 'Name4.Surname4@email.com']
        }

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


class ParseEoscProvider(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-resourcefeed_eoscprovider_eudat.json', encoding='utf-8') as feed_file:
            resources = feed_file.read()
        with open('tests/sample-providerfeed_eoscprovider_eudat.json', encoding='utf-8') as feed_file:
            providers = feed_file.read()
        with open('tests/sample-resourcefeed_extensions.json', encoding='utf-8') as feed_file:
            resource_extensions = feed_file.read()
        logger.customer = CUSTOMER_NAME
        eosc_topo = ParseTopo(logger, providers, resources, True, CUSTOMER_NAME)
        self.group_groups = eosc_topo.get_group_groups()
        self.group_endpoints = eosc_topo.get_group_endpoints()
        self.id_groupname = buildmap_id2groupname(self.group_endpoints)
        fakemap_idgroupnames = {
            'grnet.grnet-test': 'grnet-test',
            'openaire.validator': 'OpenAIRE Validator',
            'openaire.zenodo': 'Zenodo',
            'openaire.amnesia': 'AMNESIA',
            'grnet.hpc__national_hpc_infrastructure': 'HPC | National HPC Infrastructure'
        }
        eosc_topo_extensions = ParseExtensions(logger, resource_extensions, fakemap_idgroupnames, True, CUSTOMER_NAME)
        self.extensions = eosc_topo_extensions.get_extensions()
        self.maxDiff = None

    def test_groupGroups(self):
        self.assertEqual(self.group_groups, [
            {
                'group': 'eudat',
                'subgroup': 'eudat.b2access',
                'tags': {
                    'info_projectname': 'EUDAT',
                    'provider_tags': 'Data Infrastructure, European Data Initiative'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'eudat',
                'subgroup': 'eudat.b2note',
                'tags': {
                    'info_projectname': 'EUDAT',
                    'provider_tags': 'Data Infrastructure, European Data Initiative'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'eudat',
                'subgroup': 'eudat.b2share',
                'tags': {
                    'info_projectname': 'EUDAT',
                    'provider_tags': 'Data Infrastructure, European Data Initiative'
                },
                'type': 'PROJECT'},
            {
                'group': 'eudat',
                'subgroup': 'eudat.b2drop',
                'tags': {
                    'info_projectname': 'EUDAT',
                    'provider_tags': 'Data Infrastructure, European Data Initiative'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'eudat',
                'subgroup': 'eudat.b2safe',
                'tags': {
                    'info_projectname': 'EUDAT',
                    'provider_tags': 'Data Infrastructure, European Data Initiative'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'eudat',
                'subgroup': 'eudat.b2find',
                'tags': {
                    'info_projectname': 'EUDAT',
                    'provider_tags': 'Data Infrastructure, European Data Initiative'
                },
                'type': 'PROJECT'
            }
        ])

    def test_meshContactsProviders(self):
        sample_resources_contacts = {
            'www.eudat.eu+eudat.b2access': ['helpdesk@eudat.eu']
        }

        attach_contacts_topodata(logger, sample_resources_contacts, self.group_endpoints)
        self.assertEqual(self.group_endpoints[0],
            {
                'group': 'eudat.b2access',
                'hostname': 'www.eudat.eu_eudat.b2access',
                'notifications': {
                    'contacts': ['helpdesk@eudat.eu'], 'enabled': True
                },
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'www.eudat.eu',
                    'info_ID': 'eudat.b2access',
                    'info_groupname': 'B2ACCESS',
                    'info_URL': 'https://www.eudat.eu/services/b2access',
                    'service_tags': 'single sign-on, federated identity management, federated AAI proxy'
                },
                'type': 'SERVICEGROUPS'
            }
        )

    def test_groupEndoints(self):
        self.assertEqual(self.group_endpoints,[
            {
                'group': 'eudat.b2access',
                'hostname': 'www.eudat.eu_eudat.b2access',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'www.eudat.eu',
                    'info_ID': 'eudat.b2access',
                    'info_groupname': 'B2ACCESS',
                    'info_URL': 'https://www.eudat.eu/services/b2access',
                    'service_tags': 'single sign-on, federated identity management, federated '
                                'AAI proxy'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'eudat.b2note',
                'hostname': 'b2note.eudat.eu_eudat.b2note',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'b2note.eudat.eu',
                    'info_ID': 'eudat.b2note',
                    'info_groupname': 'B2NOTE',
                    'info_URL': 'https://b2note.eudat.eu',
                    'service_tags': 'annotation'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'eudat.b2share',
                'hostname': 'www.eudat.eu_eudat.b2share',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'www.eudat.eu',
                    'info_ID': 'eudat.b2share',
                    'info_groupname': 'B2SHARE',
                    'info_URL': 'https://www.eudat.eu/services/b2share',
                    'service_tags': 'data repository, data sharing, data publishing, FAIR'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'eudat.b2drop',
                'hostname': 'www.eudat.eu_eudat.b2drop',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'www.eudat.eu',
                    'info_ID': 'eudat.b2drop',
                    'info_groupname': 'B2DROP',
                    'info_URL': 'https://www.eudat.eu/services/b2drop',
                    'service_tags': 'sync and share'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'eudat.b2safe',
                'hostname': 'www.eudat.eu_eudat.b2safe',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'www.eudat.eu',
                    'info_ID': 'eudat.b2safe',
                    'info_groupname': 'B2SAFE',
                    'info_URL': 'https://www.eudat.eu/services/b2safe',
                    'service_tags': 'replication, Policy-based data management, persistent identifiers, data archiving'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'eudat.b2find',
                'hostname': 'www.eudat.eu_eudat.b2find',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'www.eudat.eu',
                    'info_ID': 'eudat.b2find',
                    'info_groupname': 'B2FIND',
                    'info_URL': 'https://www.eudat.eu/services/b2find',
                    'service_tags': 'metadata, search, harvesting, interdisciplinary, discovery'
                },
                'type': 'SERVICEGROUPS'
            }
        ])

    def test_idGroupname(self):
        self.assertEqual(self.id_groupname, {
            'eudat.b2access': 'B2ACCESS',
            'eudat.b2drop': 'B2DROP',
            'eudat.b2find': 'B2FIND',
            'eudat.b2note': 'B2NOTE',
            'eudat.b2safe': 'B2SAFE',
            'eudat.b2share': 'B2SHARE'
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
                'group': 'grnet.grnet-test',
                'hostname': 'srce.hr_367752f8-a1e8-45d0-9e2d-fcae3c2fc0e2',
                'service': 'eu.eosc.generic.https',
                'tags': {
                    'hostname': 'srce.hr',
                    'info_ID': '367752f8-a1e8-45d0-9e2d-fcae3c2fc0e2',
                    'info_URL': 'https://srce.hr',
                    'info_groupname': 'grnet-test',
                    'info_monitored_by': 'eosc'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'grnet.grnet-test',
                'hostname': 'grnet.gr_367752f8-a1e8-45d0-9e2d-fcae3c2fc0e2',
                'service': 'eu.eosc.generic.https',
                'tags': {
                    'hostname': 'grnet.gr',
                    'info_ID': '367752f8-a1e8-45d0-9e2d-fcae3c2fc0e2',
                    'info_URL': 'https://grnet.gr',
                    'info_groupname': 'grnet-test',
                    'info_monitored_by': 'eosc'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'grnet.grnet-test',
                'hostname': 'kathimerini.gr_367752f8-a1e8-45d0-9e2d-fcae3c2fc0e2',
                'service': 'eu.eosc.generic.https',
                'tags': {
                    'hostname': 'kathimerini.gr',
                    'info_ID': '367752f8-a1e8-45d0-9e2d-fcae3c2fc0e2',
                    'info_URL': 'https://kathimerini.gr',
                    'info_groupname': 'grnet-test',
                    'info_monitored_by': 'eosc'
                },
                'type': 'SERVICEGROUPS'
            },
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
                'group': 'openaire.zenodo',
                'hostname': 'some.endpoint.zenodo.com_5ce1854d-a4e0-4ec3-adc9-3e09d42945a5',
                'service': 'eu.eosc.ckan',
                'tags': {
                    'hostname': 'some.endpoint.zenodo.com',
                    'info_ID': '5ce1854d-a4e0-4ec3-adc9-3e09d42945a5',
                    'info_URL': 'some.endpoint.zenodo.com',
                    'info_groupname': 'Zenodo',
                    'info_monitored_by': 'string'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'openaire.amnesia',
                'hostname': 'argo.grnet.gr_0920c959-ea0c-412b-a282-dd97e7c594fc',
                'service': 'eu.eosc.portal',
                'tags': {
                    'hostname': 'argo.grnet.gr',
                    'info_ID': '0920c959-ea0c-412b-a282-dd97e7c594fc',
                    'info_URL': 'argo.grnet.gr',
                    'info_groupname': 'AMNESIA',
                    'info_monitored_by': 'asdf'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'grnet.hpc__national_hpc_infrastructure',
                'hostname': 'hpc.grnet.gr_18afc30d-2f78-4417-8b11-315ea1611ad7',
                'service': 'eu.eosc.portal',
                'tags': {
                    'hostname': 'hpc.grnet.gr',
                    'info_ID': '18afc30d-2f78-4417-8b11-315ea1611ad7',
                    'info_URL': 'https://hpc.grnet.gr/',
                    'info_groupname': 'HPC | National HPC Infrastructure',
                    'info_monitored_by': 'monitored_by-eosc'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'grnet.hpc__national_hpc_infrastructure',
                'hostname': 'hpc.grnet.gr_18afc30d-2f78-4417-8b11-315ea1611ad7_d59ff23a-1ed8-39a3-949f-b1b1b82547d0',
                'service': 'eu.eosc.portal',
                'tags': {
                    'hostname': 'hpc.grnet.gr',
                    'info_ID': '18afc30d-2f78-4417-8b11-315ea1611ad7_d59ff23a-1ed8-39a3-949f-b1b1b82547d0',
                    'info_URL': 'https://hpc.grnet.gr/some/path',
                    'info_groupname': 'HPC | National HPC Infrastructure',
                    'info_monitored_by': 'monitored_by-eosc'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'grnet.hpc__national_hpc_infrastructure',
                'hostname': 'hpc.grnet.gr_18afc30d-2f78-4417-8b11-315ea1611ad7_d002662b-7924-3906-b845-c3c2ebf33e5a',
                'service': 'eu.eosc.portal',
                'tags': {
                    'hostname': 'hpc.grnet.gr',
                    'info_ID': '18afc30d-2f78-4417-8b11-315ea1611ad7_d002662b-7924-3906-b845-c3c2ebf33e5a',
                    'info_URL': 'https://hpc.grnet.gr/some/path/another',
                    'info_groupname': 'HPC | National HPC Infrastructure',
                    'info_monitored_by': 'monitored_by-eosc'
                },
                'type': 'SERVICEGROUPS'
            }
        ])


class ParseEoscProviderExtras(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-resourcefeed_extras.json', encoding='utf-8') as feed_file:
            resources = feed_file.read()
        with open('tests/sample-providerfeed_extras.json', encoding='utf-8') as feed_file:
            providers = feed_file.read()
        resources = ParseResourcesExtras(logger, resources, ['horizontalService'], CUSTOMER_NAME).data
        logger.customer = CUSTOMER_NAME
        eosc_topo = ParseTopo(logger, providers, resources, True, CUSTOMER_NAME)
        self.group_groups = eosc_topo.get_group_groups()
        self.group_endpoints = eosc_topo.get_group_endpoints()

        self.maxDiff = None

    def test_groupGroups(self):
        self.assertEqual(self.group_groups, [
            {
                'group': 'provider_0310_1',
                'subgroup': 'provider_0310_1.resource_provider_0310_1_upate',
                'tags': {
                    'info_projectname': 'Provider 03.10 1'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'aginfra',
                'subgroup': 'aginfra.chart_visualization',
                'tags': {
                    'info_projectname': 'AGINFRA+', 'provider_tags': 'Agri-food'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'aginfra',
                'subgroup': 'aginfra.agris_elastic_index',
                'tags': {
                    'info_projectname': 'AGINFRA+',
                    'provider_tags': 'Agri-food'
                },
                'type': 'PROJECT'
            },
            {
                'group': '3200-beta2',
                'subgroup': '3200-beta2._test_412',
                'tags': {
                    'info_projectname': '3.20.0-beta.2 update',
                    'provider_tags': 'tag'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'geant',
                'subgroup': 'geant.eosc_profile v4.0.0 test for eoscen-218 and eoscen-220',
                'tags': {
                    'info_projectname': 'GÉANT',
                    'provider_tags': 'e-infrastructure, Pan-European GÉANT network, '
                                    'Networks, Trust and Identity'
                },
                'type': 'PROJECT'
            }
        ])

    def test_groupEndpoints(self):
        self.assertEqual(self.group_endpoints, [
            {
                'group': 'provider_0310_1.resource_provider_0310_1_upate',
                'hostname': 'www.cyfronet.pl_provider_0310_1.resource_provider_0310_1_upate',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'www.cyfronet.pl',
                    'info_ID': 'provider_0310_1.resource_provider_0310_1_upate',
                    'info_URL': 'https://www.cyfronet.pl/zalacznik/8437',
                    'info_groupname': 'resource Provider 03.10 1 upate',
                    'service_tags': 'ee, horizontalService'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'aginfra.chart_visualization',
                'hostname': 'support.d4science.org_aginfra.chart_visualization',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'support.d4science.org',
                    'info_ID': 'aginfra.chart_visualization',
                    'info_URL': 'https://support.d4science.org/projects/aginfraplus_wiki/wiki/CHART_VIS',
                    'info_groupname': 'AGINFRA+ Chart Visualization',
                    'service_tags': 'visualization, chart, graph, line, mind map, '
                                    'scatter plot, spline, bar, pie, donought, horizontalService'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'aginfra.agris_elastic_index',
                'hostname': 'support.d4science.org_aginfra.agris_elastic_index',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'support.d4science.org',
                    'info_ID': 'aginfra.agris_elastic_index',
                    'info_URL': 'https://support.d4science.org/projects/aginfraplus_wiki/wiki/AGRIS_Elastic_Index',
                    'info_groupname': 'AGINFRA+ AGRIS Elastic Index',
                    'service_tags': 'Publication Discovery, Metadata, Search, horizontalService'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': '3200-beta2._test_412',
                'hostname': 'www.cyfronet.pl_3200-beta2._test_412',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'www.cyfronet.pl',
                    'info_ID': '3200-beta2._test_412',
                    'info_URL': 'https://www.cyfronet.pl/zalacznik/8437',
                    'info_groupname': 'test 4.12',
                    'service_tags': 'horizontalService'
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'geant.eosc_profile v4.0.0 test for eoscen-218 and eoscen-220',
                'hostname': 'wiki.geant.org_geant.eosc_profile v4.0.0 test for eoscen-218 '
                        'and eoscen-220',
                'service': 'eu.eosc.portal.services.url',
                'tags': {
                    'hostname': 'wiki.geant.org',
                    'info_ID': 'geant.eosc_profile v4.0.0 test for eoscen-218 and '
                                'eoscen-220',
                    'info_URL': 'https://wiki.geant.org/display/SYS/GEANT+Service+Catalogue+v3.00%3Ev4.00+compliance',
                    'info_groupname': 'EOSC Profile v4.0.0 Test for EOSCEN-218 and '
                                      'EOSCEN-220',
                    'service_tags': 'horizontalService'
                },
                'type': 'SERVICEGROUPS'
            }
        ])

    def test_FailedEoscResourcesExtrasTopology(self):
        logger.customer = CUSTOMER_NAME
        with self.assertRaises(ConnectorParseError) as cm:
            resources = ParseResourcesExtras(logger, 'FAILED_DATA', ['horizontalService'], CUSTOMER_NAME).data
        excep = cm.exception
        self.assertTrue('JSON feed' in excep.msg)
        self.assertTrue('JSONDecodeError' in excep.msg)


if __name__ == '__main__':
    unittest.main()
