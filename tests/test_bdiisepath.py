import unittest
import json

from argo_connectors.log import Logger
from argo_connectors.mesh.storage_element_path import attach_sepath_topodata

from bonsai import LDAPEntry


logger = Logger('test_contactfeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


class MeshSePathAndTopodata(unittest.TestCase):
    def setUp(self):
        logger.customer = CUSTOMER_NAME
        with open('tests/sample-bdii_sepaths.json') as fh:
            content = fh.read()
            self.sample_ldap = json.loads(content)
        self.maxDiff = None
        self.bdiiopts =  {
            'bdii': 'True', 'bdiihost': 'bdii.egi.cro-ngi.hr',
            'bdiiport': '2170',
            'bdiiqueryattributessepath': 'GlueVOInfoAccessControlBaseRule GlueVOInfoPath',
            'bdiiqueryattributessrm': 'GlueServiceEndpoint',
            'bdiiquerybase': 'o=grid',
            'bdiiqueryfiltersepath': '(objectClass=GlueSATop)',
            'bdiiqueryfiltersrm': '(&(objectClass=GlueService)(|(GlueServiceType=srm_v1)(GlueServiceType=srm)))'
        }


        self.sample_storage_endpoints = [
            {
                "type": "SITES", "group": "UKI-SCOTGRID-GLASGOW", "service":
                "eu.egi.storage.accounting", "hostname":
                "svr018.gla.scotgrid.ac.uk", "notifications": None, "tags":
                {
                    "scope": "EGI, wlcg, tier2, atlas, cms, lhcb",
                    "monitored": "1",
                    "production": "1"
                }
            },
            {
                "type": "SITES", "group": "CA-WATERLOO-T2", "service": "SRM",
                "hostname": "lcg-se1.uw.computecanada.ca", "notifications": None,
                "tags": {
                    "scope": "EGI",
                    "monitored": "1",
                    "production": "1",
                    "info_URL": "httpg://lcg-se1.uw.computecanada.ca:8443/srm/managerv2",
                    "info_SRM_port": "8443"
                }
            },
            {
                "type": "SITES", "group": "CA-SFU-T2", "service": "SRM",
                "hostname": "lcg-se1.sfu.computecanada.ca", "notifications": None,
                "tags": {"scope": "EGI", "monitored": "1", "production": "1",
                         "info_URL":
                         "httpg://lcg-se1.sfu.computecanada.ca:8443/srm/managerv2",
                         "info_SRM_port": "8443"
                }
            },
            {
                "type": "SITES", "group": "wuppertalprod", "service": "SRM",
                "hostname": "grid-se.physik.uni-wuppertal.de",
                "notifications": None,
                "tags": {"scope": "EGI, wlcg, tier2, atlas", "monitored": "1",
                         "production": "1", "info_id": "3077G0",
                         "info_SRM_port": "8443"
                }
            },
            {
                "type": "SITES",
                "group": "UKI-LT2-QMUL",
                "service": "SRM",
                "hostname": "se02.esc.qmul.ac.uk",
                "notifications": None,
                "tags": {"scope": "EGI, wlcg, tier2, atlas, lhcb", "monitored": "1",
                         "production": "0", "info_id": "8458G0",
                         "info_SRM_port": "8444"
                }
            }
        ]
        self._construct_ldap_entries()

    def _construct_ldap_entries(self):
        tmp = list()
        for entry in self.sample_ldap:
            new_entry = LDAPEntry(entry['dn'])
            for key, value in entry.items():
                if key == 'dn':
                    continue
                new_entry[key] = value
            tmp.append(new_entry)
        self.sample_ldap = tmp

    def test_meshSePathTopo(self):
        attach_sepath_topodata(logger, self.bdiiopts, self.sample_ldap, self.sample_storage_endpoints)
        self.assertEqual(self.sample_storage_endpoints, [
            {
                'group': 'UKI-SCOTGRID-GLASGOW',
                'hostname': 'svr018.gla.scotgrid.ac.uk',
                'notifications': None,
                'service': 'eu.egi.storage.accounting',
                'tags': {
                    'monitored': '1',
                    'production': '1',
                    'scope': 'EGI, wlcg, tier2, atlas, cms, lhcb',
                    'vo_ukqcd.vo.gridpp.ac.uk_attr_SE_PATH': '/dpm/gla.scotgrid.ac.uk/home/ukqcd.vo.gridpp.ac.uk'
                },
                'type': 'SITES'
            },
            {
                'group': 'CA-WATERLOO-T2',
                'hostname': 'lcg-se1.uw.computecanada.ca',
                'notifications': None,
                'service': 'SRM',
                'tags': {
                    'info_SRM_port': '8443',
                    'info_URL': 'httpg://lcg-se1.uw.computecanada.ca:8443/srm/managerv2',
                    'monitored': '1',
                    'production': '1',
                    'scope': 'EGI'
                },
                'type': 'SITES'},
            {
                'group': 'CA-SFU-T2',
                'hostname': 'lcg-se1.sfu.computecanada.ca',
                'notifications': None,
                'service': 'SRM',
                'tags': {
                    'info_SRM_port': '8443',
                    'info_URL': 'httpg://lcg-se1.sfu.computecanada.ca:8443/srm/managerv2',
                    'monitored': '1',
                    'production': '1',
                    'scope': 'EGI',
                    'vo_atlas_attr_SE_PATH': '/atlas'
                },
                'type': 'SITES'
            },
            {
                'group': 'wuppertalprod',
                'hostname': 'grid-se.physik.uni-wuppertal.de',
                'notifications': None,
                'service': 'SRM',
                'tags': {
                    'info_SRM_port': '8443',
                    'info_id': '3077G0',
                    'monitored': '1',
                    'production': '1',
                    'scope': 'EGI, wlcg, tier2, atlas',
                    'vo_dteam_attr_SE_PATH': '/pnfs/physik.uni-wuppertal.de/data/dteam',
                    'vo_ops_attr_SE_PATH': '/pnfs/physik.uni-wuppertal.de/data/ops'
                },
                'type': 'SITES'
            },
            {
                'group': 'UKI-LT2-QMUL',
                'hostname': 'se02.esc.qmul.ac.uk',
                'notifications': None,
                'service': 'SRM',
                'tags': {'info_SRM_port': '8444',
                    'info_id': '8458G0',
                    'monitored': '1',
                    'production': '0',
                    'scope': 'EGI, wlcg, tier2, atlas, lhcb',
                    'vo_dteam_attr_SE_PATH': '/info',
                    'vo_ops_attr_SE_PATH': '/info'
                },
                'type': 'SITES'
            }])


if __name__ == '__main__':
    unittest.main()
