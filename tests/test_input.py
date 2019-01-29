import datetime
import httplib
import mock
import modules.config
import unittest2 as unittest

from bin.downtimes_gocdb_connector import GOCDBReader as DowntimesGOCDBReader
from bin.downtimes_gocdb_connector import main as downtimes_main
from bin.topology_gocdb_connector import GOCDBReader, TopoFilter
from bin.weights_vapor_connector import Vapor as VaporReader
from bin.poem_connector import PoemReader
from modules.log import Logger

class ConnectorSetup(object):
    downtimes_feed = \
        """<?xml version="1.0" encoding="UTF-8"?>\n<results>\n
           <DOWNTIME ID="22154" PRIMARY_KEY="100728G0" CLASSIFICATION="SCHEDULED">\n
               <PRIMARY_KEY>100728G0</PRIMARY_KEY>\n
               <HOSTNAME>nagios.c4.csir.co.za</HOSTNAME>\n
               <SERVICE_TYPE>ngi.SAM</SERVICE_TYPE>\n
               <ENDPOINT>nagios.c4.csir.co.zangi.SAM</ENDPOINT>\n
               <HOSTED_BY>ZA-MERAKA</HOSTED_BY>\n
               <GOCDB_PORTAL_URL>https://goc.egi.eu/portal/index.php?Page_Type=Downtime&amp;id=22154</GOCDB_PORTAL_URL>\n
               <AFFECTED_ENDPOINTS/>\n
               <SEVERITY>OUTAGE</SEVERITY>\n
               <DESCRIPTION>Preparation for decommissioning of the service.</DESCRIPTION>\n
               <INSERT_DATE>1481808624</INSERT_DATE>\n
               <START_DATE>1482105600</START_DATE>\n
               <END_DATE>1488240000</END_DATE>\n
               <FORMATED_START_DATE>2016-12-19 00:00</FORMATED_START_DATE>\n
               <FORMATED_END_DATE>2017-02-28 00:00</FORMATED_END_DATE>\n
           </DOWNTIME>\n
           <DOWNTIME ID="22209" PRIMARY_KEY="100784G0" CLASSIFICATION="SCHEDULED">\n
               <PRIMARY_KEY>100784G0</PRIMARY_KEY>\n
               <HOSTNAME>ce1.grid.lebedev.ru</HOSTNAME>\n
               <SERVICE_TYPE>CE</SERVICE_TYPE>\n
               <ENDPOINT>ce1.grid.lebedev.ruCE</ENDPOINT>\n
               <HOSTED_BY>ru-Moscow-FIAN-LCG2</HOSTED_BY>\n
               <GOCDB_PORTAL_URL>https://goc.egi.eu/portal/index.php?Page_Type=Downtime&amp;id=22209</GOCDB_PORTAL_URL>\n
               <AFFECTED_ENDPOINTS/>\n
               <SEVERITY>OUTAGE</SEVERITY>\n
               <DESCRIPTION>Problems with hosting room (hack for ATLAS site status script that does not currently handle site status and works only on DTs)</DESCRIPTION>\n
               <INSERT_DATE>1482748113</INSERT_DATE>\n
               <START_DATE>1482882540</START_DATE>\n
               <END_DATE>1485215940</END_DATE>\n
               <FORMATED_START_DATE>2016-12-27 23:49</FORMATED_START_DATE>\n
               <FORMATED_END_DATE>2017-01-23 23:59</FORMATED_END_DATE>\n
           </DOWNTIME>\n
           <DOWNTIME ID="22209" PRIMARY_KEY="100784G0" CLASSIFICATION="SCHEDULED">\n
               <PRIMARY_KEY>100784G0</PRIMARY_KEY>\n
               <HOSTNAME>ce1.grid.lebedev.ru</HOSTNAME>\n
               <SERVICE_TYPE>APEL</SERVICE_TYPE>\n
               <ENDPOINT>ce1.grid.lebedev.ruAPEL</ENDPOINT>\n
               <HOSTED_BY>ru-Moscow-FIAN-LCG2</HOSTED_BY>\n
               <GOCDB_PORTAL_URL>https://goc.egi.eu/portal/index.php?Page_Type=Downtime&amp;id=22209</GOCDB_PORTAL_URL>\n
               <AFFECTED_ENDPOINTS/>\n
               <SEVERITY>OUTAGE</SEVERITY>\n
               <DESCRIPTION>Problems with hosting room (hack for ATLAS site status script that does not currently handle site status and works only on DTs)</DESCRIPTION>\n
               <INSERT_DATE>1482748113</INSERT_DATE>\n
               <START_DATE>1482882540</START_DATE>\n
               <END_DATE>1485215940</END_DATE>\n
               <FORMATED_START_DATE>2016-12-27 23:49</FORMATED_START_DATE>\n
               <FORMATED_END_DATE>2017-01-23 23:59</FORMATED_END_DATE>\n
           </DOWNTIME>\n
           </results>\n"""

    weights_feed = \
         """[ {"ngi": "NGI_FRANCE", "site":
                    [
                        { "codeCountry":"FR",
                            "country":"france",
                            "description":"Grille au service de la Recherche en Ile de France",
                            "latitude":"48.699262",
                            "longitude":"2.170686",
                            "ngi":"NGI_FRANCE",
                            "subregioncode":"155",
                            "url":"http://grif.fr/",
                            "id":"GRIF-LLR",
                            "Tier":"NA",
                            "SI2000":"0",
                            "specint2000":"0",
                            "HEPSPEC2006":"0",
                            "ExecutionEnvironmentLogicalCPUs":"0",
                            "ExecutionEnvironmentPhysicalCPUs":"0",
                            "ExecutionEnvironmentTotalInstances":"0",
                            "ComputingManagerLogicalCPUs":"0",
                            "ComputingManagerPhysicalCPUs":"0",
                            "ComputationPower":"0",
                            "WaitingJobs":"0",
                            "RunningJobs":"410",
                            "TotalJobs":"410",
                            "DataStoreTotalSize":"0",
                            "DataStoreFreeSize":"0",
                            "DataStoreUsedSize":"0",
                            "ServiceOnlineTotalSize":"1537617",
                            "ServiceOnlineFreeSize":"103181",
                            "ServiceOnlineUsedSize":"1412339",
                            "ServiceNearlineTotalSize":"0",
                            "ServiceNearlineFreeSize":"0",
                            "ServiceNearlineUsedSize":"0",
                            "TotalSize":"1537615",
                            "FreeSize":"125269",
                            "UsedSize":"1412344",
                            "TotalSizeOnline":"1537615",
                            "FreeSizeOnline":"125269",
                            "UsedSizeOnline":"1412344",
                            "TotalSizeNearline":"0",
                            "FreeSizeNearline":"0",
                            "UsedSizeNearline":"0"
                        }
                    ]
                },
                {"ngi": "NGI_FRANCE", "site":
                    [
                        { "id":"IN2P3-IRES",
                            "description":"EGI Site",
                            "url":"http://www.iphc.cnrs.fr/",
                            "Tier":"2",
                            "ngi":"NGI_FRANCE",
                            "country":"france",
                            "codeCountry":"FR",
                            "longitude":"7.7095",
                            "latitude":"48.6056",
                            "logCpu":"2420",
                            "PhyCpu":"272",
                            "subregioncode":"155",
                            "SI2000":"3142",
                            "specint2000":"3142",
                            "HEPSPEC2006":"13",
                            "ExecutionEnvironmentLogicalCPUs":"2420",
                            "ExecutionEnvironmentPhysicalCPUs":"272",
                            "ExecutionEnvironmentTotalInstances":"302",
                            "ComputingManagerLogicalCPUs":"0",
                            "ComputingManagerPhysicalCPUs":"0",
                            "ComputationPower":"30414.559999999998",
                            "WaitingJobs":"691",
                            "RunningJobs":"431",
                            "TotalJobs":"1122",
                            "DataStoreTotalSize":"0",
                            "DataStoreFreeSize":"0",
                            "DataStoreUsedSize":"0",
                            "ServiceOnlineTotalSize":"1616089",
                            "ServiceOnlineFreeSize":"255735",
                            "ServiceOnlineUsedSize":"1360353",
                            "ServiceNearlineTotalSize":"0",
                            "ServiceNearlineFreeSize":"0",
                            "ServiceNearlineUsedSize":"0",
                            "TotalSize":"1604423",
                            "FreeSize":"255752",
                            "UsedSize":"1348671",
                            "TotalSizeOnline":"1604423",
                            "FreeSizeOnline":"255752",
                            "UsedSizeOnline":"1348671",
                            "TotalSizeNearline":"0",
                            "FreeSizeNearline":"0",
                            "UsedSizeNearline":"0"
                        }
                    ]
                },
                {"ngi": "NGI_DE", "site":
                    [
                        { "id":"FZK-LCG2",
                            "description":"DE-KIT, Forschungszentrum Karlsruhe (FZK), GridKa",
                            "url":"http://www.gridka.de/",
                            "Tier":"1",
                            "ngi":"NGI_DE",
                            "country":"germany",
                            "codeCountry":"DE",
                            "longitude":"8.4321",
                            "latitude":"49.0963",
                            "logCpu":"20964",
                            "PhyCpu":"0",
                            "subregioncode":"155",
                            "SI2000":"0",
                            "specint2000":"0",
                            "HEPSPEC2006":"0",
                            "ExecutionEnvironmentLogicalCPUs":"0",
                            "ExecutionEnvironmentPhysicalCPUs":"0",
                            "ExecutionEnvironmentTotalInstances":"0",
                            "ComputingManagerLogicalCPUs":"20964",
                            "ComputingManagerPhysicalCPUs":"0",
                            "ComputationPower":"0",
                            "WaitingJobs":"12158",
                            "RunningJobs":"-239724",
                            "TotalJobs":"-227464",
                            "DataStoreTotalSize":"0",
                            "DataStoreFreeSize":"0",
                            "DataStoreUsedSize":"0",
                            "ServiceOnlineTotalSize":"0",
                            "ServiceOnlineFreeSize":"0",
                            "ServiceOnlineUsedSize":"0",
                            "ServiceNearlineTotalSize":"0",
                            "ServiceNearlineFreeSize":"0",
                            "ServiceNearlineUsedSize":"0",
                            "TotalSize":"0",
                            "FreeSize":"0",
                            "UsedSize":"0",
                            "TotalSizeOnline":"0",
                            "FreeSizeOnline":"0",
                            "UsedSizeOnline":"0",
                            "TotalSizeNearline":"0",
                            "FreeSizeNearline":"0",
                            "UsedSizeNearline":"0"
                        }
                    ]
                }
            ]"""

    group_groups_feed = \
        """<?xml version="1.0" encoding="UTF-8"?>
           <results>
           <SITE ID="40" PRIMARY_KEY="73G0" NAME="TU-Kosice">
               <PRIMARY_KEY>73G0</PRIMARY_KEY>
               <SHORT_NAME>TU-Kosice</SHORT_NAME>
               <OFFICIAL_NAME>Technical University of Kosice</OFFICIAL_NAME>
               <GOCDB_PORTAL_URL>https://goc.egi.eu/portal/index.php?Page_Type=Site&amp;id=40</GOCDB_PORTAL_URL>
               <HOME_URL>http://www.tuke.sk</HOME_URL>
               <CONTACT_EMAIL>grid-adm@lists.tuke.sk</CONTACT_EMAIL>
               <CONTACT_TEL>+421 55 602 5123</CONTACT_TEL>
               <GIIS_URL>ldap://mon.grid.tuke.sk:2170/Mds-Vo-name=TU-Kosice,o=grid</GIIS_URL>
               <COUNTRY_CODE>SK</COUNTRY_CODE>
               <COUNTRY>Slovakia</COUNTRY>
               <ROC>NGI_SK</ROC>
               <PRODUCTION_INFRASTRUCTURE>Production</PRODUCTION_INFRASTRUCTURE>
               <CERTIFICATION_STATUS>Certified</CERTIFICATION_STATUS>
               <TIMEZONE>Europe/Bratislava</TIMEZONE>
               <LATITUDE>48.73</LATITUDE>
               <LONGITUDE>21.24</LONGITUDE>
               <CSIRT_EMAIL>grid-adm@lists.tuke.sk</CSIRT_EMAIL>
               <NOTIFICATIONS>FALSE</NOTIFICATIONS>
               <DOMAIN>
               <DOMAIN_NAME>tuke.sk</DOMAIN_NAME>
               </DOMAIN>
               <SITE_IP>0.0.0.0/255.255.255.255</SITE_IP>
               <SCOPES>
               <SCOPE>EGI</SCOPE>
               </SCOPES>
               <EXTENSIONS/>
           </SITE>
           <SITE ID="41" PRIMARY_KEY="201G0" NAME="IISAS-Bratislava">
               <PRIMARY_KEY>201G0</PRIMARY_KEY>
               <SHORT_NAME>IISAS-Bratislava</SHORT_NAME>
               <OFFICIAL_NAME>Institute of Informatics, Slovak Academy of Sciences, Bratislava</OFFICIAL_NAME>
               <SITE_DESCRIPTION>IISAS Bratislava production EGI site</SITE_DESCRIPTION>
               <GOCDB_PORTAL_URL>https://goc.egi.eu/portal/index.php?Page_Type=Site&amp;id=41</GOCDB_PORTAL_URL>
               <CONTACT_EMAIL>grid-admin.ui@sav.sk</CONTACT_EMAIL>
               <CONTACT_TEL>+421-2-5941-1289</CONTACT_TEL>
               <GIIS_URL>ldap://sbdii.ui.savba.sk:2170/Mds-Vo-name=IISAS-Bratislava,o=grid</GIIS_URL>
               <COUNTRY_CODE>SK</COUNTRY_CODE>
               <COUNTRY>Slovakia</COUNTRY>
               <ROC>NGI_SK</ROC>
               <PRODUCTION_INFRASTRUCTURE>Production</PRODUCTION_INFRASTRUCTURE>
               <CERTIFICATION_STATUS>Certified</CERTIFICATION_STATUS>
               <TIMEZONE>Europe/Bratislava</TIMEZONE>
               <LATITUDE>48.17</LATITUDE>
               <LONGITUDE>17.07</LONGITUDE>
               <CSIRT_EMAIL>grid-admin.ui@sav.sk</CSIRT_EMAIL>
               <NOTIFICATIONS>FALSE</NOTIFICATIONS>
               <DOMAIN>
               <DOMAIN_NAME>ui.savba.sk</DOMAIN_NAME>
               </DOMAIN>
               <SITE_IP>0.0.0.0/255.255.255.255</SITE_IP>
               <SCOPES>
               <SCOPE>EGI</SCOPE>
               </SCOPES>
               <EXTENSIONS/>
           </SITE>
           <SITE ID="42" PRIMARY_KEY="8G0" NAME="prague_cesnet_lcg2_cert">
               <PRIMARY_KEY>8G0</PRIMARY_KEY>
               <SHORT_NAME>prague_cesnet_lcg2_cert</SHORT_NAME>
               <OFFICIAL_NAME>Prague Cesnet</OFFICIAL_NAME>
               <GOCDB_PORTAL_URL>https://goc.egi.eu/portal/index.php?Page_Type=Site&amp;id=42</GOCDB_PORTAL_URL>
               <HOME_URL>www.cesnet.cz</HOME_URL>
               <CONTACT_EMAIL>lcg-admin@fzu.cz</CONTACT_EMAIL>
               <CONTACT_TEL>+420 2 6605 2145</CONTACT_TEL>
               <GIIS_URL> ldap://skurut16.cesnet.cz:2170/mds-vo-name=prague_cesnet_lcg2_cert,o=grid</GIIS_URL>
               <COUNTRY_CODE>CZ</COUNTRY_CODE>
               <COUNTRY>Czech Republic</COUNTRY>
               <ROC>NGI_CZ</ROC>
               <PRODUCTION_INFRASTRUCTURE>Production</PRODUCTION_INFRASTRUCTURE>
               <CERTIFICATION_STATUS>Closed</CERTIFICATION_STATUS>
               <TIMEZONE>Europe/Prague</TIMEZONE>
               <CSIRT_EMAIL>lcg-admin@fzu.cz</CSIRT_EMAIL>
               <NOTIFICATIONS>FALSE</NOTIFICATIONS>
               <DOMAIN>
               <DOMAIN_NAME>cesnet.cz</DOMAIN_NAME>
               </DOMAIN>
               <SITE_IP>0.0.0.0/255.255.255.255</SITE_IP>
               <SCOPES>
               <SCOPE>EGI</SCOPE>
               </SCOPES>
               <EXTENSIONS/>
               </SITE>
           </results>
        """

    group_endpoints_feed = \
        """<?xml version="1.0" encoding="UTF-8"?>\n
           <results>\n
           <SERVICE_ENDPOINT PRIMARY_KEY="4497G0">\n
               <PRIMARY_KEY>8253G0</PRIMARY_KEY>\n
               <HOSTNAME>occi-api.100percentit.com</HOSTNAME>\n
               <GOCDB_PORTAL_URL>https://goc.egi.eu/portal/index.php?Page_Type=Service&amp;id=8253</GOCDB_PORTAL_URL>\n
               <BETA>N</BETA>\n
               <SERVICE_TYPE>eu.egi.cloud.vm-management.occi</SERVICE_TYPE>\n
               <CORE/>\n
               <IN_PRODUCTION>Y</IN_PRODUCTION>\n
               <NODE_MONITORED>Y</NODE_MONITORED>\n
               <SITENAME>100IT</SITENAME>\n
               <COUNTRY_NAME>United Kingdom</COUNTRY_NAME>\n
               <COUNTRY_CODE>GB</COUNTRY_CODE>\n
               <ROC_NAME>NGI_UK</ROC_NAME>\n
               <URL>https://occi-api.100percentit.com:8787/occi1.1/?image=53d9172f-599f-4340-86a2-a52b425f80a3&amp;platform=openstack&amp;resource=1</URL>\n
               <ENDPOINTS/>\n
               <SCOPES>\n
                   <SCOPE>EGI</SCOPE>\n
                   <SCOPE>FedCloud</SCOPE>\n
               </SCOPES>\n
               <EXTENSIONS/>\n
           </SERVICE_ENDPOINT>\n
           <SERVICE_ENDPOINT PRIMARY_KEY="4495G0">\n
               <PRIMARY_KEY>4495G0</PRIMARY_KEY>\n
               <HOSTNAME>egi-cloud-accounting.100percentit.com</HOSTNAME>\n
               <GOCDB_PORTAL_URL>https://goc.egi.eu/portal/index.php?Page_Type=Service&amp;id=4495</GOCDB_PORTAL_URL>\n
               <BETA>N</BETA>\n
               <SERVICE_TYPE>eu.egi.cloud.accounting</SERVICE_TYPE>\n
               <CORE/>\n
               <IN_PRODUCTION>Y</IN_PRODUCTION>\n
               <NODE_MONITORED>Y</NODE_MONITORED>\n
               <SITENAME>100IT</SITENAME>\n
               <COUNTRY_NAME>United Kingdom</COUNTRY_NAME>\n
               <COUNTRY_CODE>GB</COUNTRY_CODE>\n
               <ROC_NAME>NGI_UK</ROC_NAME>\n
               <URL>100IT</URL>\n
               <ENDPOINTS/>\n
               <SCOPES>\n
                   <SCOPE>EGI</SCOPE>\n
                   <SCOPE>FedCloud</SCOPE>\n
               </SCOPES>\n
               <EXTENSIONS/>\n
           </SERVICE_ENDPOINT>\n
           <SERVICE_ENDPOINT PRIMARY_KEY="4588G0">\n
               <PRIMARY_KEY>4588G0</PRIMARY_KEY>\n
               <HOSTNAME>occi-api.100percentit.com</HOSTNAME>\n
               <GOCDB_PORTAL_URL>https://goc.egi.eu/portal/index.php?Page_Type=Service&amp;id=4588</GOCDB_PORTAL_URL>\n
               <BETA>N</BETA>\n
               <SERVICE_TYPE>eu.egi.cloud.information.bdii</SERVICE_TYPE>\n
               <CORE/>\n
               <IN_PRODUCTION>Y</IN_PRODUCTION>\n
               <NODE_MONITORED>Y</NODE_MONITORED>\n
               <SITENAME>100IT</SITENAME>\n
               <COUNTRY_NAME>United Kingdom</COUNTRY_NAME>\n
               <COUNTRY_CODE>GB</COUNTRY_CODE>\n
               <ROC_NAME>NGI_UK</ROC_NAME>\n
               <URL>ldap://site-bdii.100percentit.com:2170</URL>\n
               <ENDPOINTS/>\n
               <SCOPES>\n
                   <SCOPE>EGI</SCOPE>\n
                   <SCOPE>FedCloud</SCOPE>\n
               </SCOPES>\n
               <EXTENSIONS/>\n
           </SERVICE_ENDPOINT>\n
           </results>\n"""

    poem_feed = """
                 {"name": "FEDCLOUD", 
                  "description": "Profile for Fedcloud CentOS 7 instance",
                  "vo": "ops", 
                  "metric_instances": 
                      [
                          {"service_flavour": "eu.egi.cloud.vm-management.occi", 
                           "metric": "eu.egi.cloud.OCCI-AppDB-Sync"}, 
                          {"service_flavour": "eu.egi.cloud.vm-management.occi",
                           "metric": "eu.egi.cloud.OCCI-Categories"}, 
                          {"service_flavour": "eu.egi.cloud.vm-management.occi", 
                           "metric": "eu.egi.cloud.OCCI-Context"}, 
                          {"service_flavour": "eu.egi.cloud.vm-management.occi", 
                           "metric": "eu.egi.cloud.OCCI-VM-OIDC"}, 
                          {"service_flavour": "org.openstack.nova", 
                           "metric": "eu.egi.cloud.OpenStack-VM-OIDC"}, 
                          {"service_flavour": "org.openstack.nova", 
                           "metric": "eu.egi.cloud.OpenStack-VM-VOMS-OIDC"}
                      ]
                  }
       """

    erroneous_poem_feed = """
                 {"name": "FEDCLOUD", 
                  "description": "Profile for Fedcloud CentOS 7 instance",
                  "vo": "ops", 
                  "metrics": 
                      [
                          {"service_flavour": "eu.egi.cloud.vm-management.occi", 
                           "metric": "eu.egi.cloud.OCCI-AppDB-Sync"}, 
                          {"service_flavour": "eu.egi.cloud.vm-management.occi",
                           "metric": "eu.egi.cloud.OCCI-Categories"}, 
                          {"service_flavour": "eu.egi.cloud.vm-management.occi", 
                           "metric": "eu.egi.cloud.OCCI-Context"}, 
                          {"service_flavour": "eu.egi.cloud.vm-management.occi", 
                           "metric": "eu.egi.cloud.OCCI-VM-OIDC"}, 
                          {"service_flavour": "org.openstack.nova", 
                           "metric": "eu.egi.cloud.OpenStack-VM-OIDC"}, 
                          {"service_flavour": "org.openstack.nova", 
                           "metric": "eu.egi.cloud.OpenStack-VM-VOMS-OIDC"}
                      ]
                  }
       """


    poem = [{'metric': u'eu.egi.cloud.OCCI-AppDB-Sync',
             'profile': u'ch.cern.SAM.FEDCLOUD',
             'service': u'eu.egi.cloud.vm-management.occi',
             'vo': 'ops'},
            {'metric': u'eu.egi.cloud.OCCI-Categories',
             'profile': u'ch.cern.SAM.FEDCLOUD',
             'service': u'eu.egi.cloud.vm-management.occi',
             'vo': 'ops'},
            {'metric': u'eu.egi.cloud.OCCI-Context',
             'profile': u'ch.cern.SAM.FEDCLOUD',
             'service': u'eu.egi.cloud.vm-management.occi',
             'vo': 'ops'},
            {'metric': u'eu.egi.cloud.OCCI-VM-OIDC',
             'profile': u'ch.cern.SAM.FEDCLOUD',
             'service': u'eu.egi.cloud.vm-management.occi',
             'vo': 'ops'},
            {'metric': u'eu.egi.cloud.OpenStack-VM-OIDC',
             'profile': u'ch.cern.SAM.FEDCLOUD',
             'service': u'org.openstack.nova',
             'vo': 'ops'},
            {'metric': u'eu.egi.cloud.OpenStack-VM-VOMS-OIDC',
             'profile': u'ch.cern.SAM.FEDCLOUD',
             'service': u'org.openstack.nova',
             'vo': 'ops'}]

    downtimes = [{'end_time': '2017-01-19T23:59:00Z',
                  'hostname': u'nagios.c4.csir.co.za',
                  'service': u'ngi.SAM',
                  'start_time': '2017-01-19T00:00:00Z'},
                 {'end_time': '2017-01-19T23:59:00Z',
                  'hostname': u'ce1.grid.lebedev.ru',
                  'service': u'CE', 'start_time':
                  '2017-01-19T00:00:00Z'},
                 {'end_time': '2017-01-19T23:59:00Z',
                  'hostname': u'ce1.grid.lebedev.ru',
                  'service': u'APEL',
                  'start_time': '2017-01-19T00:00:00Z'}]

    weights = {u'FZK-LCG2': u'0', u'IN2P3-IRES': u'30414.559999999998', u'GRIF-LLR': u'0'}

    group_groups = [{'group': u'NGI_SK', 'subgroup': u'IISAS-Bratislava',
                     'tags': {'certification': u'Certified',
                              'infrastructure': u'Production',
                              'scope': 'EGI'},
                     'type': 'NGI'},
                    {'group': u'NGI_SK', 'subgroup': u'TU-Kosice',
                     'tags': {'certification': u'Certified',
                              'infrastructure': u'Production',
                              'scope': 'EGI'},
                     'type': 'NGI'},
                    {'group': u'NGI_CZ', 'subgroup': u'prague_cesnet_lcg2_cert',
                     'tags': {'certification': u'Closed',
                              'infrastructure': u'Production',
                              'scope': 'EGI'},
                     'type': 'NGI'}]

    group_groups_servicegroup_filter = [
        {'group': 'EGI', 'subgroup': u'NGI_CH_SERVICES',
         'tags': {'monitored': '1', 'scope': 'EGI'},
         'type': 'PROJECT'},
        {'group': 'EGI', 'subgroup': u'SLA_TEST_B',
         'tags': {'monitored': '1', 'scope': 'EGI'},
         'type': 'PROJECT'},
        {'group': 'EGI', 'subgroup': u'NGI_HU_SERVICES',
         'tags': {'monitored': '1', 'scope': 'EGI'},
         'type': 'PROJECT'},
        {'group': 'EGI', 'subgroup': u'SLA_TEST',
         'tags': {'monitored': '1', 'scope': 'EGI'},
         'type': 'PROJECT'},
    ]

    group_groups_sites_filter = [
        {"group": "NGI_HR", "tags": {"scope": "EGI", "infrastructure":
                                     "Production", "certification":
                                     "Certified"},
         "type": "NGI", "subgroup": "egee.irb.hr"},
        {"group": "NGI_HR", "tags": {"scope": "EGI", "infrastructure":
                                     "Production", "certification":
                                     "Certified"},
         "type": "NGI", "subgroup": "egee.srce.hr"}
    ]

    group_endpoints_sites_filter = [
        {"group": "egee.irb.hr", "hostname": "lorienmaster.irb.hr",
         "type": "SITES", "service": "Site-BDII",
         "tags": {"scope": "EGI", "production": "1", "monitored": "1"}},
        {"group": "egee.srce.hr", "hostname": "se.srce.egi.cro-ngi.hr",
         "type": "SITES", "service": "gLite-APEL", "tags": {"scope": "EGI",
                                                    "production": "1",
                                                    "monitored": "1"}}
    ]

    group_endpoints = [{'group': u'100IT',
                        'hostname': u'occi-api.100percentit.com',
                        'service': u'eu.egi.cloud.vm-management.occi',
                        'tags': {'monitored': '1',
                                 'production': '1',
                                 'scope': 'EGI'},
                        'type': 'SITES'},
                        {'group': u'100IT',
                        'hostname': u'egi-cloud-accounting.100percentit.com',
                        'service': u'eu.egi.cloud.accounting',
                        'tags': {'monitored': '1',
                                 'production': '1',
                                 'scope': 'EGI'},
                        'type': 'SITES'},
                        {'group': u'100IT',
                        'hostname': u'occi-api.100percentit.com',
                        'service': u'eu.egi.cloud.information.bdii',
                        'tags': {'monitored': '1',
                                 'production': '1',
                                 'scope': 'EGI'},
                        'type': 'SITES'}]

    group_endpoints_servicegroup_filter = [
        {'group': u'SLA_TEST_B',
         'group_monitored': u'Y',
         'hostname': u'snf-189278.vm.okeanos.grnet.gr',
         'service': u'eu.egi.MPI',
         'tags': {'monitored': '1', 'production': '1', 'scope': 'EGI'},
         'type': 'SERVICEGROUPS'},
        {'group': u'SLA_TEST',
         'group_monitored': u'Y',
         'hostname': u'se01.marie.hellasgrid.gr',
         'service': u'SRM',
         'tags': {'monitored': '1', 'production': '1', 'scope': 'EGI'},
         'type': 'SERVICEGROUPS'},
        {'group': u'NGI_HU_SERVICES',
         'group_monitored': u'Y',
         'hostname': u'grid153.kfki.hu',
         'service': u'MyProxy',
         'tags': {'monitored': '1', 'production': '1', 'scope': 'EGI'},
         'type': 'SERVICEGROUPS'},
        {'group': u'NGI_HU_SERVICES',
         'group_monitored': u'Y',
         'hostname': u'grid146.kfki.hu',
         'service': u'ngi.ARGUS',
         'tags': {'monitored': '1', 'production': '1', 'scope': 'EGI'},
         'type': 'SERVICEGROUPS'}
    ]

    def __init__(self, connector, gconf, cconf):
        self.globalconfig = modules.config.Global(connector, gconf)
        self.customerconfig = modules.config.CustomerConf(connector, cconf)
        self.globopts = self.globalconfig.parse()
        self.customerconfig.parse()
        customers = self.customerconfig.get_customers()
        self.custname = self.customerconfig.get_custname(customers[0])
        self.jobs = self.customerconfig.get_jobs(customers[0])
        self.jobdir = self.customerconfig.get_fulldir(customers[0], self.jobs[0])

class TopologyXml(unittest.TestCase):
    def setUp(self):
        self.connset = ConnectorSetup('topology-gocdb-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts',
                  'group_endpoints', 'group_groups', 'group_endpoints_feed',
                  'group_groups_feed', 'group_groups_servicegroup_filter',
                  'group_endpoints_servicegroup_filter',
                  'group_endpoints_sites_filter', 'group_groups_sites_filter']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

        feedjobs = self.customerconfig.get_mapfeedjobs('topology-gocdb-connector.py',
                                                       'GOCDB',
                                                       deffeed='https://localhost/gocdbpi/')
        feed = feedjobs.keys()[0]
        jobcust = feedjobs.values()[0]
        scopes = self.customerconfig.get_feedscopes(feed, jobcust)
        self.gocdbreader = GOCDBReader(feed, scopes)
        self.orig_get_xmldata = self.gocdbreader._get_xmldata
        self.gocdbreader._get_xmldata = self.wrap_get_xmldata

    def wrap_get_xmldata(self, scope, pi):
        globopts = self.globalconfig.parse()
        self.orig_get_xmldata.im_func.func_globals['globopts'] = globopts
        self.orig_get_xmldata.im_func.func_globals['input'].connection.func = self.mock_conn
        return self.orig_get_xmldata(scope, pi)

    @mock.patch('modules.input.connection')
    def testServiceEndpoints(self, mock_conn):
        servicelist = dict()
        mock_conn.__name__ = 'mock_conn'
        mock_conn.return_value = self.group_endpoints_feed
        self.mock_conn = mock_conn
        self.gocdbreader.getServiceEndpoints(servicelist, '&scope=EGI')
        self.gocdbreader.serviceListEGI = servicelist
        self.gocdbreader.getGroupOfEndpoints.im_func.func_globals['fetchtype'] = 'SITES'
        sge = sorted(self.group_endpoints, key=lambda e: e['service'])
        obj_sge = sorted(self.gocdbreader.getGroupOfEndpoints(),
                         key=lambda e: e['service'])
        self.assertEqual(sge, obj_sge)

    @mock.patch('modules.input.connection')
    def testSites(self, mock_conn):
        siteslist = dict()
        mock_conn.__name__ = 'mock_conn'
        mock_conn.return_value = self.group_groups_feed
        self.mock_conn = mock_conn
        self.gocdbreader.getSitesInternal(siteslist, '&scope=EGI')
        self.gocdbreader.siteListEGI = siteslist
        self.gocdbreader.getGroupOfGroups.im_func.func_globals['fetchtype'] = 'SITES'
        sgg = sorted(self.group_groups, key=lambda e: e['subgroup'])
        obj_sgg = sorted(self.gocdbreader.getGroupOfGroups(),
                         key=lambda e: e['subgroup'])
        self.assertEqual(sgg, obj_sgg)

    def testTopoFilter(self):
        groupfilter = {'Monitored': 'Y',
                       'Scope': 'EGI',
                       'ServiceGroup': 'SLA_TEST'}
        subgroupfilter = {'Monitored': 'Y',
                          'Production': 'Y',
                          'Scope': 'EGI'}
        tf = TopoFilter(self.group_groups_servicegroup_filter,
                        self.group_endpoints_servicegroup_filter,
                        groupfilter,
                        subgroupfilter)
        self.assertEqual(tf.gg, [{'group': 'EGI', 'subgroup': u'SLA_TEST',
                                  'tags': {'monitored': '1', 'scope': 'EGI'},
                                  'type': 'PROJECT'}])
        self.assertEqual(tf.ge, [{'group': u'SLA_TEST',
                                  'group_monitored': 'Y',
                                  'hostname': 'se01.marie.hellasgrid.gr',
                                  'service': 'SRM',
                                  'tags': {'monitored': '1',
                                           'production': '1',
                                           'scope': 'EGI'},
                                  'type': 'SERVICEGROUPS'}])
        groupfilter = {'Infrastructure': 'Production',
                       'Scope': 'EGI',
                       'Certification': 'Certified',
                       'Site': 'egee.srce.hr'}
        subgroupfilter = {'Monitored': 'Y',
                          'Production': 'Y',
                          'Scope': 'EGI'}
        tf = TopoFilter(self.group_groups_sites_filter,
                        self.group_endpoints_sites_filter,
                        groupfilter,
                        subgroupfilter)
        self.assertEqual(tf.gg, [{'group': 'NGI_HR', 'subgroup': 'egee.srce.hr',
                                  'tags': {'certification': 'Certified',
                                           'infrastructure': 'Production',
                                           'scope': 'EGI'},
                                  'type': 'NGI'}])
        self.assertEqual(tf.ge, [{'group': 'egee.srce.hr',
                                  'hostname': 'se.srce.egi.cro-ngi.hr',
                                  'service': 'gLite-APEL',
                                  'tags': {'monitored': '1',
                                           'production': '1',
                                           'scope': 'EGI'},
                                  'type': 'SITES'}])


class WeightsJson(unittest.TestCase):
    def setUp(self):
        self.connset = ConnectorSetup('weights-vapor-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts', 'jobs',
                  'jobdir', 'weights', 'weights_feed']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

    def wrap_get_weights(self, mock_conn):
        logger = Logger('weights-vapor-connector.py')
        logger.customer = 'EGI'
        logger.job = self.jobs[0]
        self.orig_get_weights.im_func.func_globals['globopts'] = self.globopts
        self.orig_get_weights.im_func.func_globals['input'].connection.func = mock_conn
        self.orig_get_weights.im_func.func_globals['logger'] = logger
        return self.orig_get_weights()

    @mock.patch('modules.input.connection')
    def testJson(self, mock_conn):
        feeds = self.customerconfig.get_mapfeedjobs('weights-vapor-connector.py',
                                                    deffeed= 'https://operations-portal.egi.eu/vapor/downloadLavoisier/option/json/view/VAPOR_Ngi_Sites_Info')
        vapor = VaporReader(feeds.keys()[0])
        datestamp = datetime.datetime.strptime('2017-01-19', '%Y-%m-%d')
        self.orig_get_weights = vapor.getWeights
        mock_conn.__name__ = 'mock_conn'
        mock_conn.return_value = 'Erroneous JSON feed'
        vapor.getWeights = self.wrap_get_weights
        self.assertEqual(vapor.getWeights(mock_conn), [])

        mock_conn.return_value = self.weights_feed
        vapor.getWeights = self.wrap_get_weights
        self.assertEqual(vapor.getWeights(mock_conn), self.weights)

class DowntimesXml(unittest.TestCase):
    def setUp(self):
        self.connset = ConnectorSetup('downtimes-gocdb-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts', 'jobs',
                  'jobdir', 'downtimes', 'downtimes_feed']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

    def wrap_get_downtimes(self, start, end, mock_conn):
        logger = Logger('downtimes-gocdb-connector.py')
        logger.customer = 'EGI'
        logger.job = self.jobs[0]
        self.orig_get_downtimes.im_func.func_globals['globopts'] = self.globopts
        self.orig_get_downtimes.im_func.func_globals['input'].connection.func = mock_conn
        self.orig_get_downtimes.im_func.func_globals['logger'] = logger
        return self.orig_get_downtimes(start, end)

    @mock.patch('modules.helpers.time.sleep')
    @mock.patch('modules.input.connection')
    def testRetryConnection(self, mock_conn, mock_sleep):
        feeds = self.customerconfig.get_mapfeedjobs('downtimes-gocdb-connector.py', deffeed='https://goc.egi.eu/gocdbpi/')
        gocdb = DowntimesGOCDBReader(feeds.keys()[0])
        datestamp = datetime.datetime.strptime('2017-01-19', '%Y-%m-%d')
        start = datestamp.replace(hour=0, minute=0, second=0)
        end = datestamp.replace(hour=23, minute=59, second=59)
        self.orig_get_downtimes = gocdb.getDowntimes
        gocdb.getDowntimes = self.wrap_get_downtimes
        mock_sleep.return_value = True
        mock_conn.__name__ = 'mock_conn'
        mock_conn.side_effect = [httplib.HTTPException('Bogus'),
                                 httplib.HTTPException('Bogus'),
                                 httplib.HTTPException('Bogus')]
        self.assertEqual(gocdb.getDowntimes(start, end, mock_conn), [])
        self.assertEqual(mock_conn.call_count, int(self.globopts['ConnectionRetry'.lower()]) + 1)
        self.assertTrue(mock_sleep.called)
        self.assertEqual(mock_sleep.call_count, int(self.globopts['ConnectionRetry'.lower()]))
        sleepretry = int(self.globopts['ConnectionSleepRetry'.lower()])
        self.assertEqual(mock_sleep.call_args_list, [mock.call(sleepretry),
                                                     mock.call(sleepretry),
                                                     mock.call(sleepretry)])

    @mock.patch('modules.input.connection')
    def testXml(self, mock_conn):
        feeds = self.customerconfig.get_mapfeedjobs('downtimes-gocdb-connector.py', deffeed='https://goc.egi.eu/gocdbpi/')
        gocdb = DowntimesGOCDBReader(feeds.keys()[0])
        datestamp = datetime.datetime.strptime('2017-01-19', '%Y-%m-%d')
        start = datestamp.replace(hour=0, minute=0, second=0)
        end = datestamp.replace(hour=23, minute=59, second=59)
        self.orig_get_downtimes = gocdb.getDowntimes
        mock_conn.__name__ = 'mock_conn'
        mock_conn.return_value = 'Erroneous XML feed'
        gocdb.getDowntimes = self.wrap_get_downtimes
        self.assertEqual(gocdb.getDowntimes(start, end, mock_conn), [])
        mock_conn.return_value = self.downtimes_feed
        gocdb.getDowntimes = self.wrap_get_downtimes
        self.assertEqual(gocdb.getDowntimes(start, end, mock_conn), self.downtimes)

    @mock.patch('bin.downtimes_gocdb_connector.sys')
    @mock.patch('modules.output.write_state')
    @mock.patch('bin.downtimes_gocdb_connector.CustomerConf', autospec=True)
    @mock.patch('bin.downtimes_gocdb_connector.argparse.ArgumentParser.parse_args')
    @mock.patch('bin.downtimes_gocdb_connector.Global')
    @mock.patch('bin.downtimes_gocdb_connector.GOCDBReader')
    def testStateFile(self, gocdbreader, glob, parse_args, customerconf, write_state, mock_sys):
        argmock = mock.Mock()
        argmock.date = ['2017-01-19']
        argmock.gloconf = ['tests/global.conf']
        argmock.custconf = ['tests/customer.conf']
        parse_args.return_value = argmock
        customerconf.get_mapfeedjobs.return_value = self.customerconfig.get_mapfeedjobs('downtimes-gocdb-connector.py',
                                                                                        deffeed='https://goc.egi.eu/gocdbpi/')
        customerconf.get_fullstatedir.side_effect = ['/var/lib/argo-connectors/states//EGI/EGI_Critical', '/var/lib/argo-connectors/states//EGI/EGI_Cloudmon', '/var/lib/argo-connectors/states//EGI/EGI_Critical', '/var/lib/argo-connectors/states//EGI/EGI_Cloudmon']
        self.globopts['generalwriteavro'] = 'False'
        self.globopts['generalpublishams'] = 'False'
        mock_sys.argv = ['downtimes-gocdb-connector.py']
        downtimes_main.func_globals['output'].write_state = write_state
        customerconf.side_effect = [customerconf, customerconf]
        gocdbreader.side_effect = [gocdbreader, gocdbreader]
        glob.side_effect = [glob, glob]
        glob.is_complete.return_value = (True, [])

        gocdbreader.state = True
        gocdbreader.getDowntimes.return_value = self.downtimes
        glob.parse.return_value = self.globopts
        downtimes_main()
        self.assertTrue(write_state.called)
        self.assertEqual(write_state.call_count, len(self.jobs))
        for call in write_state.call_args_list:
            self.assertTrue(gocdbreader.state in call[0])
            self.assertTrue('2017_01_19' in call[0])

        gocdbreader.state = False
        gocdbreader.getDowntimes.return_value = []
        downtimes_main()
        self.assertTrue(write_state.called)
        self.assertEqual(write_state.call_count, 2*len(self.jobs))
        for call in write_state.call_args_list[2:]:
            self.assertTrue(gocdbreader.state in call[0])
            self.assertTrue('2017_01_19' in call[0])

class PoemJson(unittest.TestCase):
    def setUp(self):
        self.connset = ConnectorSetup('poem-connector.py',
                                      'tests/global.conf',
                                      'tests/customer.conf')
        for c in ['globalconfig', 'customerconfig', 'globopts', 'jobs',
                  'poem_feed', 'erroneous_poem_feed', 'poem']:
            code = """self.%s = self.connset.%s""" % (c, c)
            exec code

        self.poemreader = PoemReader('EGI', self.jobs[1])
        self.poemreader.state = True
        self.orig_loadProfilesFromServer = self.poemreader.loadProfilesFromServer
        self.poemreader.loadProfilesFromServer = self.wrap_loadProfilesFromServer

    def wrap_loadProfilesFromServer(self, server, vo, namespace, profiles):
        logger = Logger('poem-connector.py')
        logger.customer = 'EGI'
        logger.job = self.jobs[1]
        self.orig_loadProfilesFromServer.im_func.func_globals['globopts'] = self.globopts
        self.orig_loadProfilesFromServer.im_func.func_globals['input'].connection.func = self.mock_conn
        self.orig_loadProfilesFromServer.im_func.func_globals['logger'] = logger
        return self.orig_loadProfilesFromServer(server, vo, namespace, profiles)

    @mock.patch('modules.input.connection')
    def testgetProfiles(self, mock_conn):
        profiles = self.customerconfig.get_profiles(self.jobs[1])
        namespace = self.customerconfig.get_namespace(self.jobs[1])
        server = {self.customerconfig.get_poemserver_host(self.jobs[1]):
                      self.customerconfig.get_poemserver_vo(self.jobs[1])}
        mock_conn.__name__ = 'mock_conn'
        mock_conn.return_value = 'Erroneous JSON feed'
        self.mock_conn = mock_conn
        with self.assertRaises(SystemExit) as cm:
            self.poemreader.getProfiles(profiles, namespace, server)
            self.assertEqual(cm.exception.code, 1)
        self.assertFalse(self.poemreader.state)

        mock_conn.return_value = self.erroneous_poem_feed
        self.mock_conn = mock_conn
        self.poemreader.state = True
        self.assertEqual(self.poemreader.getProfiles(profiles,  namespace,
                                                     server), [])
        self.assertFalse(self.poemreader.state)

        mock_conn.return_value = self.poem_feed
        self.mock_conn = mock_conn
        self.poemreader.state = True
        self.assertEqual(self.poemreader.getProfiles(profiles, namespace, server), self.poem)
        self.assertTrue(self.poemreader.state)

        self.assertEqual(self.poemreader.getProfiles(profiles + ['ARGO_MON_CRITICAL'], namespace, server), self.poem)
        self.assertFalse(self.poemreader.state)

        self.poemreader.state = True
        with self.assertRaises(SystemExit) as cm:
            self.poemreader.getProfiles(['ARGO_MON_CRITICAL'], namespace, server)
            self.assertEqual(cm.exception.code, 1)
        self.assertFalse(self.poemreader.state)

if __name__ == '__main__':
    unittest.main()
