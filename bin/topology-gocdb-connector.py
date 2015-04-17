#!/usr/bin/python

# Copyright (c) 2013 GRNET S.A., SRCE, IN2P3 CNRS Computing Centre
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS
# IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language
# governing permissions and limitations under the License.
#
# The views and conclusions contained in the software and
# documentation are those of the authors and should not be
# interpreted as representing official policies, either expressed
# or implied, of either GRNET S.A., SRCE or IN2P3 CNRS Computing
# Centre
#
# The work represented by this source file is partially funded by
# the EGI-InSPIRE project through the European Commission's 7th
# Framework Programme (contract # INFSO-RI-261323)

import urllib
import datetime
import xml.dom.minidom
import httplib
import sys

from argo_egi_connectors.writers import AvroWriter
from argo_egi_connectors.config import EGIConf, Global


LegMapServType = {'SRM' : 'SRMv2'}
fetchtype = ''

globopts = {}


class GOCDBReader:
    def __init__(self):
        self.gocdbHost = 'goc.egi.eu'
        self.gocdbUrl = 'https://' + self.gocdbHost + '/gocdbpi/'
        self.hostKey = globopts['AuthenticationHostKey']
        self.hostCert = globopts['AuthenticationHostCert']
        self.siteList = dict()
        self.serviceList = dict()
        self.groupList = dict()

    def getGroupOfServices(self):
        self.loadDataIfNeeded()

        groups = list()
        for key, group in self.groupList.iteritems():
            for service in group['services']:

                g = dict()
                g['type'] = fetchtype.upper()
                g['group'] = group['name']
                g['service'] = service['type']
                g['hostname'] = service['hostname']
                g['group_monitored'] = group['monitored']
                g['tags'] = {'scope' : group['scope'], \
                            'monitored' : 1 if service['monitored'] == "Y" else 0, \
                            'production' : 1 if service['production'] == "Y" else 0}
                groups.append(g)

        return groups

    def getGroupOfGroups(self):
        self.loadDataIfNeeded()

        groupofgroups = list()

        if fetchtype == "ServiceGroups":
            for key, value in self.groupList.iteritems():
                g = dict()
                g['type'] = 'PROJECT'
                g['group'] = 'EGI'
                g['subgroup'] = value['name']
                g['tags'] = {'monitored' : 1 if value['monitored'] == "Y" else 0, \
                            'scope' : value['scope']}
                groupofgroups.append(g)
        else:
            gg = sorted([value for key, value in self.siteList.iteritems()], \
                                    key=lambda s: s['ngi'])

            for gr in gg:
                g = dict()
                g['type'] = 'NGI'
                g['group'] = gr['ngi']
                g['subgroup'] = gr['site']
                g['tags'] = {'certification' : gr['certification'], \
                            'scope' : gr['scope'], \
                            'infrastructure' : gr['infrastructure']}
                groupofgroups.append(g)

        return groupofgroups

    def getGroupOfEndpoints(self):
        self.loadDataIfNeeded()

        groupofendpoints = list()
        ge = sorted([value for key, value in self.serviceList.iteritems()], \
                                 key=lambda s: s['site'])

        for gr in ge:
            g = dict()
            g['type'] = fetchtype.upper()
            g['group'] = gr['site']
            g['service'] = gr['type']
            g['hostname'] = gr['hostname']
            g['tags'] = {'scope' : gr['scope'], \
                         'monitored' : 1 if gr['monitored'] == "Y" else 0, \
                         'production' : 1 if gr['production'] == "Y" else 0}
            groupofendpoints.append(g)

        return groupofendpoints

    def loadDataIfNeeded(self):
        if len(self.siteList) == 0:
            self.getSitesInternal(self.siteList, 'EGI')
            self.getSitesInternal(self.siteList, 'Local')

        if len(self.serviceList) == 0:
            self.getServiceEndpoints(self.serviceList, 'EGI')
            self.getServiceEndpoints(self.serviceList, 'Local')

        if len(self.groupList) == 0:
            self.getServiceGroups(self.groupList, 'EGI')
            self.getServiceGroups(self.groupList, 'Local')

    def getServiceEndpoints(self, serviceList, scope):
        urlFile = urllib.urlopen(self.gocdbUrl + '/public/?method=get_service_endpoint&scope=' + scope)
        doc = xml.dom.minidom.parse(urlFile)
        services = doc.getElementsByTagName('SERVICE_ENDPOINT')
        for service in services:
            serviceId = ''
            if service.getAttributeNode('PRIMARY_KEY'):
                serviceId = str(service.attributes['PRIMARY_KEY'].value)
            if serviceId not in serviceList:
                serviceList[serviceId] = {}
            serviceList[serviceId]['hostname'] = service.getElementsByTagName('HOSTNAME')[0].childNodes[0].data
            serviceList[serviceId]['type'] = service.getElementsByTagName('SERVICE_TYPE')[0].childNodes[0].data
            serviceList[serviceId]['monitored'] = service.getElementsByTagName('NODE_MONITORED')[0].childNodes[0].data
            serviceList[serviceId]['production'] = service.getElementsByTagName('IN_PRODUCTION')[0].childNodes[0].data
            serviceList[serviceId]['site'] = service.getElementsByTagName('SITENAME')[0].childNodes[0].data
            serviceList[serviceId]['roc'] = service.getElementsByTagName('ROC_NAME')[0].childNodes[0].data
            serviceList[serviceId]['scope'] = scope
            serviceList[serviceId]['sortId'] = serviceList[serviceId]['hostname'] + '-' + serviceList[serviceId]['type'] + '-' + serviceList[serviceId]['site']
        urlFile.close();

    def getSitesInternal(self, siteList, scope):
        conn = httplib.HTTPSConnection(self.gocdbHost, 443, self.hostKey, self.hostCert)
        conn.request('GET', '/gocdbpi/private/?method=get_site&scope=' + scope)
        res = conn.getresponse()
        if res.status == 200:
            dom = xml.dom.minidom.parseString(res.read())
            sites = dom.getElementsByTagName('SITE')
            for site in sites:
                siteName = site.getAttribute('NAME')
                if siteName not in siteList:
                    siteList[siteName] = {'site': siteName}
                siteList[siteName]['infrastructure'] = site.getElementsByTagName('PRODUCTION_INFRASTRUCTURE')[0].childNodes[0].data
                siteList[siteName]['certification'] = site.getElementsByTagName('CERTIFICATION_STATUS')[0].childNodes[0].data
                siteList[siteName]['ngi'] = site.getElementsByTagName('ROC')[0].childNodes[0].data
                siteList[siteName]['scope'] = scope
        else:
            print('ERROR: Connection to GOCDB failed: ' + res.reason)

    def getServiceGroups(self, groupList, scope):
        conn = httplib.HTTPSConnection(self.gocdbHost, 443, self.hostKey, self.hostCert)
        conn.request('GET', '/gocdbpi/private/?method=get_service_group&scope=' + scope)
        res = conn.getresponse()
        if res.status == 200:
            doc = xml.dom.minidom.parseString(res.read())
            groups = doc.getElementsByTagName('SERVICE_GROUP')
            for group in groups:
                groupId = group.getAttribute('PRIMARY_KEY')
                if groupId not in groupList:
                    groupList[groupId] = {}
                groupList[groupId]['name'] = group.getElementsByTagName('NAME')[0].childNodes[0].data
                groupList[groupId]['monitored'] = group.getElementsByTagName('MONITORED')[0].childNodes[0].data
                groupList[groupId]['scope'] = scope
                groupList[groupId]['services'] = []
                services = group.getElementsByTagName('SERVICE_ENDPOINT')
                for service in services:
                    serviceDict = {}
                    serviceDict['hostname'] = service.getElementsByTagName('HOSTNAME')[0].childNodes[0].data
                    serviceDict['type'] = service.getElementsByTagName('SERVICE_TYPE')[0].childNodes[0].data
                    serviceDict['monitored'] = service.getElementsByTagName('NODE_MONITORED')[0].childNodes[0].data
                    serviceDict['production'] = service.getElementsByTagName('IN_PRODUCTION')[0].childNodes[0].data
                    groupList[groupId]['services'].append(serviceDict)
        else:
            print('ERROR: Connection to GOCDB failed: ' + res.reason)


def filter_by_tags(tags, listofelem):
    for attr in tags.keys():
        def getit(elem):
            value = elem['tags'][attr.lower()]
            if isinstance(value, int):
                value = "Y" if value else "N"
            if value.lower() == tags[attr].lower():
                return True
        listofelem = filter(getit, listofelem)
    return listofelem


def main():
    group_endpoints, group_groups = [], []

    certs = {'Authentication': ['HostKey', 'HostCert']}
    schemas = {'AvroSchemas': ['TopologyGOCDBGroupOfEndpoints', 'TopologyGOCDBGroupOfGroups',
                               'TopologyGOCDBGroupOfServices']}
    output = {'Output': ['TopologyGOCDBGroupOfEndpoints', 'TopologyGOCDBGroupOfGroups']}
    cglob = Global(certs, schemas, output)
    global globopts
    globopts = cglob.parse()

    cegi = EGIConf(sys.argv[0])
    cegi.parse()
    cegi.make_dirstruct()

    timestamp = datetime.datetime.utcnow().strftime('%Y_%m_%d')

    gocdb = GOCDBReader()

    for job in cegi.get_jobs():
        jobdir = cegi.get_fulldir(job)
        global fetchtype
        fetchtype = cegi.get_fetchtype(job)

        if fetchtype == 'ServiceGroups':
            group_endpoints = gocdb.getGroupOfServices()
        else:
            group_endpoints = gocdb.getGroupOfEndpoints()
        group_groups = gocdb.getGroupOfGroups()

        ggtags = cegi.get_ggtags(job)
        if ggtags:
            group_groups = filter_by_tags(ggtags, group_groups)
        filename = jobdir+globopts['OutputTopologyGOCDBGroupOfGroups'] % timestamp
        if fetchtype == 'ServiceGroups':
            avro = AvroWriter(globopts['AvroSchemasTopologyGOCDBGroupOfServices'], filename,
                              group_groups)
        else:
            avro = AvroWriter(globopts['AvroSchemasTopologyGOCDBGroupOfGroups'], filename,
                              group_groups)
        avro.write()

        getags = cegi.get_getags(job)
        if getags:
            group_endpoints = filter_by_tags(getags, group_endpoints)
        gelegmap = []
        for g in group_endpoints:
            if g['service'] in LegMapServType.keys():
                gelegmap.append(g)
                gelegmap[-1]['service'] = LegMapServType[g['service']]
        filename = jobdir+globopts['OutputTopologyGOCDBGroupOfEndpoints'] % timestamp
        avro = AvroWriter(globopts['AvroSchemasTopologyGOCDBGroupOfEndpoints'], filename,
                          group_endpoints + gelegmap)
        avro.write()

main()
