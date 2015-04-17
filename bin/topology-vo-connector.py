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

from argo_egi_connectors.writers import AvroWriter
from argo_egi_connectors.config import VOConf, Global
from exceptions import AssertionError
import datetime
import httplib
import re
import sys
import xml.dom.minidom

globopts = {}
lgroups = []
lendpoints = []

def parse_vofeed(host, path):
    if 'https' in host[0]:
        conn = httplib.HTTPSConnection(host[1], 443, globopts['AuthenticationHostKey'], globopts['AuthenticationHostCert'])
    elif 'http' in host[0]:
        conn = httplib.HTTPConnection(host[1])

    conn.request('GET', path)
    res = conn.getresponse()
    if res.status == 200:
        try:
            dom = xml.dom.minidom.parseString(res.read())
            sites = dom.getElementsByTagName('atp_site')
            assert len(sites) > 0
            for site in sites:
                gg = {}
                subgroup = site.getAttribute('name')
                assert subgroup != ''
                groups = site.getElementsByTagName('group')
                assert len(groups) > 0
                for group in groups:
                    gg = {}
                    gg['group'] = group.getAttribute('name')
                    gg['type'] = group.getAttribute('type')
                    gg['subgroup'] = subgroup
                    lgroups.append(gg)

                endpoints = site.getElementsByTagName('service')
                assert len(endpoints) > 0
                for endpoint in endpoints:
                    ge = {}
                    ge['group'] = subgroup
                    ge['hostname'] = endpoint.getAttribute('hostname')
                    ge['service'] = endpoint.getAttribute('flavour')
                    ge['type'] = 'SITES'
                    lendpoints.append(ge)
        except AssertionError:
            print parse_vofeed.__name__, "Error parsing VO-feed %s" % ('//'.join(host)+path)
    else:
        print parse_vofeed.__name__, 'ERROR: Connection to %s failed: %s' % (host[1],res.reason)

def main():
    certs = {'Authentication': ['HostKey', 'HostCert']}
    schemas = {'AvroSchemas': ['TopologyVOGroupOfEndpoints', 'TopologyVOGroupOfGroups']}
    output = {'Output': ['TopologyVOGroupOfEndpoints', 'TopologyVOGroupOfGroups']}
    cglob = Global(certs, schemas, output)
    global globopts
    globopts = cglob.parse()

    cvo = VOConf(sys.argv[0])
    cvo.parse()
    cvo.make_dirstruct()

    timestamp = datetime.datetime.utcnow().strftime('%Y_%m_%d')

    for vo, feed in cvo.get_feeds():
        urlsplit = re.split('/*', feed)
        host = (urlsplit[0], urlsplit[1])
        path = '/'+'/'.join(urlsplit[2:])
        parse_vofeed(host, path)

        filtlgroups = lgroups
        for job in cvo.get_jobs(vo):
            jobdir = cvo.get_fulldir(job)

            tags = cvo.get_ggtags(job)
            if tags:
                def ismatch(elem):
                    values = tags['Type']
                    e = elem['type'].lower()
                    for val in values:
                        if e == val.lower():
                            return True
                filtlgroups = filter(ismatch, lgroups)

            filename = jobdir + globopts['OutputTopologyVOGroupOfGroups'] % timestamp
            avro = AvroWriter(globopts['AvroSchemasTopologyVOGroupOfGroups'], filename, filtlgroups)
            avro.write()

            filename = jobdir + globopts['OutputTopologyVOGroupOfEndpoints'] % timestamp
            avro = AvroWriter(globopts['AvroSchemasTopologyVOGroupOfEndpoints'], filename, lendpoints)
            avro.write()

main()
