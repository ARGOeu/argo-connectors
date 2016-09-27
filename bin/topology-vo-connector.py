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

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.tools import gen_fname_repdate, make_connection
from argo_egi_connectors.writers import AvroWriter
from argo_egi_connectors.writers import SingletonLogger as Logger
from exceptions import AssertionError
from urlparse import urlparse

import argparse
import copy
import datetime
import os
import sys
import xml.dom.minidom


LegMapServType = {'SRM' : 'SRMv2', 'SRMv2': 'SRM'}
globopts = {}
logger = None

class VOReader:
    lendpoints= []
    lgroups = []

    def __init__(self, feed):
        self._o = urlparse(feed)
        self.feed = feed
        self._parse()

    def _parse(self):
        try:
            res = make_connection(logger, globopts, self._o.scheme, self._o.netloc, self._o.path,
                                  "VOReader._parse():")
            if res.status == 200:
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
                        self.lgroups.append(gg)

                    endpoints = site.getElementsByTagName('service')
                    assert len(endpoints) > 0
                    for endpoint in endpoints:
                        ge = {}
                        ge['group'] = subgroup
                        ge['hostname'] = endpoint.getAttribute('hostname')
                        ge['service'] = endpoint.getAttribute('flavour')
                        ge['type'] = 'SITES'
                        self.lendpoints.append(ge)
            else:
                logger.error('VOReader._parse(): HTTP response: %s %s' % (str(res.status), res.reason))
                raise SystemExit(1)
        except AssertionError:
            logger.error("VOReader._parse(): Error parsing VO-feed %s" % (o.netloc + o.path))
            raise SystemExit(1)

    def get_groupgroups(self):
        return self.lgroups

    def get_groupendpoints(self):
        return self.lendpoints


def main():
    parser = argparse.ArgumentParser(description="""Fetch wanted entities from VO feed provided in customer.conf
                                                    and write them in an appropriate place""")
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    args = parser.parse_args()

    global logger
    logger = Logger(os.path.basename(sys.argv[0]))

    certs = {'Authentication': ['HostKey', 'HostCert', 'CAPath', 'CAFile', 'VerifyServerCert']}
    schemas = {'AvroSchemas': ['TopologyGroupOfEndpoints', 'TopologyGroupOfGroups']}
    output = {'Output': ['TopologyGroupOfEndpoints', 'TopologyGroupOfGroups']}
    conn = {'Connection': ['Timeout', 'Retry']}
    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(confpath, certs, schemas, output, conn)
    global globopts
    globopts = cglob.parse()

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    feeds = confcust.get_mapfeedjobs(sys.argv[0], 'VOFeed')

    timestamp = datetime.datetime.utcnow().strftime('%Y_%m_%d')

    for feed, jobcust in feeds.items():
        vo = VOReader(feed)

        for job, cust in jobcust:
            jobdir = confcust.get_fulldir(cust, job)

            custname = confcust.get_custname(cust)

            filtlgroups = vo.get_groupgroups()
            numgg = len(filtlgroups)
            tags = confcust.get_vo_ggtags(job)
            if tags:
                def ismatch(elem):
                    values = tags['Type']
                    e = elem['type'].lower()
                    for val in values:
                        if e == val.lower():
                            return True
                filtlgroups = filter(ismatch, filtlgroups)

            filename = gen_fname_repdate(logger, timestamp, globopts['OutputTopologyGroupOfGroups'.lower()], jobdir)
            avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfGroups'.lower()], filename, filtlgroups,
                              os.path.basename(sys.argv[0]))
            avro.write()

            gelegmap = []
            group_endpoints = vo.get_groupendpoints()
            numge = len(group_endpoints)
            for g in group_endpoints:
                if g['service'] in LegMapServType.keys():
                    gelegmap.append(copy.copy(g))
                    gelegmap[-1]['service'] = LegMapServType[g['service']]
            filename = gen_fname_repdate(logger, timestamp, globopts['OutputTopologyGroupOfEndpoints'.lower()], jobdir)
            avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()], filename, group_endpoints + gelegmap,
                                                                                       os.path.basename(sys.argv[0]))
            avro.write()

            logger.info('Customer:' + custname + ' Job:' + job + ' Fetched Endpoints:%d' % (numge + len(gelegmap))+' Groups:%d' % (numgg))
            if tags:
                selstr = 'Customer:%s Job:%s Selected ' % (custname, job)
                selgg = ''
                for key, value in tags.items():
                    if isinstance(value, list):
                        value = ','.join(value)
                    selgg += '%s:%s,' % (key, value)
                selstr += 'Groups(%s):' % selgg[:len(selgg) - 1]
                selstr += '%d' % (len(filtlgroups))

                logger.info(selstr)

main()
