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

import argparse
import datetime
import os
import sys
import xml.dom.minidom
from xml.parsers.expat import ExpatError
import copy
from urlparse import urlparse

from argo_egi_connectors.writers import AvroWriter
from argo_egi_connectors.writers import SingletonLogger as Logger
from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.tools import gen_fname_repdate, make_connection

logger = None

globopts = {}
LegMapServType = {'SRM' : 'SRMv2'}
fileout = 'downtimes_%s.avro'

class GOCDBReader(object):
    def __init__(self, feed):
        self._o = urlparse(feed)
        self._parsed = True
        self.argDateFormat = "%Y-%m-%d"
        self.WSDateFormat = "%Y-%m-%d %H:%M"

    def getDowntimes(self, start, end):
        filteredDowntimes = list()

        res = make_connection(logger, globopts, self._o.scheme, self._o.netloc,
                              '/gocdbpi/private/?method=get_downtime&windowstart=%s&windowend=%s' % (start.strftime(self.argDateFormat),
                                                                                                     end.strftime(self.argDateFormat)),
                              "GOCDBReader.getDowntimes():")
        try:
            if res.status == 200:
                doc = xml.dom.minidom.parseString(res.read())
                downtimes = doc.getElementsByTagName('DOWNTIME')
                for downtime in downtimes:
                    classification = downtime.getAttributeNode('CLASSIFICATION').nodeValue
                    hostname = downtime.getElementsByTagName('HOSTNAME')[0].childNodes[0].data
                    serviceType = downtime.getElementsByTagName('SERVICE_TYPE')[0].childNodes[0].data
                    startStr = downtime.getElementsByTagName('FORMATED_START_DATE')[0].childNodes[0].data
                    endStr = downtime.getElementsByTagName('FORMATED_END_DATE')[0].childNodes[0].data
                    severity = downtime.getElementsByTagName('SEVERITY')[0].childNodes[0].data

                    startTime = datetime.datetime.strptime(startStr, self.WSDateFormat)
                    endTime = datetime.datetime.strptime(endStr, self.WSDateFormat)

                    if (startTime < start):
                        startTime = start
                    if (endTime > end):
                        endTime = end

                    if classification == 'SCHEDULED' and severity == 'OUTAGE':
                        dt = dict()
                        dt['hostname'] = hostname
                        dt['service'] = serviceType
                        dt['start_time'] = startTime.strftime('%Y-%m-%d %H:%M').replace(' ', 'T', 1).replace(' ', ':') + ':00Z'
                        dt['end_time'] = endTime.strftime('%Y-%m-%d %H:%M').replace(' ', 'T', 1).replace(' ', ':') + ':00Z'
                        filteredDowntimes.append(dt)
            else:
                logger.error('GOCDBReader.getDowntimes(): HTTP response: %s %s' % (str(res.status), res.reason))
                raise SystemExit(1)

        except ExpatError as e:
            logger.error("GOCDBReader.getDowntimes(): Error parsing feed %s - %s" %
                        (self._o.scheme + '://' + self._o.netloc + '/gocdbpi/private/?method=get_downtime', e.message))
            self._parsed = False

        return filteredDowntimes, self._parsed

def main():
    parser = argparse.ArgumentParser(description='Fetch downtimes from GOCDB for given date')
    parser.add_argument('-d', dest='date', nargs=1, metavar='YEAR-MONTH-DAY', required=True)
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    args = parser.parse_args()

    global logger
    logger = Logger(os.path.basename(sys.argv[0]))
    certs = {'Authentication': ['HostKey', 'HostCert', 'CAPath', 'CAFile', 'VerifyServerCert']}
    schemas = {'AvroSchemas': ['Downtimes']}
    output = {'Output': ['Downtimes']}
    conn = {'Connection': ['Timeout', 'Retry']}
    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(confpath, certs, schemas, output, conn)
    global globopts
    globopts = cglob.parse()

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    feeds = confcust.get_mapfeedjobs(sys.argv[0], deffeed='https://goc.egi.eu/gocdbpi/')


    if len(args.date) == 0:
        print parser.print_help()
        raise SystemExit(1)

    # calculate start and end times
    try:
        start = datetime.datetime.strptime(args.date[0], '%Y-%m-%d')
        end = datetime.datetime.strptime(args.date[0], '%Y-%m-%d')
        timestamp = start.strftime('%Y_%m_%d')
    except ValueError as e:
        logger.error(e)
        raise SystemExit(1)
    start = start.replace(hour=0, minute=0, second=0)
    end = end.replace(hour=23, minute=59, second=59)

    for feed, jobcust in feeds.items():
        gocdb = GOCDBReader(feed)
        dts, parsed = gocdb.getDowntimes(start, end)

        dtslegmap = []
        if parsed:
            for dt in dts:
                if dt['service'] in LegMapServType.keys():
                    dtslegmap.append(copy.copy(dt))
                    dtslegmap[-1]['service'] = LegMapServType[dt['service']]
            for job, cust in jobcust:
                jobdir = confcust.get_fulldir(cust, job)
                custname = confcust.get_custname(cust)

                filename = gen_fname_repdate(logger, timestamp, globopts['OutputDowntimes'.lower()], jobdir)
                avro = AvroWriter(globopts['AvroSchemasDowntimes'.lower()], filename,
                                dts + dtslegmap, os.path.basename(sys.argv[0]))
                avro.write()

            custs = set([cust for job, cust in jobcust])
            for cust in custs:
                jobs = [job for job, lcust in jobcust if cust == lcust]
                logger.info('Customer:%s Jobs:%d Fetched Date:%s Endpoints:%d' % (cust, len(jobs), args.date[0], len(dts + dtslegmap)))

main()
