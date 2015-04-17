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
import httplib
import os
import sys
import xml.dom.minidom

from argo_egi_connectors.writers import AvroWriter
from argo_egi_connectors.config import VOConf, EGIConf, Global

globopts = {}

LegMapServType = {'SRM' : 'SRMv2'}
defaultArgDateFormat = '%Y-%m-%d'

fileout = 'downtimes_%s.avro'

class GOCDBReader:
    def __init__(self):
        self.gocdbHost = 'goc.egi.eu'
        self.gocdbUrl = 'https://'+self.gocdbHost+'/gocdbpi/'
        self.hostKey =  globopts['AuthenticationHostKey']
        self.hostCert = globopts['AuthenticationHostCert']
        self.argDateFormat = "%Y-%m-%d"
        self.WSDateFormat = "%Y-%m-%d %H:%M"

    def getDowntimes(self, start, end):
        filteredDowntimes = list()

        conn = httplib.HTTPSConnection(self.gocdbHost, 443, self.hostKey, self.hostCert)
        conn.request('GET', '/gocdbpi/private/' + '?method=get_downtime&windowstart=%s&windowend=%s' % (start.strftime(self.argDateFormat), end.strftime(self.argDateFormat)))
        res = conn.getresponse()
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
            print('ERROR: Connection to GOCDB failed: ' + res.reason)

        return filteredDowntimes

def main():
    certs = {'Authentication': ['HostKey', 'HostCert']}
    schemas = {'AvroSchemas': ['DowntimesGOCDB']}
    output = {'Output': ['DowntimesGOCDB']}
    cglob = Global(certs, schemas, output)
    global globopts
    globopts = cglob.parse()
    timestamp = datetime.datetime.utcnow().strftime('%Y_%m_%d')

    cvo = VOConf(sys.argv[0])
    cvo.parse()
    cvo.make_dirstruct()

    cegi = EGIConf(sys.argv[0])
    cegi.parse()
    cegi.make_dirstruct()

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='date', nargs=1, metavar='YEAR-MONTH-DAY', required=True)
    args = parser.parse_args()

    if len(args.date) == 0:
        print parser.print_help()
        raise SystemExit(1)

    # calculate start and end times
    start = datetime.datetime.strptime(args.date[0], defaultArgDateFormat)
    end = datetime.datetime.strptime(args.date[0], defaultArgDateFormat)
    start = start.replace(hour=0, minute=0, second=0)
    end = end.replace(hour=23, minute=59, second=59)

    # read downtimes
    readerInstance = GOCDBReader()
    dts = readerInstance.getDowntimes(start, end)

    for vo in cvo.get_vos():
        for job in cvo.get_jobs(vo):
            jobdir = cvo.get_fulldir(job)
            filename = jobdir + globopts['OutputDowntimesGOCDB'] % timestamp
            avro = AvroWriter(globopts['AvroSchemasDowntimesGOCDB'], filename,
                              dts)
            avro.write()

    for job in cegi.get_jobs():
        jobdir = cegi.get_fulldir(job)
        filename = jobdir + globopts['OutputDowntimesGOCDB'] % timestamp
        avro = AvroWriter(globopts['AvroSchemasDowntimesGOCDB'], filename,
                            dts)
        avro.write()

main()
