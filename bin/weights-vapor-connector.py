#!/bin/env python

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
import json
import os
import sys

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.tools import gen_fname_repdate, make_connection
from argo_egi_connectors.writers import AvroWriter
from argo_egi_connectors.writers import SingletonLogger as Logger
from avro.datafile import DataFileReader
from avro.io import DatumReader
from urlparse import urlparse

globopts = {}
logger = None

class Vapor:
    def __init__(self, feed):
        self._o = urlparse(feed)

    def getWeights(self):
        res = make_connection(logger, globopts, self._o.scheme, self._o.netloc, self._o.path,
                              "Vapor.getWeights()):")
        if res.status == 200:
            json_data = json.loads(res.read())
            weights = dict()
            for ngi in json_data:
                for site in ngi['site']:
                    key = site['id']
                    val = site['HEPSPEC2006']
                    if val == 'NA':
                        continue
                    weights[key] = val
            return weights
        else:
            logger.error('Vapor.getWeights(): HTTP response: %s %s' % (str(res.status), res.reason))
            raise SystemExit(1)

def gen_outdict(data):
    datawr = []
    for key in data:
        w = data[key]
        datawr.append({'type': 'hepspec', 'site': key, 'weight': w})
    return datawr

def loadOldData(filename):
    oldDataDict = dict()

    if not os.path.isfile(filename):
        return oldDataDict

    reader = DataFileReader(open(filename, "r"), DatumReader())
    for weight in reader:
        oldDataDict[weight["site"]] = weight["weight"]
    reader.close()

    return oldDataDict

def main():
    parser = argparse.ArgumentParser(description="""Fetch weights information from Gstat provider
                                                    for every job listed in customer.conf""")
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    args = parser.parse_args()

    global logger
    logger = Logger(os.path.basename(sys.argv[0]))

    certs = {'Authentication': ['HostKey', 'HostCert', 'CAPath', 'CAFile', 'VerifyServerCert']}
    schemas = {'AvroSchemas': ['Weights']}
    output = {'Output': ['Weights']}
    conn = {'Connection': ['Timeout', 'Retry']}
    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(confpath, schemas, output, certs, conn)
    global globopts
    globopts = cglob.parse()

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    feeds = confcust.get_mapfeedjobs(sys.argv[0], deffeed='https://operations-portal.egi.eu/vapor/downloadLavoisier/option/json/view/VAPOR_Ngi_Sites_Info')

    timestamp = datetime.datetime.utcnow().strftime('%Y_%m_%d')
    oldDate = datetime.datetime.utcnow()

    for feed, jobcust in feeds.items():
        weights = Vapor(feed)

        newweights = dict()
        newweights.update(weights.getWeights());

        for job, cust in jobcust:
            fileprev, existfileprev = None, None
            jobdir = confcust.get_fulldir(cust, job)

            oldDataExists = False
            now = datetime.datetime.utcnow
            i = 1
            while i <= 30:
                dayprev = datetime.datetime.utcnow() - datetime.timedelta(days=i)
                fileprev = gen_fname_repdate(logger, dayprev.strftime('%Y_%m_%d'), globopts['OutputWeights'.lower()], jobdir)
                if os.path.exists(fileprev):
                    existfileprev = fileprev
                    break
                i += 1

            oldweights = dict()
            # load old data
            if existfileprev:
                oldweights.update(loadOldData(existfileprev))

                # fill old list
                for key in oldweights:
                    val = int(oldweights[key])
                    if val <= 0:
                        if key in newweights:
                            oldweights[key] = str(newweights[key])
                    if key not in newweights:
                        newweights[key] = str(oldweights[key])

            # fill new list
            for key in newweights:
                val = int(newweights[key])
                if val <= 0:
                    if key in oldweights:
                        val = int(oldweights[key])
                if key not in oldweights:
                    oldweights[key] = str(val)
                newweights[key] = str(val)

            filename = gen_fname_repdate(logger, timestamp, globopts['OutputWeights'.lower()], jobdir)

            datawr = gen_outdict(newweights)
            avro = AvroWriter(globopts['AvroSchemasWeights'.lower()], filename, datawr, os.path.basename(sys.argv[0]))
            avro.write()

            if existfileprev:
                olddata = gen_outdict(oldweights)
                os.remove(existfileprev)
                avro = AvroWriter(globopts['AvroSchemasWeights'.lower()], existfileprev, olddata, os.path.basename(sys.argv[0]))
                avro.write()

        custs = set([cust for job, cust in jobcust])
        for cust in custs:
            jobs = [job for job, lcust in jobcust if cust == lcust]
            logger.info('Customer:%s Jobs:%d Sites:%d' % (cust, len(jobs), len(datawr)))

main()
