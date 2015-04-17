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

import urllib2
import os
import json
import datetime
import sys

from avro.datafile import DataFileReader
from avro.io import DatumReader

from argo_egi_connectors.writers import AvroWriter
from argo_egi_connectors.config import VOConf, EGIConf, Global

globopts = {}

class GstatReader:
    def __init__(self):
        self.GstatRequest = globopts['URLWeightsGstat']

    def getWeights(self):
        # load server data
        urlFile = urllib2.urlopen(self.GstatRequest)
        json_data = json.load(urlFile)
        urlFile.close();
        weights = dict()
        for site in json_data:
            key = site['Sitename']
            val = site['HEPSPEC06']
            weights[key] = val
        return weights

def gen_outdict(data):
    datawr = []
    for key in data:
        w = data[key]
        datawr.append({'type': 'hepspec', 'site': key, 'weight': w})
    return datawr

def loadOldData(directory, timestamp):
    filename = directory+'/'+ globopts['OutputWeightsGstat'] % timestamp
    oldDataDict = dict()

    if not os.path.isfile(filename):
        return oldDataDict

    reader = DataFileReader(open(filename, "r"), DatumReader())
    for weight in reader:
        oldDataDict[weight["site"]] = weight["weight"]
    reader.close()

    return oldDataDict


def main():
    url = {'URL': ['WeightsGstat']}
    schemas = {'AvroSchemas': ['WeightsGstat']}
    output = {'Output': ['WeightsGstat']}
    cglob = Global(url, schemas, output)
    global globopts
    globopts = cglob.parse()

    cvo = VOConf(sys.argv[0])
    cvo.parse()
    cvo.make_dirstruct()

    cegi = EGIConf(sys.argv[0])
    cegi.parse()
    cegi.make_dirstruct()

    readerInstance = GstatReader()

    timestamp = datetime.datetime.utcnow().strftime('%Y_%m_%d')
    oldDate = datetime.datetime.utcnow()
    oldFilename = cegi.tenantdir+'/'+globopts['OutputWeightsGstat'] % oldDate.strftime('%Y_%m_%d')

    i = 0;
    oldDataExists = True
    while not os.path.isfile(oldFilename):
        oldDate = oldDate - datetime.timedelta(days=1)
        oldFilename = cegi.tenantdir+'/'+globopts['OutputWeightsGstat'] % oldDate.strftime('%Y_%m_%d')
        i = i+1
        if i >= 30:
            oldDataExists = False
            break

    # load old data
    oldData = dict()
    if oldDataExists:
        oldData.update(loadOldData(cegi.tenantdir, oldDate.strftime('%Y_%m_%d')))

    # load new data
    newData = dict()
    newData.update(readerInstance.getWeights());

    # fill new list
    for key in newData:
        newVal = int(newData[key])
        if newVal <= 0:
            if key in oldData:
                newVal = int(oldData[key])
        if key not in oldData:
            oldData[key] = str(newVal)
        newData[key] = str(newVal)

    # fill old list
    for key in oldData:
        oldVal = int(oldData[key])
        if oldVal <= 0:
            if key in newData:
                oldData[key] = newData[key]
        if key not in newData:
            newData[key] = oldData[key]

    for job in cegi.get_jobs():
        jobdir = cegi.get_fulldir(job)

        filename = jobdir + globopts['OutputWeightsGstat'] % timestamp
        datawr = gen_outdict(newData)
        avro = AvroWriter(globopts['AvroSchemasWeightsGstat'], filename, datawr)
        avro.write()

        if oldDataExists:
            datawr = gen_outdict(oldData)
            avro = AvroWriter(globopts['AvroSchemasWeightsGstat'], filename, datawr)
            avro.write()

    for vo in cvo.get_vos():
        for job in cvo.get_jobs(vo):
            jobdir = cvo.get_fulldir(job)

            filename = jobdir + globopts['OutputWeightsGstat'] % timestamp
            datawr = gen_outdict(newData)
            avro = AvroWriter(globopts['AvroSchemasWeightsGstat'], filename, datawr)
            avro.write()

            if oldDataExists:
                datawr = gen_outdict(oldData)
                avro = AvroWriter(globopts['AvroSchemasWeightsGstat'], filename, datawr)
                avro.write()


main()
