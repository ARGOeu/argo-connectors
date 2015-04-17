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

import sys
import os
import datetime
import avro.schema
import argparse
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from argo_egi_connectors.config import Global, EGIConf

globopts, cegi = {}, None

#poem
poemFileFields = 'server;ngi;profile;service_flavour;metric;vo;fqan'
poemFileFieldDelimiter = '\001'

writeToStd = 0

# reject on missing monitoring host
rejectMissingMonitoringHost = 0

# past files checking
checkInputFileForDays = 1

##############################
# find poem profile file name
##############################
def poemProfileFilenameCheck(year, month, day):

    count = 0
    dt = datetime.datetime(year=int(year), month=int(month), day=int(day))
    while True:
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        fileName = cegi.tenantdir + '/' + globopts['OutputPrefilterPoem'] % (year+'_'+month+'_'+day)
        if os.path.isfile(fileName):
            break
        if count >= checkInputFileForDays:
            fileName = None
            break
        count = count+1
        dt = dt - datetime.timedelta(days=1)

    return fileName

##############################
# load ngis
##############################
def loadNGIs(year, month, day):

    ngiTree = dict()

    profileFieldNames = poemFileFields.split(';')

    poemProfileFile = open(poemProfileFilenameCheck(year, month, day), 'r')
    poemProfiles = poemProfileFile.read().splitlines()
    poemProfileFile.close()

    for line in poemProfiles:
        if len(line) == 0 or line[0] == '#':
            continue

        profileFields = line.split(poemFileFieldDelimiter)
        profile = dict()
        for i in range(0, len(profileFieldNames)):
            profile[profileFieldNames[i]] = profileFields[i]

        if profile['server'] not in ngiTree:
            ngiTree[profile['server']] = list()

        if profile['ngi'] not in  ngiTree[profile['server']]:
            ngiTree[profile['server']].append(profile['ngi'])

        # ngiTree[profile['server']] = profile['ngi']

    return ngiTree

##############################
# load profiles
##############################
def loadFilteredProfiles(year, month, day):

    profileTree = dict()

    profileFieldNames = poemFileFields.split(';')

    poemProfileFile = open(poemProfileFilenameCheck(year, month, day), 'r')
    poemProfiles = poemProfileFile.read().splitlines()
    poemProfileFile.close()

    for line in poemProfiles:
        if len(line) == 0 or line[0] == '#':
            continue

        profileFields = line.split(poemFileFieldDelimiter)

        profile = dict()
        for i in range(0, len(profileFieldNames)):
            profile[profileFieldNames[i]] = profileFields[i]

        #find server tree
        serverTree = dict()
        if profile['server'] in profileTree.keys():
            serverTree = profileTree[profile['server']]
        else:
            profileTree[profile['server']] = serverTree

        #find type tree
        typeTree = dict()
        if profile['service_flavour'] in serverTree.keys():
            typeTree = serverTree[profile['service_flavour']]
        else:
            serverTree[profile['service_flavour']] = typeTree

        #find type metric
        metricTree = dict()
        if profile['metric'] in typeTree.keys():
            metricTree = typeTree[profile['metric']]
        else:
            typeTree[profile['metric']] = metricTree

        #find vo tree
        vo = '-'
        if len(profile['vo']) > 0:
            vo = profile['vo']
        voTree = dict()
        if vo in metricTree.keys():
            voTree = metricTree[vo]
        else:
            metricTree[vo] = voTree

        #find fqan tree
        fqan = '-'
        if len(profile['fqan']) > 0:
            vfqan = profile['fqan']
        fqanTree = dict()
        if fqan in voTree.keys():
            fqanTree = voTree[fqan]
        else:
            voTree[fqan] = fqanTree

        voTree[fqan][profile['profile']] = profile

    return profileTree

##############################
# load name mapping
##############################
def loadNameMapping(year, month, day):

    nameMappingDict = dict()

    nameMappingFile = None
    try:
        nameMappingFile = open('/etc/argo-egi-connectors/'+globopts['OutputPrefilterPoemNameMapping'], 'r')
    except IOError:
        nameMappingFile = None

    if nameMappingFile != None:

        nameMappings = nameMappingFile.read().splitlines()
        nameMappingFile.close()

        for line in nameMappings:
            if len(line) == 0 or line[0] == '#':
                continue

            nameMappingFields = line.split(':')
            if len(nameMappingFields) > 1:
                oldName = nameMappingFields[0].strip()
                newName = nameMappingFields[1].strip()
                nameMappingDict[oldName] = newName

    return nameMappingDict

##############################
# get message profiles
##############################
def getProfilesForConsumerMessage(profileTree, nameMapping, logItem):

    #chekc for nagios
    if logItem.get('monitoring_host') == None:
        return -1

    #find server tree
    if logItem['monitoring_host'] in profileTree.keys():
        serverTree = profileTree[logItem['monitoring_host']]
    else:
        return -1

    #find service type tree
    if logItem['service'] in serverTree.keys():
        typeTree = serverTree[logItem['service']]
    else:
        return -1

    #find type metric
    if logItem['metric'] in typeTree.keys():
        metricTree = typeTree[logItem['metric']]
    else:
        # check name mapping
        if logItem['metric'] in nameMapping:
            if nameMapping[logItem['metric']] in typeTree.keys():
                metricTree = typeTree[nameMapping[logItem['metric']]]
            else:
                return -1
        else:
            return -1

    #find vo tree
    if logItem.get('tags') != None and logItem.get('tags').get('vo') != None:
        if logItem['tags']['vo'] in metricTree.keys():
            voTree =  metricTree[logItem['tags']['vo']]
        else:
            return -1
    else:
        fqanList = list()
        for vo in metricTree.values():
            if '-' in vo.keys():
                fqan = vo['-']
                fqanList.extend(fqan.values())

        return fqanList

        # find vo_fqan tree
    if logItem.get('tags') != None and logItem.get('tags').get('vo_fqan') != None:
        if logItem['tags']['vo_fqan'] in voTree.keys():
            return voTree[logItem['tags']['vo_fqan']].values()
        else:
            if '-' in voTree.keys():
                return voTree['-'].values()
            else:
                return -1
    else:
        fqanList = list()
        for fqan in voTree.values():
            fqanList.extend(fqan.values())

        return fqanList


def main():
    global cegi
    cegi = EGIConf(sys.argv[0])
    cegi.parse()
    schemas = {'AvroSchemas': ['Prefilter']}
    output = {'Output': ['PrefilterPoem', 'PrefilterConsumerFilePath',
                         'Prefilter', 'PrefilterPoemNameMapping']}
    cglob = Global(schemas, output)
    global globopts
    globopts = cglob.parse()

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='date', nargs=1, metavar='YEAR-MONTH-DAY', required=True)
    args = parser.parse_args()

    date = args.date[0].split('-')

    if len(date) == 0 or len(date) != 3:
        print parser.print_help()
        raise SystemExit(1)

    year, month, day = date

    # load poem data
    ngis = loadNGIs(year, month, day)
    profiles = loadFilteredProfiles(year, month, day)
    nameMapping = loadNameMapping(year, month, day)

    # avro files
    inputFile = globopts['OutputPrefilterConsumerFilePath'] % (year+'-'+month+'-'+day)
    outputFile = cegi.tenantdir+'/'+globopts['OutputPrefilter'] % (year+'_'+month+'_'+day)

    schema = avro.schema.parse(open(globopts['AvroSchemasPrefilter']).read())
    writer = DataFileWriter(open(outputFile, "w"), DatumWriter(), schema)

    rejected = 0
    reader = DataFileReader(open(inputFile, "r"), DatumReader())
    for logItem in reader:
        #check if monitoring_host exists
        if logItem.get('monitoring_host') == None:
            if rejectMissingMonitoringHost > 0:
                rejected += 1
                if writeToStd > 0:
                    print logItem
                    print '\n'
            else:
                writer.append(logItem)
            continue

        #ngi check
        ngiOk = False
        if logItem['monitoring_host'] in ngis.keys():
            ngiList = ngis[logItem['monitoring_host']]
            if 'ALL' in ngiList or (logItem['tags'] and logItem['tags']['roc'] in ngiList):
                ngiOk = True

        if ngiOk:
            #profile check
            msgprofiles = getProfilesForConsumerMessage(profiles, nameMapping, logItem)
            if type(msgprofiles) is int:
                if writeToStd > 0:
                    print logItem
                    print '\n'
                rejected += 1
                continue

            if len(msgprofiles) > 0:
                    writer.append(logItem)
            else:
                rejected += 1
                if writeToStd > 0:
                    print logItem
                    print '\m'

    reader.close()
    writer.close()

    print 'Rejected: ' + str(rejected)

main()
