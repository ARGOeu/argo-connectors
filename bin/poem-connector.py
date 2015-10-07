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
import os
import json
import datetime
import httplib
import sys
import urlparse
import socket
import re

from argo_egi_connectors.writers import AvroWriter
from argo_egi_connectors.writers import SingletonLogger as Logger
from argo_egi_connectors.config import CustomerConf, PoemConf, Global
from argo_egi_connectors.tools import verify_cert, errmsg_from_excp
from OpenSSL.SSL import Error as SSLError

logger = None
globopts, poemopts = {}, {}
cpoem = None

class PoemReader:
    def __init__(self):
        self.poemRequest = 'https://%s/poem/api/0.2/json/metrics_in_profiles?vo_name=%s'

    def getProfiles(self):
        filteredProfiles = re.split('\s*,\s*', poemopts['FetchProfilesList'.lower()])
        availableVOs = [vo for k, v in cpoem.get_servers().items() for vo in v]
        validProfiles = self.loadValidProfiles(filteredProfiles)

        ngiall = cpoem.get_allngi()
        ngiallow = cpoem.get_allowedngi()

        profileList = []
        profileListAvro = []

        for server, profiles in ngiallow.items():
            defaultProfiles = profiles
            url = server

            urlFile = urllib.urlopen(url)
            urlLines = urlFile.read().splitlines()

            for urlLine in urlLines:
                if len(urlLine) == 0 or urlLine[0] == '#':
                    continue

                ngis = urlLine.split(':')[0].split(',')
                servers = urlLine.split(':')[2].split(',')

                for vo in availableVOs:
                    serverProfiles = []
                    if len(defaultProfiles) > 0:
                        serverProfiles = defaultProfiles
                    else:
                        serverProfiles = self.loadProfilesFromServer(servers[0], vo, filteredProfiles).keys()
                    for profile in serverProfiles:
                        if profile.upper() in validProfiles.keys():
                            for ngi in ngis:
                                for server in servers:
                                    profileList.extend(self.createProfileEntries(server, ngi, validProfiles[profile.upper()]))

            urlFile.close();

        for server, profiles in ngiall.items():

            ngis = ['ALL']
            servers = [server]
            defaultProfiles = profiles

            for vo in availableVOs:
                serverProfiles = []
                if len(defaultProfiles) > 0:
                    serverProfiles = defaultProfiles
                else:
                    serverProfiles = self.loadProfilesFromServer(servers[0], vo, filteredProfiles).keys()

            for profile in serverProfiles:
                if profile.upper() in validProfiles.keys():
                    for ngi in ngis:
                        for server in servers:
                            profileList.extend(self.createProfileEntries(server, ngi, validProfiles[profile.upper()]))

        for profile in validProfiles.values():
            for metric in profile['metrics']:
                profileListAvro.append({'profile' : profile['namespace'] + '.' + profile['name'], \
                                        'metric' : metric['name'], \
                                        'service' : metric['service_flavour'], \
                                        'vo' : profile['vo'], \
                                        'fqan' : metric['fqan']})

        return profileList, profileListAvro

    def loadValidProfiles(self, filteredProfiles):
        validProfiles = dict()

        for url, vos in cpoem.get_servers().items():
            for vo in vos:
                serverProfiles = self.loadProfilesFromServer(url, vo, filteredProfiles)
                for profile in serverProfiles.keys():
                    if not profile in validProfiles.keys():
                        validProfiles[profile] = serverProfiles[profile]
                        validProfiles[profile]['vo'] = vo

        return validProfiles

    def loadProfilesFromServer(self, server, vo, filterProfiles):
        validProfiles = dict()

        doFilterProfiles = False
        if len(filterProfiles) > 0:
            doFilterProfiles = True

        url = self.poemRequest % (server, vo)
        o = urlparse.urlparse(url, allow_fragments=True)
        logger.info('Server:%s VO:%s' % (o.netloc, vo))

        try:
            if eval(globopts['AuthenticationVerifyServerCert'.lower()]):
                verify_cert(o.netloc, globopts['AuthenticationCAPath'.lower()], 180)
            conn = httplib.HTTPSConnection(o.netloc, 443,
                                           globopts['AuthenticationHostKey'.lower()],
                                           globopts['AuthenticationHostCert'.lower()])
            conn.request('GET', o.path + '?' + o.query)

            res = conn.getresponse()
            if res.status == 200:
                json_data = json.loads(res.read())
                for profile in json_data[0]['profiles']:
                    if not doFilterProfiles or (profile['namespace']+'.'+profile['name']).upper() in filterProfiles:
                        validProfiles[(profile['namespace']+'.'+profile['name']).upper()] = profile
            elif res.status in (301, 302):
                logger.warning('Redirect: ' + urlparse.urljoin(url, res.getheader('location', '')))

            else:
                logger.error('POEMReader.loadProfilesFromServer(): HTTP response: %s %s' % (str(res.status), res.reason))
                raise SystemExit(1)
        except(SSLError, socket.error, socket.timeout, httplib.HTTPException) as e:
            logger.error('Connection error %s - %s' % (server, errmsg_from_excp(e)))
            raise SystemExit(1)

        return validProfiles

    def createProfileEntries(self, server, ngi, profile):
        entries = list()
        for metric in profile['metrics']:
            entry = dict()
            entry["profile"] = profile['namespace']+'.'+profile['name']
            entry["service"] = metric['service_flavour']
            entry["metric"] = metric['name']
            entry["server"] = server
            entry["ngi"] = ngi
            entry["vo"] = profile['vo']
            entry["fqan"] = metric['fqan']
            entries.append(entry)
        return entries

class PrefilterPoem:
    def __init__(self, outdir):
        self.outputDir = outdir
        self.outputFileTemplate = 'poem_sync_%s.out'
        self.outputFileFormat = '%s\001%s\001%s\001%s\001%s\001%s\001%s\r\n'

    def writeProfiles(self, profiles, date):
        filename = self.outputDir+'/'+self.outputFileTemplate % date
        outFile = open(filename, 'w')
        moninstance = set()
        for p in profiles:
            moninstance.add(p['server'])
            outFile.write(self.outputFileFormat % ( p['server'],
                       p['ngi'],
                       p['profile'],
                       p['service'],
                       p['metric'],
                       p['vo'],
                       p['fqan']))
        outFile.close();

        logger.info('POEM file(%s): Expanded profiles for %d monitoring instances' % (self.outputFileTemplate % date, len(moninstance)))

def gen_outprofiles(lprofiles, matched):
    lfprofiles = []

    for p in lprofiles:
        if p['profile'].split('.')[-1] in matched:
            pt = dict()
            pt['metric'] = p['metric']
            pt['profile'] = p['profile']
            pt['service'] = p['service']
            pt['tags'] = {'vo' : p['vo'], 'fqan' : p['fqan']}
            lfprofiles.append(pt)

    return lfprofiles

def main():
    global logger
    logger = Logger(os.path.basename(sys.argv[0]))

    certs = {'Authentication': ['HostKey', 'HostCert', 'VerifyServerCert', 'CAPath']}
    schemas = {'AvroSchemas': ['Poem']}
    output = {'Output': ['Poem']}
    cglob = Global(certs, schemas, output)
    global globopts
    globopts = cglob.parse()
    timestamp = datetime.datetime.utcnow().strftime('%Y_%m_%d')

    servers = {'PoemServer': ['Host', 'VO']}
    filterprofiles = {'FetchProfiles': ['List']}
    prefilterdata = {'PrefilterData': ['AllowedNGI', 'AllowedNGIProfiles', 'AllNGI', 'AllNGIProfiles']}
    global cpoem, poemopts
    cpoem = PoemConf(servers, filterprofiles, prefilterdata)
    poemopts = cpoem.parse()

    confcust = CustomerConf(sys.argv[0])
    confcust.parse()
    confcust.make_dirstruct()

    readerInstance = PoemReader()
    ps, psa = readerInstance.getProfiles()

    for cust in confcust.get_customers():
        # write profiles
        poempref = PrefilterPoem(confcust.get_custdir(cust))
        poempref.writeProfiles(ps, timestamp)

        for job in confcust.get_jobs(cust):
            jobdir = confcust.get_fulldir(cust, job)

            profiles = confcust.get_profiles(job)
            lfprofiles = gen_outprofiles(psa, profiles)

            filename = jobdir + globopts['OutputPoem'.lower()]% timestamp
            avro = AvroWriter(globopts['AvroSchemasPoem'.lower()], filename,
                              lfprofiles, os.path.basename(sys.argv[0]))
            avro.write()

            logger.info('Job:'+job+' Profiles:%s Tuples:%d' % (','.join(profiles), len(lfprofiles)))

main()
