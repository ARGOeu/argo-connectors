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
import re
from argo_egi_connectors.writers import AvroWriter
from argo_egi_connectors.config import VOConf, EGIConf, PoemConf, Global

writers = ['file', 'avro']

globopts, poemopts = {}, {}
cpoem = None

def resolve_http_redirect(url, depth=0):
    if depth > 10:
        raise Exception("Redirected "+depth+" times, giving up.")

    o = urlparse.urlparse(url,allow_fragments=True)
    conn = httplib.HTTPSConnection(o.netloc, 443,
                                   globopts['AuthenticationHostKey'],
                                   globopts['AuthenticationHostCert'])
    path = o.path
    if o.query:
        path +='?'+o.query

    try:
        conn.request("HEAD", path)
        res = conn.getresponse()
        headers = dict(res.getheaders())
        if headers.has_key('location') and headers['location'] != url:
            return resolve_http_redirect(headers['location'],
                                         globopts['AuthenticationHostKey'],
                                         globopts['AuthenticationHostCert'],
                                         depth+1)
        else:
            return url
    except:
        return url;

class PoemReader:
    def __init__(self):
        self.poemRequest = '%s/poem/api/0.2/json/metrics_in_profiles?vo_name=%s'

    def getProfiles(self):
        filteredProfiles = re.split('\s*,\s*', poemopts['FetchProfilesList'])
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

        if 'https://' not in server:
            server = 'https://' + server

        print server
        print self.poemRequest % ('',vo)

        url = resolve_http_redirect(self.poemRequest % (server,vo))
        print url

        o = urlparse.urlparse(url,allow_fragments=True)
        try:
            conn = httplib.HTTPSConnection(o.netloc, 443,
                                           globopts['AuthenticationHostKey'],
                                           globopts['AuthenticationHostCert'])
            conn.request('GET', o.path + '?' + o.query)

            res = conn.getresponse()
            if res.status == 200:
                json_data = json.loads(res.read())
                for profile in json_data[0]['profiles']:
                    if not doFilterProfiles or (profile['namespace']+'.'+profile['name']).upper() in filterProfiles:
                        validProfiles[(profile['namespace']+'.'+profile['name']).upper()] = profile
            elif res.status in (301, 302):
                print('Redirect: ' + urlparse.urljoin(url, res.getheader('location', '')))

            else:
                print('ERROR: Connection failed: ' + res.reason)
        except:
            print "Unexpected error:", sys.exc_info()[0]

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

class FileWriter:
    def __init__(self, outdir):
        self.outputDir = outdir
        self.outputFileTemplate = 'poem_sync_%s.out'
        self.outputFileFormat = '%s\001%s\001%s\001%s\001%s\001%s\001%s\r\n'

    def writeProfiles(self, profiles, date):
        filename = self.outputDir+'/'+self.outputFileTemplate % date
        outFile = open(filename, 'w')
        for p in profiles:
            outFile.write(self.outputFileFormat % ( p['server'],
                       p['ngi'],
                       p['profile'],
                       p['service'],
                       p['metric'],
                       p['vo'],
                       p['fqan']))
        outFile.close();

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
    certs = {'Authentication': ['HostKey', 'HostCert']}
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

    cvo = VOConf(sys.argv[0])
    cvo.parse()
    cvo.make_dirstruct()

    cegi = EGIConf(sys.argv[0])
    cegi.parse()
    cegi.make_dirstruct()

    readerInstance = PoemReader()
    ps, psa = readerInstance.getProfiles()

    # write profiles
    for writer in writers:
        if writer == 'file':
            writerInstance = FileWriter(cegi.tenantdir)
            writerInstance.writeProfiles(ps, timestamp)

    for vo in cvo.get_vos():
        for job in cvo.get_jobs(vo):
            jobdir = cvo.get_fulldir(job)

            voprofiles = cvo.get_profiles(job)
            lfprofiles = gen_outprofiles(psa, voprofiles)

            filename = jobdir + globopts['OutputPoem']% timestamp
            avro = AvroWriter(globopts['AvroSchemasPoem'], filename, lfprofiles)
            avro.write()

    for job in cegi.get_jobs():
        jobdir = cegi.get_fulldir(job)

        jobprofiles = cegi.get_profiles(job)
        lfprofiles = gen_outprofiles(psa, jobprofiles)

        filename = jobdir + globopts['OutputPoem'] % timestamp
        avro = AvroWriter(globopts['AvroSchemasPoem'], filename, lfprofiles)
        avro.write()

main()
