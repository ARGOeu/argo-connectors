import ConfigParser
import os, re, errno


class Global:
    def __init__(self, *args, **kwargs):
        self._args = args
        self._filename = '/etc/argo-egi-connectors/global.conf'
        self._checkpath = kwargs['checkpath'] if 'checkpath' in kwargs.keys() else False

    def parse(self):
        config = ConfigParser.ConfigParser()
        config.optionxform = str
        if not os.path.exists(self._filename):
            print self.__class__, 'Could not find %s' % self._filename
            raise SystemExit(1)
        config.read(self._filename)
        options = {}

        try:
            for arg in self._args:
                for sect, opts in arg.items():
                    for opt in opts:
                        optget = config.get(sect, opt)
                        if self._checkpath and sect != 'URL' and os.path.isfile(optget) is False:
                            raise OSError(errno.ENOENT, optget)
                        options.update({sect+opt: optget})
        except ConfigParser.NoOptionError as e:
            # TODO: syslog
            print self.__class__, "No option '%s' in section: '%s'" % (e.args[0], e.args[1])
            raise SystemExit(1)
        except ConfigParser.NoSectionError as e:
            # TODO: syslog
            print self.__class__, "No section '%s' defined" % (e.args[0])
            raise SystemExit(1)
        except OSError as e:
            print self.__class__, os.strerror(e.args[0]), e.args[1], optget
            raise SystemExit(1)

        return options

class PoemConf:
    options = {}

    def __init__(self, *args):
        self._args = args
        self._filename = '/etc/argo-egi-connectors/poem-connector.conf'

    def parse(self):
        config = ConfigParser.ConfigParser()
        config.optionxform = str
        if not os.path.exists(self._filename):
            print self.__class__, 'Could not find %s' % self._filename
            raise SystemExit(1)
        config.read(self._filename)

        try:
            for arg in self._args:
                for sect, opts in arg.items():
                    for opt in opts:
                        for section in config.sections():
                            if section.startswith(sect):
                                lopts = config.options(section)
                                for o in lopts:
                                    if o.startswith(opt):
                                        optget = config.get(section, o)
                                        self.options.update({section+o: optget})

        except ConfigParser.NoOptionError as e:
            # TODO: syslog
            print self.__class__, "No option '%s' in section: '%s'" % (e.args[0], e.args[1])
            raise SystemExit(1)
        except ConfigParser.NoSectionError as e:
            # TODO: syslog
            print self.__class__, "No section '%s' defined" % (e.args[0])
            raise SystemExit(1)

        return self.options

    def _get_ngis(self, option):
        ngis = {}

        def filtkey(elem):
            if option in elem and not\
                    re.search('Profiles[0-9]*', elem):
                return True
        for opt in filter(filtkey, self.options.keys()):
            match = re.search('(%s)([0-9]+$)' % option, opt)
            if match:
                value = match.group(1)+'Profiles'+match.group(2)
                ngis.update({self.options[opt]:
                                re.split('\s*,\s*', self.options[value])})
            elif option == opt:
                ngis.update({self.options[opt]:
                                re.split('\s*,\s*', self.options[opt+'Profiles'])})
        return ngis

    def get_allngi(self):
        return self._get_ngis('PrefilterDataAllNGI')

    def get_allowedngi(self):
        return self._get_ngis('PrefilterDataAllowedNGI')

    def get_servers(self):
        poemservers = {}
        for opt in self.options.keys():
            if 'PoemServer' in opt:
                key = re.search('\w+[0-9]+', opt)
                key = 'PoemServer' if not key else key.group(0)
                poemservers.update({self.options[key+'Host']:
                                    re.split('\s*,\s*', self.options[key+'VO'])})
        return poemservers

class PrefilterConf(Global):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._filename = '/etc/argo-egi-connectors/prefilter-egi.conf'
        self._checkpath = kwargs['checkpath'] if 'checkpath' in kwargs.keys() else False

class EGIConf:
    _egiattrs = None
    _egi = {}
    _defjobattrs = {'topology-gocdb-connector.py' : ['TopoFetchType',
                                                  'TopoSelectGroupOfGroups',
                                                  'TopoSelectGroupOfEndpoints',
                                                  'Dirname'],
                    'poem-connector.py': ['Dirname'],
                    'downtimes-gocdb-connector.py': ['Dirname'],
                    'weights-gstat-connector.py': ['Dirname'],
                    'prefilter-egi.py': []}
    _jobs, _jobattrs = {}, None
    tenantdir = ''

    def __init__(self, caller=None, **kwargs):
        self._filename = '/etc/argo-egi-connectors/customer.conf'
        if not kwargs:
            for c in self._defjobattrs.keys():
                if c in caller:
                    caller = c
            self._jobattrs = self._defjobattrs[caller]
        else:
            if 'jobattrs' in kwargs.keys():
                self._jobattrs = kwargs['jobattrs']
            if 'egiattrs' in kwargs.keys():
                self._egiattrs = kwargs['egiattrs']

    def parse(self):
        config = ConfigParser.ConfigParser()
        config.optionxform = str
        if not os.path.exists(self._filename):
            print self.__class__, 'Could not find %s' % self._filename
            raise SystemExit(1)
        config.read(self._filename)

        for section in config.sections():
            try:
                self.tenantdir = config.get('DIR', 'OutputDir')
            except ConfigParser.NoOptionError as e:
                # TODO: syslog
                print self.__class__, "No option '%s' in section: '%s'" % (e.args[0], e.args[1])
                raise SystemExit(1)
            except ConfigParser.NoSectionError as e:
                # TODO: syslog
                print self.__class__, "No section '%s' defined" % (e.args[0])
                raise SystemExit(1)

            if section.startswith('CUSTOMER'):
                try:
                    egijobs = config.get(section, 'Jobs').split(',')
                    egijobs = [job.strip() for job in egijobs]
                except ConfigParser.NoOptionError as e:
                    # TODO: syslog
                    print self.__class__, "No option '%s' in section: '%s'" % (e.args[0], e.args[1])
                    raise SystemExit(1)

                self._egi.update({section: {'Jobs': egijobs}})
                if self._egiattrs:
                    for attr in self._egiattrs:
                        if config.has_option(section, attr):
                            self._egi[section].update({attr: config.get(section, attr)})

        for job in self._egi['CUSTOMER']['Jobs']:
            if not job.startswith('JOB_'):
                print self.__class__, "Referenced job %s must start with JOB_" % job
                raise SystemExit(1)

            if config.has_section(job):
                try:
                    profiles = config.get(job, 'Profiles')
                except ConfigParser.NoOptionError as e:
                    # TODO: syslog
                    print self.__class__, "No option '%s' in section: '%s'" % (e.args[0], e.args[1])
                    raise SystemExit(1)

                self._jobs.update({job: {'Profiles': profiles}})
                if self._jobattrs:
                    for attr in self._jobattrs:
                        if config.has_option(job, attr):
                            self._jobs[job].update({attr: config.get(job, attr)})
            else:
                print self.__class__, "Could not find Jobs: %s for EGI" % job
                raise SystemExit(1)

    def _sect_to_dir(self, sect):
        try:
            match = re.match('(?:^\w+?_)(\w+)', sect)
            assert match != None
            dirname = match.group(1)
        except (AssertionError, KeyError) as e:
            # TODO: syslog
            print self.__class__, "Could not get Dirname for %s" % e
            raise SystemExit(1)
        return dirname

    def _dir_from_sect(self, sect, d):
        dirname = ''

        for k, v in d.items():
            if k == sect:
                if 'Dirname' in v.keys():
                    dirname = v['Dirname']
                else:
                    dirname = self._sect_to_dir(sect)

        return dirname

    def get_jobdir(self, job):
        return self._dir_from_sect(job, self._jobs)

    def get_fulldir(self, job):
        return self.tenantdir + '/' + self.get_jobdir(job) + '/'

    def make_dirstruct(self):
        dirs = []
        for job in self.get_jobs():
            dirs.append( self.tenantdir+'/'+self.get_jobdir(job))
        for d in dirs:
            try:
                os.makedirs(d)
            except OSError as e:
                if e.args[0] != errno.EEXIST:
                    print self.__class__,  os.strerror(e.args[0]), e.args[1], d
                    raise SystemExit(1)

    def get_jobs(self):
        jobs = []
        try:
            jobs = self._egi['CUSTOMER']['Jobs']
        except KeyError:
            # TODO: syslog
            print self.__class__, "Could not get Jobs for EGI"
            raise SystemExit(1)
        return jobs


    def get_profiles(self, job):
        profiles = self._jobs[job]['Profiles'].split(',')
        for i, p in enumerate(profiles):
            profiles[i] = p.strip()
        return profiles

    def get_fetchtype(self, job):
        return self._jobs[job]['TopoFetchType']

    def _get_tags(self, job, option):
        tags = {}
        if option in self._jobs[job].keys():
            tagstr = self._jobs[job][option]
            for tag in tagstr.split(','):
                mt = re.match('\s*(\w+)\s*:\s*(\w+)\s*', tag)
                if mt is not None:
                    tkey = mt.group(1)
                    tvalue = mt.group(2)
                    tags.update({tkey: tvalue})
                else:
                    print self.__class__, "Could not parse option %s: %s" % (option, tag)
        return tags

    def get_ggtags(self, job):
        return self._get_tags(job, 'TopoSelectGroupOfGroups')

    def get_getags(self, job):
        return self._get_tags(job, 'TopoSelectGroupOfEndpoints')

class VOConf:
    _defjobattrs = {'topology-vo-connector.py': ['TopoSelectGroupOfGroups',
                                                 'Dirname'],
                    'poem-connector.py': ['Dirname'],
                    'downtimes-gocdb-connector.py': ['Dirname'],
                    'weights-gstat-connector.py': ['Dirname']}

    _defvoattrs = {'topology-vo-connector.py': [],
                   'poem-connector.py': [],
                   'downtimes-gocdb-connector.py': [],
                   'weights-gstat-connector.py': []}
    _vo, _voattrs = {}, None
    _jobs, _jobattrs = {}, None
    tenantdir = ''

    def __init__(self, caller=None, **kwargs):
        self._filename = '/etc/argo-egi-connectors/customer.conf'
        if not kwargs:
            for c in self._defjobattrs.keys():
                if c in caller:
                    caller = c
            self._jobattrs = self._defjobattrs[caller]
            self._voattrs = self._defvoattrs[caller]
        else:
            if 'jobattrs' in kwargs.keys():
                self._jobattrs = kwargs['jobattrs']
            if 'voattrs' in kwargs.keys():
                self._voattrs = kwargs['voattrs']

    def parse(self):
        config = ConfigParser.ConfigParser()
        config.optionxform = str
        if not os.path.exists(self._filename):
            print self.__class__, 'Could not find %s' % self._filename
            raise SystemExit(1)
        config.read(self._filename)

        for section in config.sections():
            try:
                self.tenantdir = config.get('DIR', 'OutputDir')
            except ConfigParser.NoOptionError as e:
                # TODO: syslog
                print self.__class__, "No option '%s' in section: '%s'" % (e.args[0], e.args[1])
                raise SystemExit(1)
            except ConfigParser.NoSectionError as e:
                # TODO: syslog
                print self.__class__, "No section '%s' defined" % (e.args[0])
                raise SystemExit(1)

            if section.startswith('VO_'):
                try:
                    vojobs = config.get(section, 'Jobs').split(',')
                    vojobs = [job.strip() for job in vojobs]
                    vofeed = config.get(section, 'VOFeed')
                except ConfigParser.NoOptionError as e:
                    # TODO: syslog
                    print self.__class__, "No option '%s' in section: '%s'" % (e.args[0], e.args[1])
                    raise SystemExit(1)

                self._vo.update({section: {'Jobs': vojobs, 'VOFeed': vofeed}})

                if self._voattrs:
                    for attr in self._voattrs:
                        if config.has_option(section, attr):
                            self._vo[section].update({attr: config.get(section, attr)})

        for vo in self._vo:
            for job in self._vo[vo]['Jobs']:
                if not job.startswith('JOB_'):
                    print self.__class__, "Referenced job %s must start with JOB_" % job
                    raise SystemExit(1)
                if config.has_section(job):
                    try:
                        profiles = config.get(job, 'Profiles')
                    except ConfigParser.NoOptionError as e:
                        # TODO: syslog
                        print self.__class__, "No option '%s' in section: '%s'" % (e.args[0], e.args[1])
                        raise SystemExit(1)

                    self._jobs.update({job: {'Profiles': profiles}})
                    if self._jobattrs:
                        for attr in self._jobattrs:
                            if config.has_option(job, attr):
                                self._jobs[job].update({attr: config.get(job, attr)})
                else:
                    print self.__class__, "Could not find Jobs: %s for VO: %s" % (job, vo)
                    raise SystemExit(1)

    def _sect_to_dir(self, sect):
        try:
            match = re.match('(?:^\w+?_)(\w+)', sect)
            assert match != None
            dirname = match.group(1)
        except (AssertionError, KeyError) as e:
            # TODO: syslog
            print self.__class__, "Could not get Dirname for %s" % e
            raise SystemExit(1)
        return dirname

    def _dir_from_sect(self, sect, d):
        dirname = ''

        for k, v in d.items():
            if k == sect:
                if 'Dirname' in v.keys():
                    dirname = v['Dirname']
                else:
                    dirname = self._sect_to_dir(sect)

        return dirname


    def get_jobdir(self, job):
        return self._dir_from_sect(job, self._jobs)

    def get_fulldir(self, job):
        return self.tenantdir + '/' + self.get_jobdir(job) + '/'

    def make_dirstruct(self):
        dirs = []
        for vo in self._vo.keys():
            for job in self.get_jobs(vo):
                dirs.append(self.tenantdir+'/'+self.get_jobdir(job))
        for d in dirs:
            try:
                os.makedirs(d)
            except OSError as e:
                if e.args[0] != errno.EEXIST:
                    print self.__class__, os.strerror(e.args[0]), e.args[1], d
                    raise SystemExit(1)

    def get_feeds(self):
        feeds = []

        for k, v in self._vo.items():
            if 'VOFeed' in v.keys():
                feeds.append((k, v['VOFeed']))

        return feeds

    def get_vos(self):
        return self._vo.keys()

    def get_jobs(self, vo):
        jobs = []
        try:
            jobs = self._vo[vo]['Jobs']
        except KeyError:
            # TODO: syslog
            print self.__class__, "Could not get Jobs for %s" % vo
            raise SystemExit(1)
        return jobs

    def get_profiles(self, job):
        profiles = self._jobs[job]['Profiles'].split(',')
        for i, p in enumerate(profiles):
            profiles[i] = p.strip()
        return profiles

    def get_ggtags(self, job):
        if 'TopoSelectGroupOfGroups' in self._jobs[job].keys():
            t = self._jobs[job]['TopoSelectGroupOfGroups']
            match = re.match("\s*(\w+)\s*:\s*(\(.*\))", t)
            if match is not None:
                tkey = match.group(1)
                tvalue = match.group(2).strip("() ")
                tvalue = re.split("\s*,\s*", tvalue)
                return {tkey: tvalue}
            else:
                match = re.match("\s*(\w+)\s*:(.*)", t)
                if match is not None:
                    tkey = match.group(1)
                    tvalue = match.group(2).strip()
                    return {tkey: [tvalue]}
                else:
                    print self.__class__, "Could not parse option TopoSelectGroupOfGroups: %s" % t
                    return {}
        else:
            return {}
