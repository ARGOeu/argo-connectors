import configparser
import errno
import os
import re

from .log import Logger


class Global(object):
    """
       Class represents parser for global.conf
    """
    # options common for all connectors
    conf_general = {'General': ['WriteAvro', 'PublishWebAPI', 'PassExtensions']}
    conf_auth = {'Authentication': ['HostKey', 'HostCert', 'CAPath', 'CAFile',
                                    'VerifyServerCert', 'UsePlainHttpAuth',
                                    'HttpUser', 'HttpPass']}
    conf_conn = {'Connection': ['Timeout', 'Retry', 'SleepRetry']}
    conf_state = {'InputState': ['SaveDir', 'Days']}
    conf_webapi = {'WebAPI': ['Token', 'Host']}

    # options specific for every connector
    conf_topo_schemas = {'AvroSchemas': ['TopologyGroupOfEndpoints',
                                         'TopologyGroupOfGroups']}
    conf_topo_output = {'Output': ['TopologyGroupOfEndpoints',
                                   'TopologyGroupOfGroups']}
    conf_downtimes_schemas = {'AvroSchemas': ['Downtimes']}
    conf_downtimes_output = {'Output': ['Downtimes']}
    conf_weights_schemas = {'AvroSchemas': ['Weights']}
    conf_weights_output = {'Output': ['Weights']}
    conf_metricprofile_output = {'Output': ['MetricProfile']}
    conf_metricprofile_schemas = {'AvroSchemas': ['MetricProfile']}

    def __init__(self, caller, confpath=None, **kwargs):
        self.optional = dict()

        self.logger = Logger(str(self.__class__))
        self._filename = '/etc/argo-connectors/global.conf' if not confpath else confpath
        self._checkpath = kwargs['checkpath'] if 'checkpath' in kwargs.keys() else False

        self.optional.update(self._lowercase_dict(self.conf_auth))
        self.optional.update(self._lowercase_dict(self.conf_webapi))

        self.shared_secopts = self._merge_dict(self.conf_general,
                                               self.conf_auth, self.conf_conn,
                                               self.conf_state,
                                               self.conf_webapi)
        self.secopts = {
            'topology-gocdb-connector.py':
            self._merge_dict(self.shared_secopts,
                                self.conf_topo_schemas,
                                self.conf_topo_output),
            'topology-json-connector.py':
            self._merge_dict(self.shared_secopts,
                                self.conf_topo_schemas,
                                self.conf_topo_output),
            'topology-provider-connector.py':
            self._merge_dict(self.shared_secopts,
                                self.conf_topo_schemas,
                                self.conf_topo_output),
            'downtimes-csv-connector.py':
            self._merge_dict(self.shared_secopts,
                                self.conf_downtimes_schemas,
                                self.conf_downtimes_output),
            'downtimes-gocdb-connector.py':
            self._merge_dict(self.shared_secopts,
                                self.conf_downtimes_schemas,
                                self.conf_downtimes_output),
            'weights-vapor-connector.py':
            self._merge_dict(self.shared_secopts,
                                self.conf_weights_schemas,
                                self.conf_weights_output),
            'topology-csv-connector.py':
            self._merge_dict(self.shared_secopts,
                                self.conf_topo_schemas,
                                self.conf_topo_output),
            'metricprofile-webapi-connector.py':
            self._merge_dict(self.shared_secopts,
                                self.conf_metricprofile_schemas,
                                self.conf_metricprofile_output),
            'service-types-gocdb-connector.py':
            self._merge_dict(self.shared_secopts),
            'service-types-csv-connector.py':
            self._merge_dict(self.shared_secopts),
            'service-types-json-connector.py':
            self._merge_dict(self.shared_secopts),

        }

        if caller:
            self.caller_secopts = self.secopts[os.path.basename(caller)]
        else:
            self.caller_secopts = self.shared_secopts

    def _merge_dict(self, *args):
        newd = dict()
        for d in args:
            newd.update(d)
        return newd

    def _lowercase_dict(self, d):
        newd = dict()
        for k in d.keys():
            opts = [o.lower() for o in d[k]]
            newd[k.lower()] = opts
        return newd

    def merge_opts(self, custopt, section):
        newd = custopt.copy()
        opts = [o for o in self.options.keys() if o.startswith(section)]
        for o in opts:
            if o in newd:
                continue
            newd.update({o: self.options[o]})

        return newd

    def is_complete(self, opts, section):
        all = set([section + o for o in self.optional[section]])
        diff = all.symmetric_difference(opts.keys())
        if diff:
            return (False, diff)
        return (True, None)

    def _concat_sectopt(self, d):
        opts = list()

        for k in d.keys():
            for v in d[k]:
                opts.append(k + v)

        return opts

    def _one_active(self, options):
        loweropts = self._lowercase_dict(options)

        lval = [eval(self.options[k]) for k in self._concat_sectopt(loweropts)]

        if any(lval):
            return True
        else:
            return False

    def parse(self):
        config = configparser.ConfigParser()

        if not os.path.exists(self._filename):
            self.logger.error('Could not find %s' % self._filename)
            raise SystemExit(1)

        config.read(self._filename)
        options = {}

        lower_section = [sec.lower() for sec in config.sections()]

        try:
            for sect, opts in self.caller_secopts.items():
                if (sect.lower() not in lower_section and sect.lower() not in
                    self.optional.keys()):
                    raise configparser.NoSectionError(sect.lower())

                for opt in opts:
                    for section in config.sections():
                        if section.lower().startswith(sect.lower()):
                            try:
                                optget = config.get(section, opt)
                                if self._checkpath and os.path.isfile(optget) is False:
                                    raise OSError(errno.ENOENT, optget)

                                if ('output' in section.lower() and 'DATE' not
                                    in optget):
                                    self.logger.error('No DATE placeholder in %s' % opt)
                                    raise SystemExit(1)

                                options.update({(sect + opt).lower(): optget})

                            except configparser.NoOptionError as e:
                                s = e.section.lower()
                                if (s in self.optional.keys() and
                                    e.option in self.optional[s]):
                                    pass
                                else:
                                    raise e

            self.options = options

            if not self._one_active(self.conf_general):
                self.logger.error('At least one of %s needs to be True' % (', '.join(self._concat_sectopt(self.conf_general))))
                raise SystemExit(1)

        except configparser.NoOptionError as e:
            self.logger.error(e.message)
            raise SystemExit(1)
        except configparser.NoSectionError as e:
            self.logger.error("%s defined" % (e.args[0]))
            raise SystemExit(1)
        except OSError as e:
            self.logger.error('%s %s' % (os.strerror(e.args[0]), e.args[1]))
            raise SystemExit(1)

        return options


class CustomerConf(object):
    """
       Class with parser for customer.conf and additional helper methods
    """
    _custattrs = None
    _cust = {}
    _defjobattrs = {'topology-gocdb-connector.py': [''],
                    'topology-json-connector.py': [''],
                    'topology-csv-connector.py': [''],
                    'topology-provider-connector.py': [''],
                    'metricprofile-webapi-connector.py': ['MetricProfileNamespace'],
                    'downtimes-gocdb-connector.py': ['DowntimesFeed', 'TopoUIDServiceEndpoints'],
                    'downtimes-csv-connector.py': ['DowntimesFeed', 'TopoUIDServiceEndpoints'],
                    'weights-vapor-connector.py': ['WeightsFeed',
                                                   'TopoFetchType'],
                    'service-types-gocdb-connector.py': ['ServiceTypesFeed'],
                    'service-types-csv-connector.py': ['ServiceTypesFeed'],
                    'service-types-json-connector.py': ['ServiceTypesFeed']
                    }
    _jobs, _jobattrs = {}, None
    _cust_optional = ['AuthenticationUsePlainHttpAuth',
                      'TopoUIDServiceEnpoints', 'AuthenticationHttpUser',
                      'AuthenticationHttpPass', 'BDII', 'BDIIHost', 'BDIIPort',
                      'BDIIQueryBase', 'BDIIQueryFilterSRM',
                      'BDIIQueryAttributesSRM', 'BDIIQueryFilterSEPATH',
                      'BDIIQueryAttributesSEPATH', 'WebAPIToken',
                      'WeightsEmpty', 'DowntimesEmpty', 'ServiceTypesFeed']
    tenantdir = ''
    deftopofeed = 'https://goc.egi.eu/gocdbpi/'

    def __init__(self, caller, confpath, **kwargs):
        self.logger = Logger(str(self.__class__))
        self._filename = '/etc/argo-connectors/customer.conf' if not confpath else confpath
        if not kwargs:
            self._jobattrs = self._defjobattrs[os.path.basename(caller)]
        else:
            if 'jobattrs' in kwargs.keys():
                self._jobattrs = kwargs['jobattrs']
            if 'custattrs' in kwargs.keys():
                self._custattrs = kwargs['custattrs']

    def parse(self):
        config = configparser.ConfigParser()
        if not os.path.exists(self._filename):
            self.logger.error('Could not find %s' % self._filename)
            raise SystemExit(1)
        config.read(self._filename)

        lower_custopt = [oo.lower() for oo in self._cust_optional]

        for section in config.sections():
            if section.lower().startswith('CUSTOMER_'.lower()):
                optopts = dict()

                try:
                    custjobs = config.get(section, 'Jobs').split(',')
                    custjobs = [job.strip() for job in custjobs]
                    custdir = config.get(section, 'OutputDir')
                    custname = config.get(section, 'Name')
                    topofetchtype = config.get(section, 'TopoFetchType')
                    topofeed = config.get(section, 'TopoFeed')
                    topotype = config.get(section, 'TopoType')
                    topouidservendpoints = config.get(section, 'TopoUIDServiceEndpoints', fallback=False)
                    toposcope = config.get(section, 'TopoScope', fallback=None)
                    topofeedsites = config.get(section, 'TopoFeedSites', fallback=None)
                    topofeedendpoints = config.get(section, 'TopoFeedServiceEndpoints', fallback=None)
                    topofeedendpointsextensions = config.get(section, 'TopoFeedServiceEndpointsExtensions', fallback=None)
                    topofeedservicegroups = config.get(section, 'TopoFeedServiceGroups', fallback=None)
                    topofeedpaging = config.get(section, 'TopoFeedPaging', fallback='GOCDB')
                    servicetypesfeed = config.get(section, 'ServiceTypesFeed', fallback=None)
                    downtimesfeed = config.get(section, 'DowntimesFeed', fallback=None)

                    if not custdir.endswith('/'):
                        custdir = '{}/'.format(custdir)

                    for o in lower_custopt:
                        try:
                            code = "optopts.update(%s = config.get(section, '%s'))" % (o, o)
                            exec(code)
                        except configparser.NoOptionError as e:
                            if e.option in lower_custopt:
                                pass
                            else:
                                raise e

                except configparser.NoOptionError as e:
                    self.logger.error(e.message)
                    raise SystemExit(1)

                self._cust.update({section: {'Jobs': custjobs, 'OutputDir':
                                             custdir, 'Name': custname,
                                             'DowntimesFeed': downtimesfeed,
                                             'ServiceTypesFeed': servicetypesfeed,
                                             'TopoFeed': topofeed,
                                             'TopoFeedEndpoints': topofeedendpoints,
                                             'TopoFeedEndpointsExtensions': topofeedendpointsextensions,
                                             'TopoFeedPaging': topofeedpaging,
                                             'TopoFeedServiceGroups': topofeedservicegroups,
                                             'TopoFeedSites': topofeedsites,
                                             'TopoFetchType': topofetchtype,
                                             'TopoScope': toposcope,
                                             'TopoType': topotype,
                                             'TopoUIDServiceEnpoints': topouidservendpoints
                                             }})
                if optopts:
                    auth, webapi, empty_data, bdii = {}, {}, {}, {}
                    for k, v in optopts.items():
                        if k.startswith('authentication'):
                            auth.update({k: v})
                        if k.startswith('webapi'):
                            webapi.update({k: v})
                        if k.startswith('bdii'):
                            bdii.update({k: v})
                        if k.endswith('empty'):
                            empty_data.update({k: v})
                    self._cust[section].update(AuthOpts=auth)
                    self._cust[section].update(WebAPIOpts=webapi)
                    self._cust[section].update(BDIIOpts=bdii)
                    self._cust[section].update(EmptyDataOpts=empty_data)

                if self._custattrs:
                    for attr in self._custattrs:
                        if config.has_option(section, attr):
                            self._cust[section].update({attr: config.get(section, attr)})

        for cust in self._cust:
            for job in self._cust[cust]['Jobs']:
                if config.has_section(job):
                    try:
                        profiles = config.get(job, 'Profiles')
                        dirname = config.get(job, 'Dirname')
                    except configparser.NoOptionError as e:
                        self.logger.error(e.message)
                        raise SystemExit(1)

                    self._jobs.update({job: {'Profiles': profiles, 'Dirname': dirname}})
                    if self._jobattrs:
                        for attr in self._jobattrs:
                            if config.has_option(job, attr):
                                self._jobs[job].update({attr: config.get(job, attr)})
                else:
                    self.logger.error("Could not find Jobs: %s for customer: %s" % (job, cust))
                    raise SystemExit(1)

    def _sect_to_dir(self, sect):
        try:
            match = re.match('(?:^\w+?_)(\w+)', sect)
            assert match != None
            dirname = match.group(1)
        except (AssertionError, KeyError) as e:
            self.logger.error("Could not get Dirname for %s" % e)
            raise SystemExit(1)
        return dirname

    def _dir_from_sect(self, sect, d):
        dirname = ''

        for k, v in d.items():
            if k == sect:
                if 'Dirname' in v.keys():
                    dirname = v['Dirname']
                elif 'OutputDir' in v.keys():
                    dirname = v['OutputDir']
                else:
                    dirname = self._sect_to_dir(sect)

        return dirname

    def get_jobdir(self, job):
        return self._dir_from_sect(job, self._jobs)

    def get_authopts(self, feed=None, jobcust=None):
        if jobcust:
            for job, cust in jobcust:
                if 'AuthOpts' in self._cust[cust]:
                    return self._cust[cust]['AuthOpts']
                else:
                    return dict()
        else:
            return self._get_cust_options('AuthOpts')

    def get_bdiiopts(self, cust=None):
        if cust:
            if 'BDIIOpts' in self._cust[cust]:
                return self._cust[cust]['BDIIOpts']
            else:
                return dict()
        else:
            return self._get_cust_options('BDIIOpts')

    def is_complete_bdii(self, opts):
        diff = []
        for opt in self._cust_optional:
            if opt.lower().startswith('bdii'):
                if opt.lower() not in opts:
                    diff.append(opt)
        if len(diff) > 0:
            return (False, diff)
        return (True, None)

    def get_fulldir(self, cust, job):
        return self.get_custdir(cust) + '/' + self.get_jobdir(job) + '/'

    def get_fullstatedir(self, root, cust, job=None):
        if job:
            return root + '/' + self.get_custname(cust) + '/' + self.get_jobdir(job)
        else:
            return root + '/' + self.get_custname(cust) + '/'

    def get_custdir(self, cust=None):
        if cust:
            return self._dir_from_sect(cust, self._cust)
        else:
            return self._get_cust_options('OutputDir')

    def get_custname(self, cust=None):
        if cust:
            return self._cust[cust]['Name']
        else:
            return self._get_cust_options('Name')

    def get_webapiopts(self, cust=None):
        if cust:
            if 'WebAPIOpts' in self._cust[cust]:
                return self._cust[cust]['WebAPIOpts']
            else:
                return dict()
        else:
            return self._get_cust_options('WebAPIOpts')

    def make_dirstruct(self, root=None):
        dirs = []
        for cust in self._cust.keys():
            for job in self.get_jobs(cust):
                if root:
                    dirs.append(root + '/' + self.get_custname(cust) + '/' + self.get_jobdir(job))
                else:
                    dirs.append(self.get_custdir(cust) + '/' + self.get_jobdir(job))
            for d in dirs:
                try:
                    os.makedirs(d)
                except OSError as e:
                    if e.args[0] != errno.EEXIST:
                        self.logger.error('%s %s %s' % (os.strerror(e.args[0]), e.args[1], d))
                        raise SystemExit(1)

    def get_jobs(self, cust):
        jobs = []
        try:
            jobs = self._cust[cust]['Jobs']
        except KeyError:
            self.logger.error("Could not get Jobs for %s" % cust)
            raise SystemExit(1)
        return jobs

    def get_customers(self):
        return self._cust.keys()

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
            match = re.findall("(\w+)\s*:\s*(\(.*?\))", tagstr)
            if match is not None:
                for m in match:
                    tags.update({m[0]: [e.strip('() ') for e in m[1].split(',')]})
            match = re.findall('([\w]+)\s*:\s*([\w\.\-\_]+)', tagstr)
            if match is not None:
                for m in match:
                    tags.update({m[0]: m[1]})
            else:
                self.logger.error("Could not parse option %s: %s" % (option, tagstr))
                return dict()
        return tags

    def get_gocdb_ggtags(self, job):
        return self._get_tags(job, 'TopoSelectGroupOfGroups')

    def get_gocdb_getags(self, job):
        return self._get_tags(job, 'TopoSelectGroupOfEndpoints')

    def get_vo_ggtags(self, job):
        return self._get_tags(job, 'TopoSelectGroupOfGroups')

    def _get_feed(self, job, key):
        try:
            feed = self._jobs[job][key]
        except KeyError:
            feed = ''
        return feed

    def _get_cust_options(self, opt):
        target_option = None

        # safely assume here only one customer definition in the config file
        for options in self._cust.values():
            for option in options:
                if option.lower() == opt.lower():
                    target_option = options[option]
        return target_option

    def get_downfeed(self):
        return self._get_cust_options('DowntimesFeed')

    def get_topofeed(self):
        return self._get_cust_options('TopoFeed')

    def get_topofeedsites(self):
        return self._get_cust_options('TopoFeedSites')

    def get_topofeedendpoints(self):
        return self._get_cust_options('TopoFeedEndpoints')

    def get_topofeedendpointsextensions(self):
        return self._get_cust_options('TopoFeedEndpointsExtensions')

    def get_topofeedservicegroups(self):
        return self._get_cust_options('TopoFeedServiceGroups')

    def get_topofeedpaging(self):
        return eval(self._get_cust_options('TopoFeedPaging'))

    def get_toposcope(self):
        return self._get_cust_options('TopoScope')

    def get_topofetchtype(self):
        fetchtype = self._get_cust_options('TopoFetchType')
        if ',' in fetchtype:
            fetchtype = [type.strip().lower() for type in fetchtype.split(',')]
        else:
            fetchtype = [fetchtype.lower()]
        return fetchtype

    def get_uidserviceendpoints(self):
        uidservend = self._get_cust_options('TopoUIDServiceEnpoints')
        if isinstance(uidservend, str):
            return eval(uidservend)
        else:
            return False

    def _is_paginated(self, job):
        paging = False

        try:
            paging = self._jobs[job]['TopoFeedPaging']
        except KeyError:
            pass

        return paging

    def _update_feeds(self, feeds, feedurl, job, cust):
        if feedurl in feeds.keys():
            feeds[feedurl].append((job, cust))
        elif feedurl:
            feeds[feedurl] = []
            feeds[feedurl].append((job, cust))

    def is_paginated(self, feed, jobcust):
        paginated = False

        for job, cust in jobcust:
            paginated = self._is_paginated(job)
            if paginated:
                break

        return eval(str(paginated))

    def pass_uidserviceendpoints(self, job):
        if not isinstance(job, set):
            do_pass = False
            try:
                do_pass = eval(self._jobs[job]['TopoUIDServiceEndpoints'])
            except KeyError:
                pass

            return do_pass
        else:
            ret = list()

            for jb in job:
                try:
                    do_pass = eval(self._jobs[jb]['TopoUIDServiceEndpoints'])
                    ret.append(do_pass)
                except KeyError:
                    ret.append(False)
            return ret

    def get_mapfeedjobs(self, caller, name=None, deffeed=None):
        feeds = {}
        for c in self.get_customers():
            for job in self.get_jobs(c):
                if 'downtimes' in caller:
                    feedurl = self._get_feed(job, 'DowntimesFeed')
                    if feedurl:
                        self._update_feeds(feeds, feedurl, job, c)
                    else:
                        feedurl = deffeed
                        self._update_feeds(feeds, feedurl, job, c)
                elif 'weights' in caller:
                    feedurl = self._get_feed(job, 'WeightsFeed')
                    if feedurl:
                        self._update_feeds(feeds, feedurl, job, c)
                    else:
                        feedurl = deffeed
                        self._update_feeds(feeds, feedurl, job, c)

        return feeds

    def send_empty(self, caller, cust=None):
        try:
            if cust:
                if 'downtimes' in caller:
                    return eval(self._cust[cust]['EmptyDataOpts']['downtimesempty'])
                elif 'weights' in caller:
                    return eval(self._cust[cust]['EmptyDataOpts']['weightsempty'])
            else:
                if 'downtimes' in caller:
                    return eval(self._get_cust_options('EmptyDataOpts')['downtimesempty'])
                elif 'weights' in caller:
                    return eval(self._get_cust_options('EmptyDataOpts')['weightsempty'])
        except KeyError:
            return False

    def get_namespace(self, job):
        namespace = None
        try:
            namespace = self._jobs[job]['MetricProfileNamespace']
        except KeyError:
            pass

        return namespace

    def get_servicesfeed(self):
        feed = self._get_cust_options('ServiceTypesFeed')
        if feed:
            return feed
        else:
            return self._get_cust_options('TopoFeed')
