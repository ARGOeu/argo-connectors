import configparser
import errno
import os
import copy
import re

from argo_connectors.log import Logger
from argo_connectors.config.glob import Global
from argo_connectors.exceptions import ConnectorConfError


class AuthOpts(object):
    def __init__(self):
        auth_custopts = Customer._get_cust_options('AuthOpts')
        self.auth_opts = Global.merge_opts(auth_custopts, 'authentication')

        auth_complete, missing = Global.is_complete(self.auth_opts, 'authentication')
        self.missing = None
        if not auth_complete:
            self.missing = missing

    @property
    def opts(self):
        if self.missing:
            return None
        else:
            return self.auth_opts


class WebAPIOpts(object):
    def __init__(self):
        webapi_custopts = Customer._get_cust_options('WebAPIOpts')
        self.webapi_opts = Global.merge_opts(webapi_custopts, 'webapi')
        webapi_complete, missopt = Global.is_complete(self.webapi_opts, 'webapi')
        self.missing = None
        if not webapi_complete:
            self.missing = missopt

    @property
    def opts(self):
        if self.missing:
            return None
        else:
            return self.webapi_opts


class BDIIOpts(object):
    def __init__(self):
        self.bdii_custopts = Customer._get_cust_options('BDIIOpts')
        self.missing = None
        if self.bdii_custopts:
            bdii_complete, missing = Customer.is_complete_bdii(self.bdii_custopts)
            if not bdii_complete:
                self.missing = missing

    @property
    def opts(self):
        if self.missing:
            return None
        else:
            for key, value in self.bdii_custopts.items():
                if isinstance(value, str) and value in ['False', 'True']:
                    self.bdii_custopts[key] = eval(value)
            return self.bdii_custopts


class _CustomerConf(object):
    """
       Class with parser for customer.conf and additional helper methods
    """
    _custattrs = None
    _cust = {}
    _defjobattrs = {
        'topology-gocdb-connector.py': [''],
        'topology-json-connector.py': [''],
        'topology-csv-connector.py': [''],
        'topology-provider-connector.py': [''],
        'topology-combiner.py': [''],
        'downtimes-combiner.py': [''],
        'service-types-combiner.py': [''],
        'topology-lot1sc-connector.py': [''],
        'downtimes-gocdb-connector.py': ['DowntimesFeed', 'TopoUIDServiceEndpoints'],
        'downtimes-csv-connector.py': ['DowntimesFeed', 'TopoUIDServiceEndpoints'],
        'weights-vapor-connector.py': ['WeightsFeed',
                                       'TopoFetchType'],
        'service-types-gocdb-connector.py': ['ServiceTypesFeed'],
        'service-types-csv-connector.py': ['ServiceTypesFeed'],
        'service-types-json-connector.py': ['ServiceTypesFeed']
    }
    _jobs, _jobattrs = {}, None
    _cust_optional = [
        'AuthenticationUsePlainHttpAuth',
        'TopoUIDServiceEndpoints', 'AuthenticationHttpUser',
        'AuthenticationHttpPass', 'BDII', 'BDIIHost', 'BDIIPort',
        'BDIIQueryBase', 'BDIIQueryFilterSRM',
        'BDIIQueryAttributesSRM', 'BDIIQueryFilterSEPATH',
        'BDIIQueryAttributesSEPATH', 'WebAPIToken',
        'WeightsEmpty', 'DowntimesEmpty', 'ServiceTypesFeed',
        'HonorNotificationFlag', 'TopoTiers'
    ]
    tenantdir = ''
    deftopofeed = 'https://goc.egi.eu/gocdbpi/'

    def __call__(self, caller=None, confpath=None, **kwargs):
        if caller:
            self.__init__(caller, confpath, **kwargs)
        else:
            self.__init__(str(self.__class__), confpath, **kwargs)
        return self

    def __init__(self, caller, confpath=None, combiner=None, **kwargs):
        self.caller = caller
        self.combiner = combiner
        self._filename = f"{os.environ['VIRTUAL_ENV']}/etc/customer.conf" if not confpath else confpath
        try:
            if not kwargs:
                self._jobattrs = self._defjobattrs[os.path.basename(caller)]
            else:
                if 'jobattrs' in kwargs.keys():
                    self._jobattrs = kwargs['jobattrs']
                if 'custattrs' in kwargs.keys():
                    self._custattrs = kwargs['custattrs']

            self.parse()

            self.auth_opts = AuthOpts()
            self.webapi_opts = WebAPIOpts()
            self.bdii_opts = BDIIOpts()

        except KeyError:
            pass

    def parse(self):
        config = configparser.ConfigParser()
        if not os.path.exists(self._filename):
            raise ConnectorConfError('Could not find %s' % self._filename)
        try:
            config.read(self._filename)
        except (configparser.DuplicateOptionError) as e:
            raise ConnectorConfError(e.message)

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
                    topofeed = config.get(section, 'TopoFeed', fallback=None)
                    vaporpi = config.get(section, 'Vaporpi', fallback=None)
                    topotype = config.get(section, 'TopoType')
                    topouidservendpoints = config.get(
                        section, 'TopoUIDServiceEndpoints', fallback=False)
                    toposcope = config.get(section, 'TopoScope', fallback=None)
                    topotiers = config.get(section, 'TopoTiers', fallback=list())
                    topofeedsites = config.get(
                        section, 'TopoFeedSites', fallback=None)
                    topofeedendpoints = config.get(
                        section, 'TopoFeedServiceEndpoints', fallback=None)
                    topofeedendpointsextensions = config.get(
                        section, 'TopoFeedServiceEndpointsExtensions', fallback=None)
                    topofeedservicegroups = config.get(
                        section, 'TopoFeedServiceGroups', fallback=None)
                    oidctoken = config.get(
                        section, 'OIDCRefreshToken', fallback=None)
                    oidctokenapi = config.get(
                        section, 'OIDCTokenEndpoint', fallback=None)
                    oidcclientid = config.get(
                        section, 'OIDCClientId', fallback=None)
                    topofeedpaging = config.get(
                        section, 'TopoFeedPaging', fallback='GOCDB')
                    servicetypesfeed = config.get(
                        section, 'ServiceTypesFeed', fallback=None)
                    downtimesfeed = config.get(
                        section, 'DowntimesFeed', fallback=None)
                    notifflag = config.getboolean(
                        section, 'HonorNotificationFlag', fallback=None)
                    topotagserviceendpoints = config.get(section, 'TopoTagServiceEndpoints', fallback=list())
                    topotagsites = config.get(section, 'TopoTagSites', fallback=list())
                    topotagservicegroups = config.get(section, 'TopoTagServiceGroups', fallback=list())
                    toposettype = config.get(section, 'TopoSetType', fallback='')

                    if not custdir.endswith('/'):
                        custdir = '{}/'.format(custdir)

                    for o in lower_custopt:
                        try:
                            code = "optopts.update(%s = config.get(section, '%s'))" % (
                                o, o)
                            exec(code)
                        except configparser.NoOptionError as e:
                            if e.option in lower_custopt:
                                pass
                            else:
                                raise e

                except (configparser.NoOptionError) as e:
                    raise ConnectorConfError(e.message)

                self._cust.update({section: {
                    'Jobs': custjobs,
                    'OutputDir': custdir, 'Name': custname,
                    'DowntimesFeed': downtimesfeed,
                    'ServiceTypesFeed': servicetypesfeed,
                    'TopoFeed': topofeed,
                    'Vaporpi': vaporpi,
                    'TopoFeedEndpoints': topofeedendpoints,
                    'TopoFeedServiceEndpoints': topofeedendpoints,
                    'TopoFeedEndpointsExtensions': topofeedendpointsextensions,
                    'TopoFeedServiceEndpointsExtensions': topofeedendpointsextensions,
                    'TopoFeedPaging': topofeedpaging,
                    'TopoFeedServiceGroups': topofeedservicegroups,
                    'TopoFeedSites': topofeedsites,
                    'TopoFetchType': topofetchtype,
                    'TopoScope': toposcope,
                    'TopoTiers': topotiers,
                    'TopoType': topotype,
                    'TopoSetType': toposettype,
                    'TopoTagServiceEndpoints': topotagserviceendpoints or list(),
                    'TopoTagServiceGroups': topotagservicegroups or list(),
                    'TopoTagSites': topotagsites or list(),
                    'TopoUIDServiceEndpoints': topouidservendpoints,
                    'HonorNotificationFlag': notifflag,
                    'OIDCTokenEndpoint': oidctokenapi,
                    'OIDCRefreshToken': oidctoken,
                    'OIDCClientId': oidcclientid
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
                            self._cust[section].update(
                                {attr: config.get(section, attr)})

        for cust in self._cust:
            for job in self._cust[cust]['Jobs']:
                if config.has_section(job):
                    try:
                        dirname = config.get(job, 'Dirname')
                    except configparser.NoOptionError as e:
                        raise ConnectorConfError(e.message)

                    self._jobs.update(
                        {job: {'Dirname': dirname}})
                    if self._jobattrs:
                        for attr in self._jobattrs:
                            if config.has_option(job, attr):
                                self._jobs[job].update(
                                    {attr: config.get(job, attr)})
                else:
                    raise ConnectorConfError("Could not find Jobs: %s for customer: %s" % (job, cust))

    def valid(self):
        isok = True

        if not self.auth_opts:
            Logger.error('%s options incomplete, missing %s' %
                         ('authentication', ' '.join(self.auth_opts.missing)))
            isok = False

        if not self.webapi_opts.opts:
            Logger.error('%s options incomplete, missing %s' %
                         ('webapi', ' '.join(self.webapi_opts.missing)))
            isok = False

        if self.bdii_opts.missing:
            Logger.error('%s options incomplete, missing %s' %
                         ('bdii', ' '.join(self.bdii_opts.missing)))
            isok = False

        if not isok:
            raise ConnectorConfError('Not properly configured')
        else:
            return True

    def _sect_to_dir(self, sect):
        try:
            match = re.match('(?:^\w+?_)(\w+)', sect)
            assert match != None
            dirname = match.group(1)
        except (AssertionError, KeyError) as e:
            raise ConnectorConfError("Could not get Dirname for %s" % e)
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

    def make_dirstruct(self, root=None, jobdir=True):
        dirs = []
        for cust in self._cust.keys():
            for job in self.get_jobs(cust):
                if root:
                    if jobdir:
                        dirs.append(root + '/' + self.get_custname(cust) + '/' + self.get_jobdir(job))
                    else:
                        dirs.append(root + '/' + self.get_custname(cust))
                else:
                    if jobdir:
                        dirs.append(self.get_custdir(cust) + '/' + self.get_jobdir(job))
                    else:
                        dirs.append(self.get_custdir(cust) + '/' + self.get_custname(cust))
            for d in dirs:
                try:
                    os.makedirs(d)
                except OSError as e:
                    if e.args[0] != errno.EEXIST:
                        raise ConnectorConfError('%s %s %s' % (
                            os.strerror(e.args[0]), e.args[1], d))

    def get_jobs(self, cust):
        jobs = []
        try:
            jobs = self._cust[cust]['Jobs']
        except KeyError:
            raise ConnectorConfError("Could not get Jobs for %s" % cust)
        return jobs

    def get_customers(self):
        if self._cust:
            return self._cust.keys()

    def get_fetchtype(self, job):
        return self._jobs[job]['TopoFetchType']

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

    def opt(self, option):
        ret_opt = self._get_cust_options(option)
        if isinstance(ret_opt, str) and ret_opt in ['False', 'True']:
            return eval(ret_opt)
        elif isinstance(ret_opt, str) and ',' in ret_opt:
            ret_opt_arr = [op.strip().lower() for op in ret_opt.split(',')]
            return ret_opt_arr
        else:
            return ret_opt

    @property
    def custopts(self):
        for (option, value) in self._cust.items():
            return self._cust[option]

    def get_topofetchtype(self):
        fetchtype = self._get_cust_options('TopoFetchType')

        if fetchtype:
            if ',' in fetchtype:
                fetchtype = [type.strip().lower() for type in fetchtype.split(',')]
            else:
                fetchtype = [fetchtype.lower()]
        else:
            fetchtype = ['ServiceGroups'.lower()]

        return fetchtype

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

    def configure(self, newoptions):
        exist_options = list(self._cust)
        exist_options = self._cust[exist_options[0]]

        for newopt in newoptions['config']:
            if newopt in exist_options:
                exist_options[newopt] = newoptions['config'][newopt]
            if newopt.lower().startswith('authentication'):
                exist_options['AuthOpts'][newopt.lower()] = newoptions['config'][newopt]
                self.auth_opts.opts[newopt.lower()] = newoptions['config'][newopt]
            if newopt.lower().startswith('BDII'):
                exist_options['BDIIOpts'][newopt.lower()] = newoptions['config'][newopt]


class _CombinerCustomerConf(object):
    def __init__(self, connector=None, combuid=None, tenant_name=None):
        self.combuid = combuid
        self.tenant_name = tenant_name

        if not getattr(self, 'combinit', None):
            self.combinit = dict()
        if not connector:
            self.combinit[combuid] = _CustomerConf('config/customer.py')
        else:
            self.combinit[combuid] = _CustomerConf(connector)

        if tenant_name:
            self._preconf_tenantname()

    def __call__(self, connector=None, combuid=None, tenant_name=None):
        self.__init__(connector, combuid, tenant_name)
        return self.combinit[combuid]

    def _preconf_tenantname(self):
        optdict = copy.deepcopy(self.combinit[self.combuid]._cust)
        key_sample, key_orig = None, None

        for key, value in optdict.items():
            key_tenant, key_orig = key, key
            break

        key_sample = key_tenant.split('_')
        new_key = f'{key_sample[0]}_{self.tenant_name}'
        optdict[new_key] = optdict.pop(key_orig)

        if not optdict[new_key].get('Name', None) or not optdict[new_key].get('OutputDir', None):
            raise ConnectorConfError('Default configuration missing keys')

        optdict[new_key]['Name'] = self.tenant_name
        optdict[new_key]['OutputDir'] = optdict[new_key]['OutputDir'].replace(
            key_sample[1],
            self.tenant_name
        )

        del self.combinit[self.combuid]._cust[key_orig]
        self.combinit[self.combuid]._cust = optdict

    def get_conf(self, combuid):
        return self.combinit[combuid]


def get_custconf(combuid):
    if combuid:
        return CombinerCustomer.get_conf(combuid)
    else:
        return Customer


Customer = _CustomerConf('config/customer.py')
CombinerCustomer = _CombinerCustomerConf('config/customer.py')
