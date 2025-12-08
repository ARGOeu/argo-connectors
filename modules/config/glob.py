import configparser
import errno
import os

from argo_connectors.exceptions import ConnectorConfError


class _GlobalConf(object):
    """
       Class represents parser for global.conf
    """
    # options common for all connectors
    conf_general = {'General': ['WriteJson',
                                'PublishWebAPI', 'PassExtensions', 'CompressJson']}
    conf_auth = {'Authentication': ['HostKey', 'HostCert', 'CAPath', 'CAFile',
                                    'VerifyServerCert', 'UsePlainHttpAuth',
                                    'HttpUser', 'HttpPass']}
    conf_conn = {'Connection': ['Timeout', 'Retry', 'SleepRetry', 'RetryRandom', 'SleepRandomRetryMax']}
    conf_state = {'InputState': ['SaveDir', 'Days']}
    conf_webapi = {'WebAPI': ['Token', 'Host']}

    # options specific for every connector
    conf_topo_output = {'Output': ['TopologyGroupOfEndpoints',
                                   'TopologyGroupOfGroups']}
    conf_downtimes_output = {'Output': ['Downtimes']}
    conf_weights_output = {'Output': ['Weights']}
    conf_servicetypes_output = {'Output': ['ServiceTypes']}

    def __call__(self, caller=None, confpath=None, **kwargs):
        if caller:
            self.__init__(caller, confpath, **kwargs)
        else:
            self.__init__(str(self.__class__), confpath, **kwargs)
        return self

    def __init__(self, caller, confpath=None, **kwargs):
        self.caller = caller
        self.optional = dict()

        self._filename = f"{os.environ['VIRTUAL_ENV']}/etc/global.conf" if not confpath else confpath

        self._checkpath = kwargs['checkpath'] if 'checkpath' \
            in kwargs.keys() else False

        self.optional.update(self._lowercase_dict(self.conf_auth))
        self.optional.update(self._lowercase_dict(self.conf_webapi))

        self.shared_secopts = self._merge_dict(self.conf_general,
                                               self.conf_auth, self.conf_conn,
                                               self.conf_state,
                                               self.conf_webapi)
        self.secopts = {
            'connectors-probe':
            self._merge_dict(self.shared_secopts),
            'topology-gocdb-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_topo_output),
            'topology-combiner.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_topo_output),
            'service-types-combiner.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_servicetypes_output),
            'downtimes-combiner.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_downtimes_output),
            'topology-json-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_topo_output),
            'topology-provider-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_topo_output),
            'topology-lot1sc-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_topo_output),
            'downtimes-csv-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_downtimes_output),
            'downtimes-gocdb-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_downtimes_output),
            'weights-vapor-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_weights_output),
            'topology-csv-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_topo_output),
            'service-types-gocdb-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_servicetypes_output),
            'service-types-csv-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_servicetypes_output),
            'service-types-json-connector.py':
            self._merge_dict(self.shared_secopts,
                             self.conf_servicetypes_output),
        }

        try:
            if self.caller:
                self.caller_secopts = self.secopts[os.path.basename(self.caller)]
            else:
                self.caller_secopts = self.shared_secopts

        except KeyError:
            pass

        self.parse()

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
        opts = [o for o in self._options.keys() if o.startswith(section)]
        for o in opts:
            if o in newd:
                continue
            newd.update({o: self._options[o]})

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

        lval = [self._options[k] for k in self._concat_sectopt(loweropts)]

        if any(lval):
            return True
        else:
            return False

    def parse(self, confpath=None):
        config = configparser.ConfigParser()

        if confpath:
            self._filename = confpath

        if not os.path.exists(self._filename):
            raise ConnectorConfError('Could not find %s' % self._filename)

        config.read(self._filename)
        self._options = {}

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
                                    raise ConnectorConfError('No DATE placeholder in %s' % opt)

                                if optget in ['True', 'False']:
                                    self._options.update({(sect + opt).lower(): eval(optget)})
                                else:
                                    self._options.update({(sect + opt).lower(): optget})

                            except configparser.NoOptionError as e:
                                s = e.section.lower()
                                if (s in self.optional.keys() and
                                        e.option in self.optional[s]):
                                    pass
                                else:
                                    raise e

            if not self._one_active(self.conf_general):
                raise ConnectorConfError('At least one of %s needs to be True' % (
                    ', '.join(self._concat_sectopt(self.conf_general))))

        except configparser.NoOptionError as e:
            raise ConnectorConfError(e.message)
        except configparser.NoSectionError as e:
            raise ConnectorConfError("%s defined" % (e.args[0]))
        except OSError as e:
            raise ConnectorConfError('%s %s' % (os.strerror(e.args[0]), e.args[1]))
        except AttributeError:
            # caller_secopts uninitialized on initial creation as it's called with
            # unknown caller "config/glob.py"
            pass

        return self._options

    def configure(self, extops):
        for opt in extops:
            val = extops[opt]
            opt = ''.join(opt.split('_')).lower()
            self._options[opt] = val

    def options(self):
        return self._options


Global = _GlobalConf('config/glob.py')
