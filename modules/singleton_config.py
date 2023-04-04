import os
import sys

import uvloop
import argparse

from argo_connectors.log import Logger
from argo_connectors.config import Global, CustomerConf
from argo_connectors.utils import date_check


logger = None
# globopts = {}
# custname = ''
# isok = True


def get_webapi_opts(cglob, confcust, custname):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' %
                     (custname, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)
    return webapi_opts


def get_bdii_opts(confcust):
    bdii_custopts = confcust._get_cust_options('BDIIOpts')
    if bdii_custopts:
        bdii_complete, missing = confcust.is_complete_bdii(bdii_custopts)
        if not bdii_complete:
            logger.error('%s options incomplete, missing %s' %
                         ('bdii', ' '.join(missing)))
            raise SystemExit(1)
        return bdii_custopts
    else:
        return None


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class ConfigClass(metaclass=Singleton):
    _loop = None
    
    def __init__(self):
        self._setup_loop()

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf',
                            help='path to customer configuration file', type=str, required=False)
        parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf',
                            help='path to global configuration file', type=str, required=False)
        parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY',
                            help='write data for this date', type=str, required=False)
        args = parser.parse_args()
        return args

    def _setup_loop(self):
        if self._loop is None:
            self._loop = uvloop.new_event_loop()

    def get_loop(self):
        return self._loop

    def get_logger(self):
        logger = Logger(os.path.basename(sys.argv[0]))
        return logger

    def get_connector_name(self):
        return sys.argv[0]

    def get_fixed_date(self, args):
        fixed_date = None
        if args.date and date_check(args.date):
            fixed_date = args.date
        return fixed_date

    def get_cglob(self, args):  #
        confpath = args.gloconf[0] if args.gloconf else None
        cglob = Global(sys.argv[0], confpath)
        return cglob

    def get_globopts(self, cglob): #
        globopts = cglob.parse()
        return globopts
    
    def get_confcust(self, globopts, args):
        confpath = args.custconf[0] if args.custconf else None
        confcust = CustomerConf(sys.argv[0], confpath)
        confcust.parse()
        confcust.make_dirstruct()
        confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
        return confcust

    def topofeed_data(self, confcust):
        topofeed = confcust.get_topofeed()
        return topofeed

    def topofeedpaging_data(self, confcust):
        topofeedpaging = confcust.get_topofeedpaging()
        return topofeedpaging

    def uidservendp_data(self, confcust):
        uidservendp = confcust.get_uidserviceendpoints()
        return uidservendp

    def topofetchtype_data(self, confcust):
        topofetchtype = confcust.get_topofetchtype()
        return topofetchtype

    def custname_data(self, confcust):
        custname = confcust.get_custname()
        return custname

    def get_auth_opts(self, confcust, cglob, logger): #
        auth_custopts = confcust.get_authopts()
        auth_opts = cglob.merge_opts(auth_custopts, 'authentication')
        auth_complete, missing = cglob.is_complete(auth_opts, 'authentication')
        if not auth_complete:
            logger.error('%s options incomplete, missing %s' %
                         ('authentication', ' '.join(missing)))
            raise SystemExit(1)
        return auth_opts

    def bdii_opts_data(self, confcust):
        bdii_opts = get_bdii_opts(confcust)
        return bdii_opts

    def get_webapi_opts_data(self, confcust, cglob, connector_name): #
        webapi_opts = get_webapi_opts(cglob, confcust, connector_name)
        return webapi_opts

    def notiflag_data(self, confcust):
        notiflag = confcust.get_notif_flag()
        return notiflag

    def service_data(self, confcust):
        toposcope = confcust.get_toposcope()
        topofeedendpoints = confcust.get_topofeedendpoints()
        topofeedservicegroups = confcust.get_topofeedservicegroups()
        topofeedsites = confcust.get_topofeedsites()

        if toposcope:
            SERVICE_ENDPOINTS_PI = topofeedendpoints + toposcope
            SERVICE_GROUPS_PI = topofeedservicegroups + toposcope
            SITES_PI = topofeedsites + toposcope

        else:
            SERVICE_ENDPOINTS_PI = topofeedendpoints
            SERVICE_GROUPS_PI = topofeedservicegroups
            SITES_PI = topofeedsites

        return SERVICE_ENDPOINTS_PI, SERVICE_GROUPS_PI, SITES_PI

    def vaporrpi_data(self, confcust):
        VAPORPI = confcust.get_vaporpi()
        return VAPORPI

    def get_feeds(self, confcust, VAPORPI):
        feeds = confcust.get_mapfeedjobs(sys.argv[0], deffeed=VAPORPI)
        return feeds

    def get_feed(self, confcust):
        feed = confcust.get_servicesfeed()
        return feed

    def get_initsync(self, args):
        return args.initsync

    def get_downtime_feed(self, confcust):
        return confcust.get_downfeed()

    def get_target_date(self, args):
        return args.date[0]

    def is_csv(self):
        return True

    def get_targetdate(self, args):
        return args.date[0]