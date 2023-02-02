#!/usr/bin/python3

import argparse
import os
import sys

import asyncio
import uvloop

from argo_connectors.config import Global, CustomerConf
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.log import Logger
from argo_connectors.tasks.common import write_state
from argo_connectors.tasks.gocdb_topology import TaskGocdbTopology
from argo_connectors.utils import date_check

logger = None

# GOCDB explicitly says &scope='' for all scopes
# TODO: same methods can be served on different paths
SERVICE_ENDPOINTS_PI = '/gocdbpi/private/?method=get_service_endpoint&scope='
SITES_PI = '/gocdbpi/private/?method=get_site&scope='
SERVICE_GROUPS_PI = '/gocdbpi/private/?method=get_service_group&scope='

ROC_CONTACTS = '/gocdbpi/private/?method=get_roc_contacts'
SITE_CONTACTS = '/gocdbpi/private/?method=get_site_contacts'
PROJECT_CONTACTS = '/gocdbpi/private/?method=get_project_contacts'
SERVICEGROUP_CONTACTS = '/gocdbpi/private/?method=get_service_group_role'

globopts = {}
custname = ''

isok = True


def get_webapi_opts(cglob, confcust):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' % (logger.customer, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)
    return webapi_opts


def get_bdii_opts(confcust):
    bdii_custopts = confcust._get_cust_options('BDIIOpts')
    if bdii_custopts:
        bdii_complete, missing = confcust.is_complete_bdii(bdii_custopts)
        if not bdii_complete:
            logger.error('%s options incomplete, missing %s' % ('bdii', ' '.join(missing)))
            raise SystemExit(1)
        return bdii_custopts
    else:
        return None


def main():
    global logger, globopts, confcust
    parser = argparse.ArgumentParser(description="""Fetch entities (ServiceGroups, Sites, Endpoints)
                                                    from GOCDB for every customer and job listed in customer.conf and write them
                                                    in an appropriate place""")
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY', help='write data for this date', type=str, required=False)
    args = parser.parse_args()
    group_endpoints, group_groups = [], []
    logger = Logger(os.path.basename(sys.argv[0]))

    fixed_date = None
    if args.date and date_check(args.date):
        fixed_date = args.date

    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(sys.argv[0], confpath)
    globopts = cglob.parse()
    pass_extensions = eval(globopts['GeneralPassExtensions'.lower()])

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    topofeed = confcust.get_topofeed()
    topofeedpaging = confcust.get_topofeedpaging()
    uidservendp = confcust.get_uidserviceendpoints()
    topofetchtype = confcust.get_topofetchtype()
    custname = confcust.get_custname()
    logger.customer = custname

    auth_custopts = confcust.get_authopts()
    auth_opts = cglob.merge_opts(auth_custopts, 'authentication')
    auth_complete, missing = cglob.is_complete(auth_opts, 'authentication')
    if not auth_complete:
        logger.error('%s options incomplete, missing %s' % ('authentication', ' '.join(missing)))
        raise SystemExit(1)

    bdii_opts = get_bdii_opts(confcust)
    webapi_opts = get_webapi_opts(cglob, confcust)

    toposcope = confcust.get_toposcope()
    topofeedendpoints = confcust.get_topofeedendpoints()
    topofeedservicegroups = confcust.get_topofeedservicegroups()
    topofeedsites = confcust.get_topofeedsites()
    notiflag = confcust.get_notif_flag()
    global SERVICE_ENDPOINTS_PI, SERVICE_GROUPS_PI, SITES_PI
    if toposcope:
        SERVICE_ENDPOINTS_PI = SERVICE_ENDPOINTS_PI + toposcope
        SERVICE_GROUPS_PI = SERVICE_GROUPS_PI + toposcope
        SITES_PI = SITES_PI + toposcope
    if topofeedendpoints:
        SERVICE_ENDPOINTS_PI = topofeedendpoints
    else:
        SERVICE_ENDPOINTS_PI = topofeed + SERVICE_ENDPOINTS_PI
    if topofeedservicegroups:
        SERVICE_GROUPS_PI = topofeedservicegroups
    else:
        SERVICE_GROUPS_PI = topofeed + SERVICE_GROUPS_PI
    if topofeedsites:
        SITES_PI = topofeedsites
    else:
        SITES_PI = topofeed + SITES_PI

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        task = TaskGocdbTopology(
            loop, logger, sys.argv[0], SITE_CONTACTS, SERVICEGROUP_CONTACTS,
            SERVICE_ENDPOINTS_PI, SERVICE_GROUPS_PI, SITES_PI, globopts,
            auth_opts, webapi_opts, bdii_opts, confcust, custname, topofeed,
            topofetchtype, fixed_date, uidservendp, pass_extensions,
            topofeedpaging, notiflag
        )
        loop.run_until_complete(task.run())

    except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        loop.run_until_complete(
            write_state(sys.argv[0], globopts, confcust, fixed_date, False)
        )

    finally:
        loop.close()


if __name__ == '__main__':
    main()
