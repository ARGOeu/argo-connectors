#!/usr/bin/python3

import argparse
import datetime
import os
import sys

import asyncio
import uvloop

from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_connectors.log import Logger
from argo_connectors.tasks.flat_servicetypes import TaskFlatServiceTypes
from argo_connectors.tasks.common import write_state
from argo_connectors.utils import date_check

from argo_connectors.config import Global, CustomerConf

logger = None
globopts = {}


def get_webapi_opts(cglob, confcust):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' % (logger.customer, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)
    return webapi_opts


def main():
    global logger, globopts
    parser = argparse.ArgumentParser(description='Fetch service types from CSV or JSON')
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY', help='write data for this date', type=str, required=False)
    parser.add_argument('--initial', dest='initsync', help='initial sync of service types', action='store_true', default=False, required=False)
    args = parser.parse_args()

    fixed_date = None
    if args.date and date_check(args.date):
        fixed_date = args.date

    logger = Logger(os.path.basename(sys.argv[0]))
    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(sys.argv[0], confpath)
    globopts = cglob.parse()

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    feed = confcust.get_servicesfeed()
    custname = confcust.get_custname()
    logger.customer = confcust.get_custname()

    auth_custopts = confcust.get_authopts()
    auth_opts = cglob.merge_opts(auth_custopts, 'authentication')
    auth_complete, missing = cglob.is_complete(auth_opts, 'authentication')
    if not auth_complete:
        logger.error('%s options incomplete, missing %s' % ('authentication', ' '.join(missing)))
        raise SystemExit(1)

    webapi_opts = get_webapi_opts(cglob, confcust)

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        task = TaskFlatServiceTypes(
            loop, logger, sys.argv[0], globopts, auth_opts, webapi_opts,
            confcust, custname, feed, fixed_date, is_csv=False,
            initsync=args.initsync
        )
        loop.run_until_complete(task.run())

    except (KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        loop.run_until_complete(
            write_state(sys.argv[0], globopts, confcust, fixed_date, False)
        )

    finally:
        loop.close()

if __name__ == '__main__':
    main()
