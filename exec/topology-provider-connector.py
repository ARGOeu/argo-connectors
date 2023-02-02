#!/usr/bin/python3

import argparse
import os
import sys
import json

import uvloop
import asyncio

from argo_connectors.exceptions import ConnectorError, ConnectorHttpError, ConnectorParseError
from argo_connectors.io.statewrite import state_write
from argo_connectors.log import Logger
from argo_connectors.config import Global, CustomerConf
from argo_connectors.utils import filename_date, datestamp, date_check
from argo_connectors.tasks.provider_topology import TaskProviderTopology
from argo_connectors.tasks.common import write_state


logger = None
globopts = {}
custname = ''


def get_webapi_opts(cglob, confcust):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' % (logger.customer, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)
    return webapi_opts


def main():
    global logger, globopts, confcust

    parser = argparse.ArgumentParser(description="""Fetch and construct entities from EOSC-PROVIDER feed""")
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY', help='write data for this date', type=str, required=False)
    args = parser.parse_args()
    group_endpoints, group_groups = list(), list()
    logger = Logger(os.path.basename(sys.argv[0]))

    fixed_date = None
    if args.date and date_check(args.date):
        fixed_date = args.date

    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(sys.argv[0], confpath)
    globopts = cglob.parse()

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    global custname
    custname = confcust.get_custname()

    # safely assume here one customer defined in customer file
    cust = list(confcust.get_customers())[0]
    jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
    fetchtype = confcust.get_topofetchtype()[0]

    webapi_opts = get_webapi_opts(cglob, confcust)

    state = None
    logger.customer = custname
    uidservendp = confcust.get_uidserviceendpoints()
    topofeed = confcust.get_topofeed()
    topofeedpaging = confcust.get_topofeedpaging()

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        task = TaskProviderTopology(
            loop, logger, sys.argv[0], globopts, webapi_opts, confcust,
            topofeedpaging, uidservendp, fetchtype, fixed_date
        )
        loop.run_until_complete(task.run())

    except (ConnectorError, ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        loop.run_until_complete(
            write_state(sys.argv[0], globopts, confcust, fixed_date, False)
        )

if __name__ == '__main__':
    main()
