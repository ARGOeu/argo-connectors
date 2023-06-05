#!/usr/bin/python3

import argparse
import datetime
import os
import sys

import asyncio
import uvloop

from argo_connectors.singleton_config import ConfigClass
from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_connectors.log import Logger
from argo_connectors.tasks.gocdb_downtimes import TaskGocdbDowntimes
from argo_connectors.tasks.common import write_state

from argo_connectors.config import Global, CustomerConf

logger = None
globopts = {}


def get_webapi_opts(cglob, confcust):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' %
                     (logger.customer, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)
    return webapi_opts


def main():
    global logger, globopts
    parser = argparse.ArgumentParser(
        description='Fetch downtimes from GOCDB for given date')
    parser.add_argument('-d', dest='date', nargs=1,
                        metavar='YEAR-MONTH-DAY', required=True)
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf',
                        help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf',
                        help='path to global configuration file', type=str, required=False)
    args = parser.parse_args()

    try:
        start = datetime.datetime.strptime(args.date[0], '%Y-%m-%d')
        end = datetime.datetime.strptime(args.date[0], '%Y-%m-%d')
        timestamp = start.strftime('%Y_%m_%d')
        start = start.replace(hour=0, minute=0, second=0)
        end = end.replace(hour=23, minute=59, second=59)

    except ValueError as exc:
        logger.error(exc)
        raise SystemExit(1)

    config = ConfigClass(args)
    args = config.parse_args()
    cglob = config.get_cglob(args)
    globopts = config.get_globopts(cglob)
    confcust = config.get_confcust(globopts, args)
    logger = config.get_logger()

    loop = config.get_loop()
    asyncio.set_event_loop(loop)

    try:
        cust = list(confcust.get_customers())[0]
        cust = confcust.get_custname(cust)

        task = TaskGocdbDowntimes(cust, start, end, timestamp)

        loop.run_until_complete(task.run())

    except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        loop.run_until_complete(
            write_state(sys.argv[0], globopts, confcust, timestamp, False)
        )

    loop.close()


if __name__ == '__main__':
    main()
