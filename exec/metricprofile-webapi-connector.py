#!/usr/bin/env python

import argparse
import os
import sys

import asyncio

from argo_connectors.config import CustomerConf, Global
from argo_connectors.log import Logger
from argo_connectors.tasks.webapi_metricprofile import TaskWebApiMetricProfile
from argo_connectors.utils import date_check

logger = None

globopts = dict()
custname = ''


def main():
    global logger, globopts
    parser = argparse.ArgumentParser(description='Fetch metric profile for every job of the customer')
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY', help='write data for this date', type=str, required=False)
    args = parser.parse_args()

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

    loop = asyncio.get_event_loop()

    for cust in confcust.get_customers():
        try:
            task = TaskWebApiMetricProfile(
                loop, logger, sys.argv[0], globopts, cglob, confcust, cust, fixed_date
            )
            loop.run_until_complete(task.run())

        except (KeyboardInterrupt) as exc:
            logger.error(repr(exc))

        finally:
            loop.close()


if __name__ == '__main__':
    main()
