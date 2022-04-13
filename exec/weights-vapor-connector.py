#!/usr/bin/python3

import argparse
import os
import sys

import asyncio
import uvloop

from argo_egi_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_egi_connectors.tasks.vapor_weights import TaskVaporWeights
from argo_egi_connectors.tasks.common import write_weights_metricprofile_state as write_state
from argo_egi_connectors.log import Logger

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.utils import date_check

globopts = {}
logger = None

VAPORPI = 'https://operations-portal.egi.eu/vapor/downloadLavoisier/option/json/view/VAPOR_Ngi_Sites_Info'


def main():
    global logger, globopts
    parser = argparse.ArgumentParser(description="""Fetch weights information from Gstat provider
                                                    for every job listed in customer.conf""")
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
    feeds = confcust.get_mapfeedjobs(sys.argv[0], deffeed=VAPORPI)

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    for feed, jobcust in feeds.items():
        customers = set(map(lambda jc: confcust.get_custname(jc[1]), jobcust))
        customers = customers.pop() if len(customers) == 1 else '({0})'.format(','.join(customers))
        sjobs = set(map(lambda jc: jc[0], jobcust))
        jobs = list(sjobs)[0] if len(sjobs) == 1 else '({0})'.format(','.join(sjobs))
        logger.job = jobs
        logger.customer = customers

        try:
            task = TaskVaporWeights(loop, logger, sys.argv[0], globopts,
                                    confcust, VAPORPI, jobcust, cglob,
                                    fixed_date)
            loop.run_until_complete(task.run())

        except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
            logger.error(repr(exc))
            for job, cust in jobcust:
                loop.run_until_complete(
                    write_state(sys.argv[0], globopts, cust, job, confcust, fixed_date, True)
                )


if __name__ == '__main__':
    main()
