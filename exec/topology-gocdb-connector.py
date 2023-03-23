#!/usr/bin/python3

import argparse
import os
import sys

import asyncio
import uvloop

from argo_connectors.singleton_config import ConfigClass
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.log import Logger
from argo_connectors.tasks.common import write_state
from argo_connectors.tasks.gocdb_topology import TaskGocdbTopology
from argo_connectors.utils import date_check

logger = None
globopts = {}
custname = ''
isok = True

# GOCDB explicitly says &scope='' for all scopes

def get_webapi_opts(cglob, confcust):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' %
                     (logger.customer, 'webapi', ' '.join(missopt)))
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


def main():
    global logger, globopts, confcust
    parser = argparse.ArgumentParser(description="""Fetch entities (ServiceGroups, Sites, Endpoints)
                                                    from GOCDB for every customer and job listed in customer.conf and write them
                                                    in an appropriate place""")
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf',
                        help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf',
                        help='path to global configuration file', type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY',
                        help='write data for this date', type=str, required=False)
    args = parser.parse_args()

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    
    config = ConfigClass(args)
    fixed_date = config.get_fixed_date()

    try:
        task = TaskGocdbTopology(config, loop)

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
