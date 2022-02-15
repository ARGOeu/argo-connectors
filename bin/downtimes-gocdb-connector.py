#!/usr/bin/python3

# Copyright (c) 2013 GRNET S.A., SRCE, IN2P3 CNRS Computing Centre
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS
# IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language
# governing permissions and limitations under the License.
#
# The views and conclusions contained in the software and
# documentation are those of the authors and should not be
# interpreted as representing official policies, either expressed
# or implied, of either GRNET S.A., SRCE or IN2P3 CNRS Computing
# Centre
#
# The work represented by this source file is partially funded by
# the EGI-InSPIRE project through the European Commission's 7th
# Framework Programme (contract # INFSO-RI-261323)

import argparse
import datetime
import os
import sys
from urllib.parse import urlparse

import uvloop
import asyncio

from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_egi_connectors.io.webapi import WebAPI
from argo_egi_connectors.io.avrowrite import AvroWriter
from argo_egi_connectors.io.statewrite import state_write
from argo_egi_connectors.log import Logger

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.utils import filename_date, module_class_name
from argo_egi_connectors.parse.gocdb_downtimes import ParseDowntimes

logger = None
globopts = {}

DOWNTIMEPI = '/gocdbpi/private/?method=get_downtime'


async def fetch_data(feed, auth_opts, start, end):
    feed_parts = urlparse(feed)
    start_fmt = start.strftime("%Y-%m-%d")
    end_fmt = end.strftime("%Y-%m-%d")
    session = SessionWithRetry(logger, os.path.basename(sys.argv[0]), globopts)
    res = await session.http_get(
        '{}://{}{}&windowstart={}&windowend={}'.format(feed_parts.scheme,
                                                       feed_parts.netloc,
                                                       DOWNTIMEPI, start_fmt,
                                                       end_fmt)
    )

    return res


def parse_source(res, start, end, uidservtype):
    gocdb = ParseDowntimes(logger, res, start, end, uidservtype)
    return gocdb.get_data()


def get_webapi_opts(cglob, confcust):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' % (logger.customer, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)
    return webapi_opts


async def send_webapi(webapi_opts, date, dts):
    webapi = WebAPI(sys.argv[0], webapi_opts['webapihost'],
                    webapi_opts['webapitoken'], logger,
                    int(globopts['ConnectionRetry'.lower()]),
                    int(globopts['ConnectionTimeout'.lower()]),
                    int(globopts['ConnectionSleepRetry'.lower()]), date=date)
    await webapi.send(dts, downtimes_component=True)


async def write_state(confcust, timestamp, state):
    # safely assume here one customer defined in customer file
    cust = list(confcust.get_customers())[0]
    statedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
    await state_write(sys.argv[0], statedir, state,
                      globopts['InputStateDays'.lower()], timestamp)


def write_avro(confcust, dts, timestamp):
    custdir = confcust.get_custdir()
    filename = filename_date(logger, globopts['OutputDowntimes'.lower()], custdir, stamp=timestamp)
    avro = AvroWriter(globopts['AvroSchemasDowntimes'.lower()], filename)
    ret, excep = avro.write(dts)
    if not ret:
        logger.error('Customer:{} {}'.format(logger.customer, repr(excep)))
        raise SystemExit(1)


def main():
    global logger, globopts
    parser = argparse.ArgumentParser(description='Fetch downtimes from GOCDB for given date')
    parser.add_argument('-d', dest='date', nargs=1, metavar='YEAR-MONTH-DAY', required=True)
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    args = parser.parse_args()

    logger = Logger(os.path.basename(sys.argv[0]))
    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(sys.argv[0], confpath)
    globopts = cglob.parse()

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    feed = confcust.get_topofeed()
    logger.customer = confcust.get_custname()

    if len(args.date) == 0:
        print(parser.print_help())
        raise SystemExit(1)

    # calculate start and end times
    try:
        start = datetime.datetime.strptime(args.date[0], '%Y-%m-%d')
        end = datetime.datetime.strptime(args.date[0], '%Y-%m-%d')
        timestamp = start.strftime('%Y_%m_%d')
        start = start.replace(hour=0, minute=0, second=0)
        end = end.replace(hour=23, minute=59, second=59)
    except ValueError as exc:
        logger.error(exc)
        raise SystemExit(1)

    uidservtype = confcust.get_uidserviceendpoints()

    auth_custopts = confcust.get_authopts()
    auth_opts = cglob.merge_opts(auth_custopts, 'authentication')
    auth_complete, missing = cglob.is_complete(auth_opts, 'authentication')
    if not auth_complete:
        missing_err = ''.join(missing)
        logger.error('Customer:{} authentication options incomplete, missing {}'.format(logger.customer, missing_err))
        raise SystemExit(1)

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # we don't have multiple tenant definitions in one
        # customer file so we can safely assume one tenant/customer
        write_empty = confcust.send_empty(sys.argv[0])
        if not write_empty:
            res = loop.run_until_complete(
                fetch_data(feed, auth_opts, start, end)
            )
            dts = parse_source(res, start, end, uidservtype)
        else:
            dts = []

        loop.run_until_complete(
            write_state(confcust, timestamp, True)
        )

        webapi_opts = get_webapi_opts(cglob, confcust)

        if eval(globopts['GeneralPublishWebAPI'.lower()]):
            loop.run_until_complete(
                send_webapi(webapi_opts, args.date[0], dts)
            )

        if dts or write_empty:
            cust = list(confcust.get_customers())[0]
            logger.info('Customer:%s Fetched Date:%s Endpoints:%d' %
                        (confcust.get_custname(cust), args.date[0], len(dts)))

        if eval(globopts['GeneralWriteAvro'.lower()]):
            write_avro(confcust, dts, timestamp)

    except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        loop.run_until_complete(
            write_state(confcust, timestamp, False)
        )

    loop.close()

if __name__ == '__main__':
    main()
