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
import os
import sys

import uvloop
import asyncio

from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_egi_connectors.io.webapi import WebAPI
from argo_egi_connectors.io.avrowrite import AvroWriter
from argo_egi_connectors.io.statewrite import state_write
from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.vapor import ParseWeights

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.utils import filename_date, module_class_name, date_check
from urllib.parse import urlparse

globopts = {}
logger = None

VAPORPI = 'https://operations-portal.egi.eu/vapor/downloadLavoisier/option/json/view/VAPOR_Ngi_Sites_Info'


async def fetch_data(feed):
    feed_parts = urlparse(feed)
    session = SessionWithRetry(logger, os.path.basename(sys.argv[0]), globopts)
    res = await session.http_get('{}://{}{}'.format(feed_parts.scheme,
                                                    feed_parts.netloc,
                                                    feed_parts.path))
    return res


def parse_source(res):
    weights = ParseWeights(logger, res).get_data()
    return weights


def get_webapi_opts(cust, job, cglob, confcust):
    webapi_custopts = confcust.get_webapiopts(cust)
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s Job:%s %s options incomplete, missing %s' % (logger.customer, job, 'webapi', ' '.join(missopt)))
    return webapi_opts


async def send_webapi(job, confcust, webapi_opts, fixed_date, weights):
    webapi = WebAPI(sys.argv[0], webapi_opts['webapihost'],
                    webapi_opts['webapitoken'], logger,
                    int(globopts['ConnectionRetry'.lower()]),
                    int(globopts['ConnectionTimeout'.lower()]),
                    int(globopts['ConnectionSleepRetry'.lower()]),
                    report=confcust.get_jobdir(job), endpoints_group='SITES',
                    date=fixed_date)
    await webapi.send(weights)


async def write_state(cust, job, confcust, fixed_date, state):
    jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust, job)
    if fixed_date:
        await state_write(sys.argv[0], jobstatedir, state,
                          globopts['InputStateDays'.lower()],
                          fixed_date.replace('-', '_'))
    else:
        await state_write(sys.argv[0], jobstatedir, state,
                          globopts['InputStateDays'.lower()])


def write_avro(cust, job, confcust, fixed_date, weights):
    jobdir = confcust.get_fulldir(cust, job)
    if fixed_date:
        filename = filename_date(logger, globopts['OutputWeights'.lower()], jobdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(logger, globopts['OutputWeights'.lower()], jobdir)
    avro = AvroWriter(globopts['AvroSchemasWeights'.lower()], filename)
    ret, excep = avro.write(weights)
    if not ret:
        logger.error('Customer:%s Job:%s %s' % (logger.customer, logger.job, repr(excep)))
        raise SystemExit(1)


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
            res = loop.run_until_complete(
                fetch_data(feed)
            )
            weights = parse_source(res)

            for job, cust in jobcust:
                logger.customer = confcust.get_custname(cust)
                logger.job = job

                write_empty = confcust.send_empty(sys.argv[0], cust)

                if write_empty:
                    weights = []

                webapi_opts = get_webapi_opts(cust, job, cglob, confcust)

                if eval(globopts['GeneralPublishWebAPI'.lower()]):
                    loop.run_until_complete(
                        send_webapi(job, confcust, webapi_opts, fixed_date, weights)
                    )

                if eval(globopts['GeneralWriteAvro'.lower()]):
                    write_avro(cust, job, confcust, fixed_date, weights)

                loop.run_until_complete(
                    write_state(cust, job, confcust, fixed_date, True)
                )

                if weights or write_empty:
                    custs = set([cust for job, cust in jobcust])
                    for cust in custs:
                        jobs = [job for job, lcust in jobcust if cust == lcust]
                        logger.info('Customer:%s Jobs:%s Sites:%d' % (confcust.get_custname(cust),
                                                                      jobs[0] if len(jobs) == 1 else '({0})'.format(','.join(jobs)),
                                                                      len(weights)))

        except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
            logger.error(repr(exc))
            for job, cust in jobcust:
                loop.run_until_complete(
                    write_state(cust, job, confcust, fixed_date, False)
                )


if __name__ == '__main__':
    main()
