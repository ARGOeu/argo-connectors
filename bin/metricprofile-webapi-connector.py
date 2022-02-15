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
import re
import sys

import uvloop
import asyncio

from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_egi_connectors.io.avrowrite import AvroWriter
from argo_egi_connectors.io.statewrite import state_write
from argo_egi_connectors.log import Logger

from argo_egi_connectors.config import CustomerConf, Global
from argo_egi_connectors.utils import filename_date, module_class_name, datestamp, date_check
from argo_egi_connectors.parse.webapi_metricprofile import ParseMetricProfiles

logger = None

globopts = dict()
custname = ''
API_PATH = '/api/v2/metric_profiles'


async def fetch_data(host, token):
    session = SessionWithRetry(logger, os.path.basename(sys.argv[0]), globopts,
                               token=token)
    res = await session.http_get('{}://{}{}'.format('https', host, API_PATH))
    return res


def parse_source(res, profiles, namespace):
    metric_profiles = ParseMetricProfiles(logger, res, profiles, namespace).get_data()
    return metric_profiles


async def write_state(cust, job, confcust, fixed_date, state):
    jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust, job)
    if fixed_date:
        await state_write(sys.argv[0], jobstatedir, state,
                          globopts['InputStateDays'.lower()],
                          fixed_date.replace('-', '_'))
    else:
        await state_write(sys.argv[0], jobstatedir, state,
                          globopts['InputStateDays'.lower()])


def write_avro(cust, job, confcust, fixed_date, fetched_profiles):
    jobdir = confcust.get_fulldir(cust, job)
    if fixed_date:
        filename = filename_date(logger, globopts['OutputMetricProfile'.lower()], jobdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(logger, globopts['OutputMetricProfile'.lower()], jobdir)
    avro = AvroWriter(globopts['AvroSchemasMetricProfile'.lower()], filename)
    ret, excep = avro.write(fetched_profiles)
    if not ret:
        logger.error('Customer:%s Job:%s %s' % (logger.customer, logger.job, repr(excep)))
        raise SystemExit(1)


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

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    for cust in confcust.get_customers():
        custname = confcust.get_custname(cust)

        for job in confcust.get_jobs(cust):
            logger.customer = confcust.get_custname(cust)
            logger.job = job

            profiles = confcust.get_profiles(job)
            webapi_custopts = confcust.get_webapiopts(cust)
            webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
            webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')

            if not webapi_complete:
                logger.error('Customer:%s Job:%s %s options incomplete, missing %s' % (custname, logger.job, 'webapi', ' '.join(missopt)))
                continue

            try:
                res = loop.run_until_complete(
                    fetch_data(webapi_opts['webapihost'], webapi_opts['webapitoken'])
                )

                fetched_profiles = parse_source(res, profiles, confcust.get_namespace(job))

                loop.run_until_complete(
                    write_state(cust, job, confcust, fixed_date, True)
                )

                if eval(globopts['GeneralWriteAvro'.lower()]):
                    write_avro(cust, job, confcust, fixed_date, fetched_profiles)

                logger.info('Customer:' + custname + ' Job:' + job + ' Profiles:%s Tuples:%d' % (', '.join(profiles), len(fetched_profiles)))

            except (ConnectorHttpError, KeyboardInterrupt, ConnectorParseError) as exc:
                logger.error(repr(exc))
                loop.run_until_complete(
                    write_state(cust, job, confcust, fixed_date, False)
                )


if __name__ == '__main__':
    main()
