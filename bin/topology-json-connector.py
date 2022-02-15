#!/usr/bin/python3

import argparse
import os
import sys
import json

import uvloop
import asyncio

from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_egi_connectors.io.webapi import WebAPI
from argo_egi_connectors.io.avrowrite import AvroWriter
from argo_egi_connectors.io.statewrite import state_write
from argo_egi_connectors.log import Logger
from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.utils import filename_date, datestamp, date_check
from argo_egi_connectors.parse.flat_topology import ParseFlatEndpoints, ParseContacts
from argo_egi_connectors.mesh.contacts import attach_contacts_topodata

from urllib.parse import urlparse

logger = None
globopts = {}
custname = ''


def is_feed(feed):
    data = urlparse(feed)

    if not data.netloc:
        return False
    else:
        return True


async def send_webapi(webapi_opts, data, topotype, fixed_date=None):
    webapi = WebAPI(sys.argv[0], webapi_opts['webapihost'],
                    webapi_opts['webapitoken'], logger,
                    int(globopts['ConnectionRetry'.lower()]),
                    int(globopts['ConnectionTimeout'.lower()]),
                    int(globopts['ConnectionSleepRetry'.lower()]),
                    date=fixed_date)
    await webapi.send(data, topotype)


def get_webapi_opts(cglob, confcust):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' % (logger.customer, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)
    return webapi_opts


async def fetch_data(feed):
    remote_topo = urlparse(feed)
    session = SessionWithRetry(logger, custname, globopts)
    res = await session.http_get('{}://{}{}'.format(remote_topo.scheme,
                                                    remote_topo.netloc,
                                                    remote_topo.path))
    return res


def parse_source_topo(res, uidservtype, fetchtype):
    # group_groups, group_endpoints = ParseEoscTopo(logger, res, uidservtype, fetchtype).get_data()
    topo = ParseFlatEndpoints(logger, res, custname, uidservtype, fetchtype, scope=custname)
    group_groups = topo.get_groupgroups()
    group_endpoints = topo.get_groupendpoints()

    return group_groups, group_endpoints


async def write_state(confcust, fixed_date, state):
    cust = list(confcust.get_customers())[0]
    jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
    fetchtype = confcust.get_topofetchtype()
    if fixed_date:
        await state_write(sys.argv[0], jobstatedir, state,
                          globopts['InputStateDays'.lower()],
                          fixed_date.replace('-', '_'))
    else:
        await state_write(sys.argv[0], jobstatedir, state,
                          globopts['InputStateDays'.lower()])


def write_avro(confcust, group_groups, group_endpoints, fixed_date):
    custdir = confcust.get_custdir()
    if fixed_date:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], custdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], custdir)
    avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfGroups'.lower()], filename)
    ret, excep = avro.write(group_groups)
    if not ret:
        logger.error('Customer:%s : %s' % (logger.customer, repr(excep)))
        raise SystemExit(1)

    if fixed_date:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], custdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], custdir)
    avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()], filename)
    ret, excep = avro.write(group_endpoints)
    if not ret:
        logger.error('Customer:%s : %s' % (logger.customer, repr(excep)))
        raise SystemExit(1)


def main():
    global logger, globopts, confcust

    parser = argparse.ArgumentParser(description="""Fetch and construct entities from EOSC-PORTAL feed""")
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

    state = None
    logger.customer = custname
    uidservtype = confcust.get_uidserviceendpoints()
    topofeed = confcust.get_topofeed()

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        if is_feed(topofeed):
            res = loop.run_until_complete(fetch_data(topofeed))
            group_groups, group_endpoints = parse_source_topo(res, uidservtype, fetchtype)
            contacts = ParseContacts(logger, res, uidservtype, is_csv=False).get_contacts()
            attach_contacts_topodata(logger, contacts, group_endpoints)
        else:
            try:
                with open(topofeed) as fp:
                    js = json.load(fp)
                    group_groups, group_endpoints = parse_source_topo(js, uidservtype, fetchtype)
            except IOError as exc:
                logger.error('Customer:%s : Problem opening %s - %s' % (logger.customer, topofeed, repr(exc)))

        loop.run_until_complete(
            write_state(confcust, fixed_date, True)
        )

        webapi_opts = get_webapi_opts(cglob, confcust)

        numge = len(group_endpoints)
        numgg = len(group_groups)

        # send concurrently to WEB-API in coroutines
        if eval(globopts['GeneralPublishWebAPI'.lower()]):
            loop.run_until_complete(
                asyncio.gather(
                    send_webapi(webapi_opts, group_groups, 'groups', fixed_date),
                    send_webapi(webapi_opts, group_endpoints,'endpoints', fixed_date)
                )
            )

        if eval(globopts['GeneralWriteAvro'.lower()]):
            write_avro(confcust, group_groups, group_endpoints, fixed_date)

        logger.info('Customer:' + custname + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (fetchtype, numgg))

    except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        loop.run_until_complete(
            write_state(confcust, fixed_date, False )
        )


if __name__ == '__main__':
    main()
