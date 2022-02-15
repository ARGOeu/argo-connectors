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
import xml.dom.minidom

from concurrent.futures import ProcessPoolExecutor
from functools import partial
from urllib.parse import urlparse

import asyncio
import uvloop

from argo_egi_connectors.exceptions import ConnectorParseError, ConnectorHttpError
from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.io.ldap import LDAPSessionWithRetry
from argo_egi_connectors.io.webapi import WebAPI
from argo_egi_connectors.io.avrowrite import AvroWriter
from argo_egi_connectors.io.statewrite import state_write
from argo_egi_connectors.log import Logger
from argo_egi_connectors.mesh.srm_port import attach_srmport_topodata
from argo_egi_connectors.mesh.storage_element_path import attach_sepath_topodata
from argo_egi_connectors.mesh.contacts import attach_contacts_topodata
from argo_egi_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites
from argo_egi_connectors.parse.gocdb_contacts import ParseSiteContacts, ParseServiceEndpointContacts, ParseServiceGroupRoles, ParseSitesWithContacts, ParseServiceGroupWithContacts

from argo_egi_connectors.config import Global, CustomerConf

from argo_egi_connectors.utils import filename_date, date_check

logger = None

# GOCDB explicitly says &scope='' for all scopes
# TODO: same methods can be served on different paths
SERVICE_ENDPOINTS_PI = '/gocdbpi/private/?method=get_service_endpoint&scope='
SITES_PI = '/gocdbpi/private/?method=get_site&scope='
SERVICE_GROUPS_PI = '/gocdbpi/private/?method=get_service_group&scope='

# SITES_PI = '/vapor/downloadLavoisier/option/xml/view/vapor_sites/param/vo=biomed'
# SERVICE_ENDPOINTS_PI = '/vapor/downloadLavoisier/option/xml/view/vapor_endpoints'

ROC_CONTACTS = '/gocdbpi/private/?method=get_roc_contacts'
SITE_CONTACTS = '/gocdbpi/private/?method=get_site_contacts'
PROJECT_CONTACTS = '/gocdbpi/private/?method=get_project_contacts'
SERVICEGROUP_CONTACTS = '/gocdbpi/private/?method=get_service_group_role'

globopts = {}
custname = ''

isok = True


def parse_source_servicegroups(res, custname, uidservtype, pass_extensions):
    group_groups = ParseServiceGroups(logger, res, custname, uidservtype,
                                      pass_extensions).get_group_groups()
    group_endpoints = ParseServiceGroups(logger, res, custname, uidservtype,
                                         pass_extensions).get_group_endpoints()

    return group_groups, group_endpoints


def parse_source_endpoints(res, custname, uidservtype, pass_extensions):
    group_endpoints = ParseServiceEndpoints(logger, res, custname, uidservtype,
                                            pass_extensions).get_group_endpoints()

    return group_endpoints


def parse_source_sites(res, custname, uidservtype, pass_extensions):
    group_groups = ParseSites(logger, res, custname, uidservtype,
                              pass_extensions).get_group_groups()

    return group_groups


def parse_source_sitescontacts(res, custname):
    contacts = ParseSiteContacts(logger, res)
    return contacts.get_contacts()


def parse_source_siteswithcontacts(res, custname):
    contacts = ParseSitesWithContacts(logger, res)
    return contacts.get_contacts()

def parse_source_servicegroupscontacts(res, custname):
    contacts = ParseServiceGroupWithContacts(logger, res)
    return contacts.get_contacts()

def parse_source_servicegroupsroles(res, custname):
    contacts = ParseServiceGroupRoles(logger, res)
    return contacts.get_contacts()

def parse_source_serviceendpoints_contacts(res, custname):
    contacts = ParseServiceEndpointContacts(logger, res)
    return contacts.get_contacts()


def get_webapi_opts(cglob, confcust):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' % (logger.customer, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)
    return webapi_opts


async def write_state(confcust, fixed_date, state):
    # safely assume here one customer defined in customer file
    cust = list(confcust.get_customers())[0]
    statedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
    if fixed_date:
        await state_write(sys.argv[0], statedir, state,
                          globopts['InputStateDays'.lower()],
                          fixed_date.replace('-', '_'))
    else:
        await state_write(sys.argv[0], statedir, state,
                          globopts['InputStateDays'.lower()])


def find_next_paging_cursor_count(res):
    cursor, count = None, None

    doc = xml.dom.minidom.parseString(res)
    count = int(doc.getElementsByTagName('count')[0].childNodes[0].data)
    links = doc.getElementsByTagName('link')
    for link in links:
        if link.getAttribute('rel') == 'next':
            href = link.getAttribute('href')
            for query in href.split('&'):
                if 'next_cursor' in query:
                    cursor = query.split('=')[1]

    return count, cursor


def filter_multiple_tags(data):
    """
        Paginated content is represented with multiple XML enclosing tags
        in a single buffer:

        <?xml version="1.0" encoding="UTF-8"?>
        <results>
            topology entities
        </results>
        <?xml version="1.0" encoding="UTF-8"?>
        <results>
            topology entities
        </results>
        ...

        Remove them and leave only one enclosing.
    """
    data_lines = data.split('\n')
    data_lines = list(filter(lambda line:
                             '</results>' not in line
                             and '<results>' not in line
                             and '<?xml' not in line,
                             data_lines))
    data_lines.insert(0, '<?xml version="1.0" encoding="UTF-8"?>')
    data_lines.insert(1, '<results>')
    data_lines.append('</results>')
    return '\n'.join(data_lines)


async def fetch_data(api, auth_opts, paginated):
    feed_parts = urlparse(api)
    fetched_data = list()
    if paginated:
        count, cursor = 1, 0
        while count != 0:
            session = SessionWithRetry(logger, os.path.basename(sys.argv[0]),
                                       globopts, custauth=auth_opts)
            res = await session.http_get('{}&next_cursor={}'.format(api,
                                                                    cursor))
            count, cursor = find_next_paging_cursor_count(res)
            fetched_data.append(res)
        return filter_multiple_tags(''.join(fetched_data))

    else:
        session = SessionWithRetry(logger, os.path.basename(sys.argv[0]),
                                   globopts, custauth=auth_opts)
        res = await session.http_get(api)
        return res


async def send_webapi(webapi_opts, data, topotype, fixed_date=None):
    webapi = WebAPI(sys.argv[0], webapi_opts['webapihost'],
                    webapi_opts['webapitoken'], logger,
                    int(globopts['ConnectionRetry'.lower()]),
                    int(globopts['ConnectionTimeout'.lower()]),
                    int(globopts['ConnectionSleepRetry'.lower()]),
                    date=fixed_date)
    await webapi.send(data, topotype)


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
        logger.error('Customer:%s: %s' % (logger.customer, repr(excep)))
        raise SystemExit(1)


def get_bdii_opts(confcust):
    bdii_custopts = confcust._get_cust_options('BDIIOpts')
    if bdii_custopts:
        bdii_complete, missing = confcust.is_complete_bdii(bdii_custopts)
        if not bdii_complete:
            logger.error('%s options incomplete, missing %s' % ('bdii', ' '.join(missing)))
            raise SystemExit(1)
        return bdii_custopts
    else:
        return None


# Fetches data from LDAP, connection parameters are set in customer.conf
async def fetch_ldap_data(host, port, base, filter, attributes):
    ldap_session = LDAPSessionWithRetry(logger, int(globopts['ConnectionRetry'.lower()]),
        int(globopts['ConnectionSleepRetry'.lower()]), int(globopts['ConnectionTimeout'.lower()]))

    res = await ldap_session.search(host, port, base, filter, attributes)
    return res


def contains_exception(list):
    for a in list:
        if isinstance(a, Exception):
            return (True, a)

    return (False, None)


def main():
    global logger, globopts, confcust
    parser = argparse.ArgumentParser(description="""Fetch entities (ServiceGroups, Sites, Endpoints)
                                                    from GOCDB for every customer and job listed in customer.conf and write them
                                                    in an appropriate place""")
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY', help='write data for this date', type=str, required=False)
    args = parser.parse_args()
    group_endpoints, group_groups = [], []
    logger = Logger(os.path.basename(sys.argv[0]))

    fixed_date = None
    if args.date and date_check(args.date):
        fixed_date = args.date

    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(sys.argv[0], confpath)
    globopts = cglob.parse()
    pass_extensions = eval(globopts['GeneralPassExtensions'.lower()])

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    topofeed = confcust.get_topofeed()
    topofeedpaging = confcust.get_topofeedpaging()
    uidservtype = confcust.get_uidserviceendpoints()
    topofetchtype = confcust.get_topofetchtype()
    custname = confcust.get_custname()
    logger.customer = custname

    auth_custopts = confcust.get_authopts()
    auth_opts = cglob.merge_opts(auth_custopts, 'authentication')
    auth_complete, missing = cglob.is_complete(auth_opts, 'authentication')
    if not auth_complete:
        logger.error('%s options incomplete, missing %s' % ('authentication', ' '.join(missing)))
        raise SystemExit(1)

    bdii_opts = get_bdii_opts(confcust)

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    group_endpoints, group_groups = list(), list()
    parsed_site_contacts, parsed_servicegroups_contacts, parsed_serviceendpoint_contacts = None, None, None

    try:
        contact_coros = [
            fetch_data(topofeed + SITE_CONTACTS, auth_opts, False),
            fetch_data(topofeed + SERVICEGROUP_CONTACTS, auth_opts, False)
        ]
        contacts = loop.run_until_complete(asyncio.gather(*contact_coros, return_exceptions=True))

        exc_raised, exc = contains_exception(contacts)
        if exc_raised:
            raise ConnectorHttpError(repr(exc))

        parsed_site_contacts = parse_source_sitescontacts(contacts[0], custname)
        parsed_servicegroups_contacts = parse_source_servicegroupsroles(contacts[1], custname)


    except (ConnectorHttpError, ConnectorParseError) as exc:
        logger.warn('SITE_CONTACTS and SERVICERGOUP_CONTACT methods not implemented')

    try:
        toposcope = confcust.get_toposcope()
        topofeedendpoints = confcust.get_topofeedendpoints()
        topofeedservicegroups = confcust.get_topofeedservicegroups()
        topofeedsites = confcust.get_topofeedsites()
        global SERVICE_ENDPOINTS_PI, SERVICE_GROUPS_PI, SITES_PI
        if toposcope:
            SERVICE_ENDPOINTS_PI = SERVICE_ENDPOINTS_PI + toposcope
            SERVICE_GROUPS_PI = SERVICE_GROUPS_PI + toposcope
            SITES_PI = SITES_PI + toposcope
        if topofeedendpoints:
            SERVICE_ENDPOINTS_PI = topofeedendpoints
        else:
            SERVICE_ENDPOINTS_PI = topofeed + SERVICE_ENDPOINTS_PI
        if topofeedservicegroups:
            SERVICE_GROUPS_PI = topofeedservicegroups
        else:
            SERVICE_GROUPS_PI = topofeed + SERVICE_GROUPS_PI
        if topofeedsites:
            SITES_PI = topofeedsites
        else:
            SITES_PI = topofeed + SITES_PI

        fetched_sites, fetched_servicegroups, fetched_endpoints = None, None, None
        fetched_bdii = None

        coros = [fetch_data(SERVICE_ENDPOINTS_PI, auth_opts, topofeedpaging)]
        if 'servicegroups' in topofetchtype:
            coros.append(fetch_data(SERVICE_GROUPS_PI, auth_opts, topofeedpaging))
        if 'sites' in topofetchtype:
            coros.append(fetch_data(SITES_PI, auth_opts, topofeedpaging))

        if bdii_opts and eval(bdii_opts['bdii']):
            host = bdii_opts['bdiihost']
            port = bdii_opts['bdiiport']
            base = bdii_opts['bdiiquerybase']

            coros.append(fetch_ldap_data(host, port, base,
                                         bdii_opts['bdiiqueryfiltersrm'],
                                         bdii_opts['bdiiqueryattributessrm'].split(' ')))

            coros.append(fetch_ldap_data(host, port, base,
                                         bdii_opts['bdiiqueryfiltersepath'],
                                         bdii_opts['bdiiqueryattributessepath'].split(' ')))

        # fetch topology data concurrently in coroutines
        fetched_topology = loop.run_until_complete(asyncio.gather(*coros, return_exceptions=True))

        fetched_endpoints = fetched_topology[0]
        if bdii_opts and eval(bdii_opts['bdii']):
            fetched_bdii = list()
            fetched_bdii.append(fetched_topology[-2])
            fetched_bdii.append(fetched_topology[-1])
        if 'sites' in topofetchtype and 'servicegroups' in topofetchtype:
            fetched_servicegroups, fetched_sites = (fetched_topology[1], fetched_topology[2])
        elif 'sites' in topofetchtype:
            fetched_sites = fetched_topology[1]
        elif 'servicegroups' in topofetchtype:
            fetched_servicegroups = fetched_topology[1]

        exc_raised, exc = contains_exception(fetched_topology)
        if exc_raised:
            raise ConnectorHttpError(repr(exc))

        # proces data in parallel using multiprocessing
        executor = ProcessPoolExecutor(max_workers=3)
        parse_workers = list()
        exe_parse_source_endpoints = partial(parse_source_endpoints,
                                             fetched_endpoints, custname,
                                             uidservtype, pass_extensions)
        exe_parse_source_servicegroups = partial(parse_source_servicegroups,
                                                 fetched_servicegroups,
                                                 custname, uidservtype,
                                                 pass_extensions)
        exe_parse_source_sites = partial(parse_source_sites, fetched_sites,
                                         custname, uidservtype,
                                         pass_extensions)

        # parse topology depend on configured components fetch. we can fetch
        # only sites, only servicegroups or both.
        if fetched_servicegroups and fetched_sites:
            parse_workers.append(
                loop.run_in_executor(executor, exe_parse_source_endpoints)
            )
            parse_workers.append(
                loop.run_in_executor(executor, exe_parse_source_servicegroups)
            )
            parse_workers.append(
                loop.run_in_executor(executor, exe_parse_source_sites)
            )
        elif fetched_servicegroups and not fetched_sites:
            parse_workers.append(
                loop.run_in_executor(executor, exe_parse_source_servicegroups)
            )
        elif fetched_sites and not fetched_servicegroups:
            parse_workers.append(
                loop.run_in_executor(executor, exe_parse_source_endpoints)
            )
            parse_workers.append(
                loop.run_in_executor(executor, exe_parse_source_sites)
            )

        parsed_topology = loop.run_until_complete(asyncio.gather(*parse_workers))

        if fetched_servicegroups and fetched_sites:
            group_endpoints = parsed_topology[0]
            group_groups, group_endpoints_sg = parsed_topology[1]
            group_endpoints += group_endpoints_sg
            group_groups += parsed_topology[2]
        elif fetched_servicegroups and not fetched_sites:
            group_groups, group_endpoints = parsed_topology[0]
        elif fetched_sites and not fetched_servicegroups:
            group_endpoints = parsed_topology[0]
            group_groups = parsed_topology[1]

        # check if we fetched SRM port info and attach it appropriate endpoint
        # data
        if bdii_opts and eval(bdii_opts['bdii']):
            attach_srmport_topodata(logger, bdii_opts['bdiiqueryattributessrm'].split(' ')[0], fetched_bdii[0], group_endpoints)
            attach_sepath_topodata(logger, bdii_opts['bdiiqueryattributessepath'].split(' ')[0], fetched_bdii[1], group_endpoints)

        # parse contacts from fetched service endpoints topology, if there are
        # any
        parsed_serviceendpoint_contacts = parse_source_serviceendpoints_contacts(fetched_endpoints, custname)

        if not parsed_site_contacts and fetched_sites:
            # GOCDB has not SITE_CONTACTS, try to grab contacts from fetched
            # sites topology entities
            parsed_site_contacts = parse_source_siteswithcontacts(fetched_sites, custname)

        attach_contacts_workers = [
            loop.run_in_executor(executor, partial(attach_contacts_topodata,
                                                   logger,
                                                   parsed_site_contacts,
                                                   group_groups)),
            loop.run_in_executor(executor, partial(attach_contacts_topodata,
                                                   logger,
                                                   parsed_serviceendpoint_contacts,
                                                   group_endpoints))
        ]

        executor = ProcessPoolExecutor(max_workers=2)
        group_groups, group_endpoints = loop.run_until_complete(asyncio.gather(*attach_contacts_workers))

        if parsed_servicegroups_contacts:
            attach_contacts_topodata(logger, parsed_servicegroups_contacts, group_groups)
        elif fetched_servicegroups:
            # GOCDB has not SERVICEGROUP_CONTACTS, try to grab contacts from fetched
            # servicegroups topology entities
            parsed_servicegroups_contacts = parse_source_servicegroupscontacts(fetched_servicegroups, custname)
            attach_contacts_topodata(logger, parsed_servicegroups_contacts, group_groups)

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

        logger.info('Customer:' + custname + ' Type:%s ' % (','.join(topofetchtype)) + 'Fetched Endpoints:%d' % (numge) + ' Groups:%d' % (numgg))

    except (ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        loop.run_until_complete(
            write_state(confcust, fixed_date, False)
        )

    finally:
        loop.close()


if __name__ == '__main__':
    main()
