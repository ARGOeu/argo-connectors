import os
import asyncio
from lxml import etree

from collections import Callable
from urllib.parse import urlparse

from concurrent.futures import ProcessPoolExecutor
from functools import partial

from argo_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites
from argo_connectors.parse.gocdb_contacts import ParseServiceEndpointContacts, ParseSitesWithContacts, ParseServiceGroupWithContacts
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.ldap import LDAPSessionWithRetry
from argo_connectors.io.statewrite import state_write
from argo_connectors.io.webapi import WebAPI
from argo_connectors.mesh.contacts import attach_contacts_topodata
from argo_connectors.mesh.srm_port import attach_srmport_topodata
from argo_connectors.mesh.storage_element_path import attach_sepath_topodata
from argo_connectors.tasks.common import write_state, write_topo_json as write_json
from argo_connectors.parse.base import ParseHelpers


def contains_exception(list):
    for a in list:
        if isinstance(a, Exception):
            return (True, a)

    return (False, None)


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


class find_next_paging_cursor_count(ParseHelpers, Callable):
    def __init__(self, logger, res):
        self.res = res
        self.logger = logger

    def __call__(self):
        try:
            return self._parse()
        except ConnectorParseError as exc:
            self.logger.error(repr(exc))
            self.logger.error("Tried to parse (512 chars): %.512s" % ''.join(
                self.res.replace('\r\n', '').replace('\n', '')))
            raise ConnectorParseError(exc)

    def _parse(self):
        cursor, count = None, None

        doc = self.parse_xml(self.res)
        xml_bytes = doc.encode("utf-8")
        sites = etree.fromstring(xml_bytes)

        for cnt in sites.xpath('.//count'):
            count = int(cnt.text)

        for lnk in sites.xpath('.//link'):
            if lnk.attrib["rel"] == "next":
                href = lnk.attrib["href"]
                for query in href.split('&'):
                    if 'next_cursor' in query:
                        cursor = query.split('=')[1]

        return count, cursor


class TaskParseTopology(object):
    def __init__(self, logger, custname, uidservendp, pass_extensions,
                 notiflag):
        self.logger = logger
        self.custname = custname
        self.uidservendp = uidservendp
        self.pass_extensions = pass_extensions
        self.notification_flag = notiflag

    def parse_source_servicegroups(self, res):
        group_groups = ParseServiceGroups(self.logger, res, self.custname,
                                          self.uidservendp,
                                          self.pass_extensions,
                                          self.notification_flag).get_group_groups()
        group_endpoints = ParseServiceGroups(self.logger, res, self.custname,
                                             self.uidservendp,
                                             self.pass_extensions,
                                             self.notification_flag).get_group_endpoints()

        return group_groups, group_endpoints

    def parse_source_endpoints(self, res):
        group_endpoints = ParseServiceEndpoints(self.logger, res, self.custname, self.uidservendp,
                                                self.pass_extensions,
                                                self.notification_flag).get_group_endpoints()

        return group_endpoints

    def parse_source_sites(self, res):
        group_groups = ParseSites(self.logger, res, self.custname,
                                  self.uidservendp,
                                  self.pass_extensions,
                                  self.notification_flag).get_group_groups()

        return group_groups


# basic function wrappers used because to avoid class TaskParseTopology pickle
# in ProcessPoolExecutor
def parse_endpoints(logger, custname, uidservendp, pass_extensions,
                    notification_flag, data):
    task = TaskParseTopology(logger, custname, uidservendp, pass_extensions,
                             notification_flag)
    return task.parse_source_endpoints(data)


def parse_sites(logger, custname, uidservendp, pass_extensions,
                notification_flag, data):
    task = TaskParseTopology(
        logger, custname, uidservendp, pass_extensions, notification_flag)
    return task.parse_source_sites(data)


def parse_servicegroups(logger, custname, uidservendp, pass_extensions,
                        notification_flag, data):
    task = TaskParseTopology(logger, custname, uidservendp, pass_extensions,
                             notification_flag)
    return task.parse_source_servicegroups(data)


class TaskParseContacts(object):
    def __init__(self, logger):
        self.logger = logger

    def parse_siteswith_contacts(self, res):
        contacts = ParseSitesWithContacts(self.logger, res)
        return contacts.get_contacts()

    def parse_servicegroups_contacts(self, res):
        contacts = ParseServiceGroupWithContacts(self.logger, res)
        return contacts.get_contacts()

    def parse_serviceendpoints_contacts(self, res):
        contacts = ParseServiceEndpointContacts(self.logger, res)
        return contacts.get_contacts()


class TaskGocdbTopology(TaskParseContacts, TaskParseTopology):
    def __init__(self, loop, logger, connector_name, SERVICE_ENDPOINTS_PI,
                 SERVICE_GROUPS_PI, SITES_PI, globopts, auth_opts, webapi_opts,
                 bdii_opts, confcust, custname, topofeed, topofetchtype,
                 fixed_date, uidservendp, pass_extensions, topofeedpaging,
                 notiflag):
        TaskParseTopology.__init__(self, logger, custname, uidservendp,
                                   pass_extensions, notiflag)
        super(TaskGocdbTopology, self).__init__(logger)
        self.loop = loop
        self.logger = logger
        self.connector_name = connector_name
        self.SERVICE_ENDPOINTS_PI = SERVICE_ENDPOINTS_PI
        self.SERVICE_GROUPS_PI = SERVICE_GROUPS_PI
        self.SITES_PI = SITES_PI
        self.globopts = globopts
        self.auth_opts = auth_opts
        self.webapi_opts = webapi_opts
        self.bdii_opts = bdii_opts
        self.confcust = confcust
        self.custname = custname
        self.topofeed = topofeed
        self.topofetchtype = topofetchtype
        self.fixed_date = fixed_date
        self.uidservendp = uidservendp
        self.pass_extensions = pass_extensions
        self.topofeedpaging = topofeedpaging
        self.notification_flag = notiflag

    async def fetch_ldap_data(self, host, port, base, filter, attributes):
        ldap_session = LDAPSessionWithRetry(self.logger, int(self.globopts['ConnectionRetry'.lower()]),
                                            int(self.globopts['ConnectionSleepRetry'.lower()]), int(self.globopts['ConnectionTimeout'.lower()]))

        res = await ldap_session.search(host, port, base, filter, attributes)
        return res

    async def fetch_data(self, api):
        feed_parts = urlparse(api)
        fetched_data = list()
        if self.topofeedpaging:
            count, cursor = 1, 0
            while count != 0:
                session = SessionWithRetry(self.logger,
                                           os.path.basename(
                                               self.connector_name),
                                           self.globopts,
                                           custauth=self.auth_opts)
                res = await session.http_get('{}&next_cursor={}'.format(api,
                                                                        cursor))

                try:
                    next_cursor = find_next_paging_cursor_count(
                        self.logger, res)
                    count, cursor = next_cursor()
                    fetched_data.append(res)

                except ConnectorParseError as exc:
                    await session.close()
                    raise exc

            return filter_multiple_tags(''.join(fetched_data))

        else:
            session = SessionWithRetry(self.logger,
                                       os.path.basename(self.connector_name),
                                       self.globopts, custauth=self.auth_opts)
            res = await session.http_get(api)

            return res

    async def send_webapi(self, data, topotype):
        webapi = WebAPI(self.connector_name, self.webapi_opts['webapihost'],
                        self.webapi_opts['webapitoken'], self.logger,
                        int(self.globopts['ConnectionRetry'.lower()]),
                        int(self.globopts['ConnectionTimeout'.lower()]),
                        int(self.globopts['ConnectionSleepRetry'.lower()]),
                        self.globopts['ConnectionRetryRandom'.lower()],
                        int(self.globopts['ConnectionSleepRandomRetryMax'.lower()]),
                        date=self.fixed_date)
        await webapi.send(data, topotype)

    async def run(self):
        fetched_sites, fetched_servicegroups, fetched_endpoints = None, None, None
        fetched_bdii = None

        group_endpoints, group_groups = list(), list()
        parsed_site_contacts, parsed_servicegroups_contacts, parsed_serviceendpoint_contacts = None, None, None

        coros = [self.fetch_data(self.SERVICE_ENDPOINTS_PI)]
        if 'servicegroups' in self.topofetchtype:
            coros.append(self.fetch_data(self.SERVICE_GROUPS_PI))
        if 'sites' in self.topofetchtype:
            coros.append(self.fetch_data(self.SITES_PI))

        if self.bdii_opts and eval(self.bdii_opts['bdii']):
            host = self.bdii_opts['bdiihost']
            port = self.bdii_opts['bdiiport']
            base = self.bdii_opts['bdiiquerybase']

            coros.append(
                self.fetch_ldap_data(host, port, base,
                                     self.bdii_opts['bdiiqueryfiltersrm'],
                                     self.bdii_opts['bdiiqueryattributessrm'].split(' '))
            )

            coros.append(
                self.fetch_ldap_data(host, port, base,
                                     self.bdii_opts['bdiiqueryfiltersepath'],
                                     self.bdii_opts['bdiiqueryattributessepath'].split(' '))
            )

        # fetch topology data concurrently in coroutines
        fetched_topology = await asyncio.gather(*coros, loop=self.loop, return_exceptions=True)

        fetched_endpoints = fetched_topology[0]
        if self.bdii_opts and eval(self.bdii_opts['bdii']):
            fetched_bdii = list()
            fetched_bdii.append(fetched_topology[-2])
            fetched_bdii.append(fetched_topology[-1])
        if 'sites' in self.topofetchtype and 'servicegroups' in self.topofetchtype:
            fetched_servicegroups, fetched_sites = (
                fetched_topology[1], fetched_topology[2])
        elif 'sites' in self.topofetchtype:
            fetched_sites = fetched_topology[1]
        elif 'servicegroups' in self.topofetchtype:
            fetched_servicegroups = fetched_topology[1]

        exc_raised, exc = contains_exception(fetched_topology)
        if exc_raised:
            raise ConnectorError(repr(exc))

        # proces data in parallel using multiprocessing
        executor = ProcessPoolExecutor(max_workers=3)
        parse_workers = list()
        exe_parse_source_endpoints = partial(parse_endpoints, self.logger,
                                             self.custname, self.uidservendp,
                                             self.pass_extensions,
                                             self.notification_flag,
                                             fetched_endpoints)
        exe_parse_source_servicegroups = partial(parse_servicegroups,
                                                 self.logger, self.custname,
                                                 self.uidservendp,
                                                 self.pass_extensions,
                                                 self.notification_flag,
                                                 fetched_servicegroups)
        exe_parse_source_sites = partial(parse_sites, self.logger,
                                         self.custname, self.uidservendp,
                                         self.pass_extensions,
                                         self.notification_flag, fetched_sites)

        # parse topology depend on configured components fetch. we can fetch
        # only sites, only servicegroups or both.

        if fetched_servicegroups and fetched_sites:
            parse_workers.append(
                self.loop.run_in_executor(executor, exe_parse_source_endpoints)
            )
            parse_workers.append(
                self.loop.run_in_executor(
                    executor, exe_parse_source_servicegroups)
            )
            parse_workers.append(
                self.loop.run_in_executor(executor, exe_parse_source_sites)
            )
        elif fetched_servicegroups and not fetched_sites:
            parse_workers.append(
                self.loop.run_in_executor(
                    executor, exe_parse_source_servicegroups)
            )
        elif fetched_sites and not fetched_servicegroups:
            parse_workers.append(
                self.loop.run_in_executor(executor, exe_parse_source_endpoints)
            )
            parse_workers.append(
                self.loop.run_in_executor(executor, exe_parse_source_sites)
            )

        parsed_topology = await asyncio.gather(*parse_workers, loop=self.loop)

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
        if self.bdii_opts and eval(self.bdii_opts['bdii']):
            attach_srmport_topodata(self.logger, self.bdii_opts['bdiiqueryattributessrm'].split(
                ' ')[0], fetched_bdii[0], group_endpoints)
            attach_sepath_topodata(self.logger, self.bdii_opts['bdiiqueryattributessepath'].split(
                ' ')[0], fetched_bdii[1], group_endpoints)

        # parse contacts from fetched service endpoints topology, if there are
        # any
        parsed_serviceendpoint_contacts = self.parse_serviceendpoints_contacts(fetched_endpoints)

        if fetched_sites:
            parsed_site_contacts = self.parse_siteswith_contacts(fetched_sites)

        attach_contacts_workers = [
            self.loop.run_in_executor(executor,
                                      partial(attach_contacts_topodata, self.logger,
                                              parsed_site_contacts,
                                              group_groups, self.notification_flag)),
            self.loop.run_in_executor(executor,
                                      partial(attach_contacts_topodata, self.logger,
                                              parsed_serviceendpoint_contacts,
                                              group_endpoints, self.notification_flag))
        ]

        executor = ProcessPoolExecutor(max_workers=2)
        group_groups, group_endpoints = await asyncio.gather(*attach_contacts_workers, loop=self.loop)

        if fetched_servicegroups:
            parsed_servicegroups_contacts = self.parse_servicegroups_contacts(fetched_servicegroups)
            attach_contacts_topodata(self.logger,
                                     parsed_servicegroups_contacts,
                                     group_groups, self.notification_flag)

        await write_state(self.connector_name, self.globopts, self.confcust, self.fixed_date, True)

        numge = len(group_endpoints)
        numgg = len(group_groups)

        # send concurrently to WEB-API in coroutines
        if eval(self.globopts['GeneralPublishWebAPI'.lower()]):
            await asyncio.gather(
                self.send_webapi(group_groups, 'groups'),
                self.send_webapi(group_endpoints, 'endpoints')
            )

        if eval(self.globopts['GeneralWriteJson'.lower()]):
            write_json(self.logger, self.globopts, self.confcust,
                       group_groups, group_endpoints, self.fixed_date)

        self.logger.info('Customer:' + self.custname + ' Type:%s ' % (','.join(
            self.topofetchtype)) + 'Fetched Endpoints:%d' % (numge) + ' Groups:%d' % (numgg))
