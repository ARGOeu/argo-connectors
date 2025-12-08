import asyncio

from lxml import etree

from collections import Callable

from concurrent.futures import ProcessPoolExecutor
from functools import partial

from argo_connectors.config.customer import get_custconf
from argo_connectors.config.glob import Global
from argo_connectors.exceptions import ConnectorError, ConnectorParseError
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.ldap import LDAPSessionWithRetry
from argo_connectors.io.webapi import WebAPI
from argo_connectors.log import Logger
from argo_connectors.mesh.contacts import attach_contacts_topodata
from argo_connectors.mesh.srm_port import attach_srmport_topodata
from argo_connectors.mesh.storage_element_path import attach_sepath_topodata
from argo_connectors.mesh.topotags import attach_tags
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.parse.gocdb_contacts import ParseServiceEndpointContacts, ParseSitesWithContacts, ParseServiceGroupWithContacts
from argo_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites
from argo_connectors.tasks.common import write_state, write_topo_json as write_json
from argo_connectors.utils import module_class_name, has_exception


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
    def __init__(self, res):
        self.res = res

    def __call__(self):
        try:
            return self._parse()
        except ConnectorParseError as exc:
            Logger.error(repr(exc))
            Logger.error("Tried to parse (512 chars): %.512s" % ''.join(
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
    def __init__(self, combuid):
        self.combuid = combuid

    def parse_source_servicegroups(self, res):
        group_groups = ParseServiceGroups(res, self.combuid).get_group_groups()
        group_endpoints = ParseServiceGroups(res, self.combuid).get_group_endpoints()

        return group_groups, group_endpoints

    def parse_source_endpoints(self, res):
        group_endpoints = ParseServiceEndpoints(res, self.combuid).get_group_endpoints()

        return group_endpoints

    def parse_source_sites(self, res):
        group_groups = ParseSites(res, self.combuid).get_group_groups()

        return group_groups


# basic function wrappers used to avoid class TaskParseTopology pickle
# in ProcessPoolExecutor
def parse_endpoints(custname, data, combuid):
    task = TaskParseTopology(combuid)
    return task.parse_source_endpoints(data)


def parse_sites(custname, data, combuid):
    task = TaskParseTopology(combuid)
    return task.parse_source_sites(data)


def parse_servicegroups(custname, data, combuid):
    task = TaskParseTopology(combuid)
    return task.parse_source_servicegroups(data)


class TaskParseContacts(object):
    def __init__(self, combuid):
        self.combuid = combuid

    def parse_siteswith_contacts(self, res):
        contacts = ParseSitesWithContacts(res)
        return contacts.get_contacts()

    def parse_servicegroups_contacts(self, res):
        contacts = ParseServiceGroupWithContacts(res)
        return contacts.get_contacts()

    def parse_serviceendpoints_contacts(self, res):
        contacts = ParseServiceEndpointContacts(res)
        return contacts.get_contacts()


class TaskGocdbTopology(TaskParseContacts, TaskParseTopology):
    def __init__(self, fixed_date, combuid=None):
        TaskParseTopology.__init__(self, combuid)
        super(TaskGocdbTopology, self).__init__(combuid)
        self.combuid = combuid
        self.globopts = Global.options()
        self.connector_name = Global.caller
        self.Customer = get_custconf(combuid)
        toposcope = self.Customer.opt('TopoScope')
        if toposcope:
            self.SERVICE_ENDPOINTS_PI = (self.Customer.opt('TopoFeedServiceEndpoints') or self.Customer.opt('TopoFeedEndpoints')) + toposcope
            self.SERVICE_GROUPS_PI = self.Customer.opt('TopoFeedServiceGroups') + toposcope
            self.SITES_PI = self.Customer.opt('TopoFeedSites') + toposcope
        else:
            self.SERVICE_ENDPOINTS_PI = self.Customer.opt('TopoFeedServiceEndpoints') or self.Customer.opt('TopoFeedEndpoints')
            self.SERVICE_GROUPS_PI = self.Customer.opt('TopoFeedServiceGroups')
            self.SITES_PI = self.Customer.opt('TopoFeedSites')
        self.auth_opts = self.Customer.auth_opts.opts
        self.bdii_opts = self.Customer.bdii_opts.opts
        self.custname = self.Customer.get_custname()
        self.topofeed = self.Customer.opt('TopoFeed')
        self.topofetchtype = self.Customer.get_topofetchtype()
        self.fixed_date = fixed_date
        self.uidservendp = self.Customer.opt('TopoUIDServiceEndpoints')
        self.topofeedpaging = self.Customer.opt('TopoFeedPaging')
        self.notification_flag = self.Customer.opt('HonorNotificationFlag')
        self.tags_ge = self.Customer.opt('TopoTagServiceEndpoints')
        self.tags_gg = self.Customer.opt('TopoTagServiceGroups') + self.Customer.opt('TopoTagSites')

    async def fetch_ldap_data(self, host, port, base, filter, attributes):
        ldap_session = LDAPSessionWithRetry()

        res = await ldap_session.search(host, port, base, filter, attributes)
        return res

    async def fetch_data(self, api):
        fetched_data = list()
        if self.topofeedpaging:
            count, cursor = 1, 0
            while count != 0:
                session = SessionWithRetry(custauth=self.auth_opts)
                res = await session.http_get('{}&next_cursor={}'.format(api,
                                                                        cursor))

                try:
                    next_cursor = find_next_paging_cursor_count(res)
                    count, cursor = next_cursor()
                    fetched_data.append(res)

                except ConnectorParseError as exc:
                    await session.close()
                    raise exc

            return filter_multiple_tags(''.join(fetched_data))

        else:
            session = SessionWithRetry(custauth=self.auth_opts)
            res = await session.http_get(api)

            return res

    async def run(self):
        fetched_sites, fetched_servicegroups, fetched_endpoints = None, None, None
        fetched_bdii = None
        loop = asyncio.get_running_loop()

        group_endpoints, group_groups = list(), list()
        parsed_site_contacts, parsed_servicegroups_contacts, parsed_serviceendpoint_contacts = None, None, None

        coros = [self.fetch_data(self.SERVICE_ENDPOINTS_PI)]
        if 'servicegroups' in self.topofetchtype:
            coros.append(self.fetch_data(self.SERVICE_GROUPS_PI))
        if 'sites' in self.topofetchtype:
            coros.append(self.fetch_data(self.SITES_PI))

        if self.bdii_opts.get('bdii', None):
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
        fetched_topology = await asyncio.gather(*coros, return_exceptions=True)

        fetched_endpoints = fetched_topology[0]
        if self.bdii_opts.get('bdii', None):
            fetched_bdii = list()
            fetched_bdii.append(fetched_topology[-2])
            fetched_bdii.append(fetched_topology[-1])
            if not all(fetched_bdii):
                raise ConnectorError("LDAP problem")

        if 'sites' in self.topofetchtype and 'servicegroups' in self.topofetchtype:
            fetched_servicegroups, fetched_sites = (
                fetched_topology[1], fetched_topology[2])
        elif 'sites' in self.topofetchtype:
            fetched_sites = fetched_topology[1]
        elif 'servicegroups' in self.topofetchtype:
            fetched_servicegroups = fetched_topology[1]

        exc_raised, exc = has_exception(fetched_topology)
        if exc_raised:
            raise ConnectorError(repr(exc))

        # proces data in parallel using multiprocessing
        executor = ProcessPoolExecutor(max_workers=3)
        parse_workers = list()
        exe_parse_source_endpoints = partial(parse_endpoints,
                                             self.custname,
                                             fetched_endpoints,
                                             self.combuid)
        exe_parse_source_servicegroups = partial(parse_servicegroups,
                                                 self.custname,
                                                 fetched_servicegroups,
                                                 self.combuid)
        exe_parse_source_sites = partial(parse_sites,
                                         self.custname, fetched_sites,
                                         self.combuid)

        # parse topology depend on configured components fetch. we can fetch
        # only sites, only servicegroups or both.

        if fetched_servicegroups and fetched_sites:
            parse_workers.append(
                loop.run_in_executor(executor, exe_parse_source_endpoints)
            )
            parse_workers.append(
                loop.run_in_executor(
                    executor, exe_parse_source_servicegroups)
            )
            parse_workers.append(
                loop.run_in_executor(executor, exe_parse_source_sites)
            )
        elif fetched_servicegroups and not fetched_sites:
            parse_workers.append(
                loop.run_in_executor(
                    executor, exe_parse_source_servicegroups)
            )
        elif fetched_sites and not fetched_servicegroups:
            parse_workers.append(
                loop.run_in_executor(executor, exe_parse_source_endpoints)
            )
            parse_workers.append(
                loop.run_in_executor(executor, exe_parse_source_sites)
            )

        parsed_topology = await asyncio.gather(*parse_workers)

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
        if self.bdii_opts.get('bdii', None):
            attach_srmport_topodata(self.bdii_opts['bdiiqueryattributessrm'].split(
                ' ')[0], fetched_bdii[0], group_endpoints)
            attach_sepath_topodata(self.bdii_opts['bdiiqueryattributessepath'].split(
                ' ')[0], fetched_bdii[1], group_endpoints)

        # parse contacts from fetched service endpoints topology, if there are
        # any
        parsed_serviceendpoint_contacts = self.parse_serviceendpoints_contacts(fetched_endpoints)

        if fetched_sites:
            parsed_site_contacts = self.parse_siteswith_contacts(fetched_sites)

        attach_contacts_workers = [
            loop.run_in_executor(executor, partial(attach_contacts_topodata,
                                                   parsed_site_contacts,
                                                   group_groups,
                                                   self.notification_flag)),
            loop.run_in_executor(executor, partial(attach_contacts_topodata,
                                                   parsed_serviceendpoint_contacts,
                                                   group_endpoints,
                                                   self.notification_flag))
        ]

        executor = ProcessPoolExecutor(max_workers=2)
        group_groups, group_endpoints = await asyncio.gather(*attach_contacts_workers)

        if fetched_servicegroups:
            parsed_servicegroups_contacts = self.parse_servicegroups_contacts(fetched_servicegroups)
            attach_contacts_topodata(parsed_servicegroups_contacts,
                                     group_groups, self.notification_flag)

        if not self.combuid:
            await write_state(self.fixed_date, True)

        if self.tags_ge or self.tags_gg:
            group_groups, group_endpoints = await attach_tags(group_groups,
                                                              group_endpoints,
                                                              self.tags_gg,
                                                              self.tags_ge)

        numge = len(group_endpoints)
        numgg = len(group_groups)

        if not self.combuid:
            # send concurrently to WEB-API in coroutines
            if self.globopts['GeneralPublishWebAPI'.lower()]:
                webapi = WebAPI(date=self.fixed_date, combuid=self.combuid)
                await asyncio.gather(
                    webapi.send(group_groups, 'groups'),
                    webapi.send(group_endpoints, 'endpoints')
                )
                await webapi.session.close()

            if self.globopts['GeneralWriteJson'.lower()]:
                write_json(group_groups, group_endpoints,
                           self.fixed_date)

        if not self.combuid:
            Logger.info('Customer:' + self.custname + ' Type:%s ' % (','.join(
                self.topofetchtype)) + 'Fetched Endpoints:%d' % (numge) + ' Groups:%d' % (numgg))
        else:
            Logger.info(module_class_name(self) + ' ID:' + self.combuid + ' Customer:' + self.custname + ' Type:%s ' % (','.join(
                self.topofetchtype)) + 'Fetched Endpoints:%d' % (numge) + ' Groups:%d' % (numgg))

            return group_groups, group_endpoints
