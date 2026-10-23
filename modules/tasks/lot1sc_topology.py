import asyncio

from urllib.parse import urlparse

from argo_connectors.config.customer import get_custconf
from argo_connectors.config.glob import Global
from argo_connectors.exceptions import ConnectorError
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.webapi import WebAPI
from argo_connectors.log import Logger
from argo_connectors.mesh.topotags import attach_tags
from argo_connectors.parse.lot1sc_topology import ParseLot1ScEndpoints
from argo_connectors.tasks.common import write_state, write_topo_json as write_json
from argo_connectors.utils import module_class_name, has_exception


class TaskLot1ScTopology():
    def __init__(self, fixed_date, combuid=None):
        self.connector_name = Global.caller
        self.globopts = Global.options()
        self.Customer = get_custconf(combuid)
        self.custname = self.Customer.get_custname()
        self.topofeed = self.Customer.opt('TopoFeed')
        self.fetchtype = self.Customer.get_topofetchtype()[0]
        self.fixed_date = fixed_date
        self.uidservendp = self.Customer.opt('TopoUIDServiceEndpoints')
        self.tiers = self.Customer.opt('TopoTiers')
        if not isinstance(self.tiers, list):
            self.tiers = [self.tiers]
        self.combuid = combuid
        self.tags_ge = self.Customer.opt('TopoTagServiceEndpoints')
        self.tags_gg = self.Customer.opt('TopoTagServiceGroups') + self.Customer.opt('TopoTagSites')
        self.toposetype = self.Customer.opt('TopoSetType')

    async def fetch_data(self, tier):
        remote_topo = urlparse(self.topofeed)
        session = SessionWithRetry()
        res = await \
            session.http_get('{}://{}{}?{}{}'.format(remote_topo.scheme,
                                                     remote_topo.netloc,
                                                     remote_topo.path,
                                                     remote_topo.query, tier))
        return res

    def parse_source_topo(self, res, tier):
        topo = ParseLot1ScEndpoints(res, tier, self.combuid)
        group_groups = topo.get_group_groups()
        group_endpoints = topo.get_group_endpoints()

        return group_groups, group_endpoints

    async def run(self):
        coros = list()
        for tier in self.tiers:
            coros.append(self.fetch_data(tier))
        # fetch topology data concurrently in coroutines

        fetched_data = await asyncio.gather(*coros, return_exceptions=True)

        exc_raised, exc = has_exception(fetched_data)
        if exc_raised:
            raise ConnectorError(repr(exc))

        group_groups, group_endpoints = list(), list()

        for tier in self.tiers:
            gg, ge = self.parse_source_topo(fetched_data[self.tiers.index(tier)], tier=tier)
            group_groups += gg
            group_endpoints += ge

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
            Logger.info('Customer:' + self.custname + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (self.fetchtype, numgg))
        else:
            Logger.info(module_class_name(self) + ' ID:' + self.combuid + ' Customer:' + self.custname + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (self.fetchtype, numgg))

            return group_groups, group_endpoints
