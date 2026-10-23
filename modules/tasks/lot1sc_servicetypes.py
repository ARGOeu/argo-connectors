import asyncio

from urllib.parse import urlparse

from argo_connectors.config.customer import get_custconf
from argo_connectors.config.glob import Global
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.webapi import WebAPI
from argo_connectors.log import Logger
from argo_connectors.parse.lot1sc_servicetypes import ParseLot1ScServiceTypes
from argo_connectors.parse.webapi_servicetypes import ParseWebApiServiceTypes
from argo_connectors.tasks.common import write_state, write_servicetypes_json as write_json
from argo_connectors.utils import module_class_name, has_exception


class TaskLot1ScServiceTypes:
    def __init__(self, fixed_date, initsync=False, combuid=None):
        self.Customer = get_custconf(combuid)
        self.connector_name = Global.caller
        self.globopts = Global.options()
        self.custname = self.Customer.get_custname()
        self.feed = self.Customer.opt('ServiceTypesFeed') or self.Customer.opt('TopoFeed')
        self.tiers = self.Customer.opt('TopoTiers')
        if not isinstance(self.tiers, list):
            self.tiers = [self.tiers]
        self.fixed_date = fixed_date
        self.initsync = initsync
        self.combuid = combuid

    async def fetch_data(self, tier):
        remote_topo = urlparse(self.feed)
        session = SessionWithRetry()
        return await session.http_get('{}://{}{}?{}{}'.format(remote_topo.scheme,
                                                              remote_topo.netloc,
                                                              remote_topo.path,
                                                              remote_topo.query,
                                                              tier))

    def parse_source(self, res):
        lot1sc = ParseLot1ScServiceTypes(res)
        return lot1sc.get_data()

    def parse_webapi_poem(self, res):
        webapi = ParseWebApiServiceTypes(res)
        return webapi.get_data(tag='poem')

    async def run(self):
        try:
            coros = [self.fetch_data(tier) for tier in self.tiers]

            if not self.initsync:
                webapi = WebAPI(date=self.fixed_date, combuid=self.combuid)
                coros.append(webapi.get('service-types', jsonret=False))

            fetched_data = await asyncio.gather(*coros, return_exceptions=True)

            exc_raised, exc = has_exception(fetched_data)
            if exc_raised:
                raise ConnectorError(repr(exc))

            if not self.initsync:
                lot1sc_data = fetched_data[:-1]
                res_webapi = fetched_data[-1]
            else:
                lot1sc_data = fetched_data

            service_types_by_name = dict()
            for res in lot1sc_data:
                for service_type in self.parse_source(res):
                    service_types_by_name[service_type['name']] = service_type
            service_types = list(service_types_by_name.values())

            if not self.initsync:
                service_types += self.parse_webapi_poem(res_webapi)

            service_types = sorted(service_types, key=lambda s: s['name'].lower())

            if not self.combuid:
                await write_state(self.fixed_date, True)

            if not self.combuid:
                if self.globopts['GeneralPublishWebAPI'.lower()]:
                    webapi = WebAPI(date=self.fixed_date, combuid=self.combuid)
                    await webapi.send(service_types, 'service-types')
                    await webapi.session.close()

                if self.globopts['GeneralWriteJson'.lower()]:
                    write_json(service_types, self.fixed_date)

            if not self.combuid:
                Logger.info('Customer:' + self.custname + ' Fetched LOT1SC ServiceTypes:%d' % (len(service_types)))
            else:
                Logger.info(module_class_name(self) + ' ID:' + self.combuid + ' Customer:' + self.custname + ' Fetched LOT1SC ServiceTypes:%d' % (len(service_types)))

                return service_types

        except (ConnectorError, ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
            Logger.error(repr(exc))
            if not self.combuid:
                await write_state(self.fixed_date, False)
            else:
                raise ConnectorError(repr(exc)) from exc
