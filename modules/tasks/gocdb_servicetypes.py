import os
import asyncio

from urllib.parse import urlparse

from argo_connectors.config.customer import get_custconf
from argo_connectors.config.glob import Global
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.webapi import WebAPI
from argo_connectors.log import Logger
from argo_connectors.parse.gocdb_servicetypes import ParseGocdbServiceTypes
from argo_connectors.parse.webapi_servicetypes import ParseWebApiServiceTypes
from argo_connectors.tasks.common import write_state, write_servicetypes_json as write_json
from argo_connectors.utils import module_class_name, has_exception


class TaskGocdbServiceTypes(object):
    def __init__(self, fixed_date, initsync=False, combuid=None):
        self.Customer = get_custconf(combuid)
        self.feed = self.Customer.opt('ServiceTypesFeed') or self.Customer.opt('TopoFeed')
        self.connector_name = Global.caller
        self.globopts = Global.options()
        self.auth_opts = self.Customer.auth_opts.opts
        self.custname = self.Customer.get_custname()
        self.fixed_date = fixed_date
        self.initsync = initsync
        self.combuid = combuid

    async def fetch_data(self):
        feed_parts = urlparse(self.feed)
        session = SessionWithRetry(custauth=self.auth_opts)
        res = await session.http_get('{}://{}{}?{}'.format(feed_parts.scheme,
                                                           feed_parts.netloc,
                                                           feed_parts.path,
                                                           feed_parts.query))
        return res

    def parse_source(self, res):
        gocdb = ParseGocdbServiceTypes(res)
        return gocdb.get_data()

    def parse_webapi_poem(self, res):
        webapi = ParseWebApiServiceTypes(res)
        return webapi.get_data(tag='poem')

    async def run(self):
        try:
            coros = [self.fetch_data()]

            if not self.initsync:
                webapi = WebAPI(date=self.fixed_date, combuid=self.combuid)
                coros.append(webapi.get('service-types', jsonret=False))

            fetched_data = await asyncio.gather(*coros, return_exceptions=True)

            exc_raised, exc = has_exception(fetched_data)
            if exc_raised:
                raise ConnectorError(repr(exc))

            if not self.initsync:
                res, res_webapi = fetched_data
            else:
                res = fetched_data[0]

            # small set data, parsing sequentially
            service_types = self.parse_source(res)
            if not self.initsync:
                service_types_poem = self.parse_webapi_poem(res_webapi)
                service_types = service_types + service_types_poem
                service_types = sorted(service_types, key=lambda s: s['name'].lower())

            if not self.combuid:
                await write_state(self.fixed_date, True)

            if not self.combuid:
                if self.globopts['GeneralPublishWebAPI'.lower()]:
                    webapi = WebAPI(date=self.fixed_date, combuid=self.combuid)
                    await webapi.send(service_types, 'service-types')
                    await webapi.session.close()

                if self.globopts['GeneralWriteJson'.lower()]:
                    write_json(service_types,
                               self.fixed_date)

            if not self.combuid:
                Logger.info('Customer:' + self.custname + ' Fetched GOCDB ServiceTypes:%d' % (len(service_types)))
            else:
                Logger.info(module_class_name(self) + ' ID:' + self.combuid + ' Customer:' + self.custname + ' Fetched GOCDB ServiceTypes:%d' % (len(service_types)))

                return service_types

        except (ConnectorError, ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
            Logger.error(repr(exc))
            if not self.combuid:
                await write_state(self.fixed_date, False)
            else:
                raise ConnectorError(repr(exc)) from exc
