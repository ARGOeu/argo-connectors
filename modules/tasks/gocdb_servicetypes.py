import os
import asyncio

from urllib.parse import urlparse

from argo_connectors.io.http import SessionWithRetry
from argo_connectors.parse.gocdb_servicetypes import ParseGocdbServiceTypes
from argo_connectors.parse.webapi_servicetypes import ParseWebApiServiceTypes
from argo_connectors.io.webapi import WebAPI
from argo_connectors.tasks.common import write_state, write_downtimes_json as write_json
from argo_connectors.exceptions import ConnectorError, ConnectorHttpError, ConnectorParseError


def contains_exception(list):
    for a in list:
        if isinstance(a, Exception):
            return (True, a)

    return (False, None)


class TaskGocdbServiceTypes(object):
    def __init__(self, loop, logger, connector_name, globopts, auth_opts,
                 webapi_opts, confcust, custname, feed, timestamp, initsync):
        self.logger = logger
        self.loop = loop
        self.connector_name = connector_name
        self.globopts = globopts
        self.auth_opts = auth_opts
        self.webapi_opts = webapi_opts
        self.confcust = confcust
        self.custname = custname
        self.feed = feed
        self.timestamp = timestamp
        self.initsync = initsync

    async def fetch_data(self):
        feed_parts = urlparse(self.feed)
        session = SessionWithRetry(self.logger,
                                   os.path.basename(self.connector_name),
                                   self.globopts, custauth=self.auth_opts)
        res = await session.http_get('{}://{}{}?{}'.format(feed_parts.scheme,
                                                           feed_parts.netloc,
                                                           feed_parts.path,
                                                           feed_parts.query))
        return res

    async def fetch_webapi(self):
        webapi = WebAPI(self.connector_name, self.webapi_opts['webapihost'],
                        self.webapi_opts['webapitoken'], self.logger,
                        int(self.globopts['ConnectionRetry'.lower()]),
                        int(self.globopts['ConnectionTimeout'.lower()]),
                        int(self.globopts['ConnectionSleepRetry'.lower()]),
                        self.globopts['ConnectionRetryRandom'.lower()],
                        int(self.globopts['ConnectionSleepRandomRetryMax'.lower()]),
                        date=self.timestamp)
        return await webapi.get('service-types', jsonret=False)

    async def send_webapi(self, data):
        webapi = WebAPI(self.connector_name, self.webapi_opts['webapihost'],
                        self.webapi_opts['webapitoken'], self.logger,
                        int(self.globopts['ConnectionRetry'.lower()]),
                        int(self.globopts['ConnectionTimeout'.lower()]),
                        int(self.globopts['ConnectionSleepRetry'.lower()]),
                        self.globopts['ConnectionRetryRandom'.lower()],
                        int(self.globopts['ConnectionSleepRandomRetryMax'.lower()]),
                        date=self.timestamp)
        await webapi.send(data, 'service-types')

    def parse_source(self, res):
        gocdb = ParseGocdbServiceTypes(self.logger, res)
        return gocdb.get_data()

    def parse_webapi_poem(self, res):
        webapi = ParseWebApiServiceTypes(self.logger, res)
        return webapi.get_data(tag='poem')

    async def run(self):
        try:
            coros = [self.fetch_data()]

            if not self.initsync:
                coros.append(self.fetch_webapi())

            fetched_data = await asyncio.gather(*coros, loop=self.loop, return_exceptions=True)

            exc_raised, exc = contains_exception(fetched_data)
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
                service_types = sorted(service_types,  key=lambda s: s['name'].lower())

            await write_state(self.connector_name, self.globopts, self.confcust, self.timestamp, True)

            if eval(self.globopts['GeneralPublishWebAPI'.lower()]):
                await self.send_webapi(service_types)
            self.logger.info('Customer:' + self.custname + ' Fetched GOCDB ServiceTypes:%d' % (len(service_types)))

        except (ConnectorError, ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
            self.logger.error(repr(exc))
            await write_state(self.connector_name, self.globopts, self.confcust, self.timestamp, False)
