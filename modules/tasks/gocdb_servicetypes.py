import os

from urllib.parse import urlparse

from argo_connectors.io.http import SessionWithRetry
from argo_connectors.parse.gocdb_servicetypes import ParseGocdbServiceTypes
from argo_connectors.io.webapi import WebAPI
from argo_connectors.tasks.common import write_state, write_downtimes_avro as write_avro
from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError


class TaskGocdbServiceTypes(object):
    def __init__(self, loop, logger, connector_name, globopts, auth_opts,
                 webapi_opts, confcust, custname, feed, timestamp):
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

    async def send_webapi(self, data):
        webapi = WebAPI(self.connector_name, self.webapi_opts['webapihost'],
                        self.webapi_opts['webapitoken'], self.logger,
                        int(self.globopts['ConnectionRetry'.lower()]),
                        int(self.globopts['ConnectionTimeout'.lower()]),
                        int(self.globopts['ConnectionSleepRetry'.lower()]),
                        date=self.timestamp)
        await webapi.send(data, 'service-types')

    def parse_source(self, res):
        gocdb = ParseGocdbServiceTypes(self.logger, res)
        return gocdb.get_data()

    async def run(self):
        try:
            res = await self.fetch_data()
            service_types = self.parse_source(res)

            await write_state(self.connector_name, self.globopts, self.confcust, self.timestamp, True)

            if eval(self.globopts['GeneralPublishWebAPI'.lower()]):
                await self.send_webapi(service_types)

            self.logger.info('Customer:' + self.custname + ' Fetched GOCDB ServiceTypes:%d' % (len(service_types)))

        except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
            self.logger.error(repr(exc))
            await write_state(self.connector_name, self.globopts, self.confcust, self.timestamp, False)
