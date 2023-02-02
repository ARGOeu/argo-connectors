import os

from urllib.parse import urlparse

from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.webapi import WebAPI
from argo_connectors.parse.flat_downtimes import ParseDowntimes
from argo_connectors.tasks.common import write_state, write_downtimes_json as write_json


class TaskCsvDowntimes(object):
    def __init__(self, loop, logger, connector_name, globopts, webapi_opts,
                 confcust, custname, feed, current_date,
                 uidservtype, targetdate, timestamp):
        self.event_loop = loop
        self.logger = logger
        self.connector_name = connector_name
        self.globopts = globopts
        self.webapi_opts = webapi_opts
        self.confcust = confcust
        self.custname = custname
        self.feed = feed
        self.current_date = current_date
        self.uidservtype = uidservtype
        self.targetdate = targetdate
        self.timestamp = timestamp

    async def fetch_data(self):
        session = SessionWithRetry(self.logger,
                                   os.path.basename(self.connector_name),
                                   self.globopts)
        res = await session.http_get(self.feed)

        return res

    def parse_source(self, res):
        csv_downtimes = ParseDowntimes(self.logger, res, self.current_date,
                                       self.uidservtype)
        return csv_downtimes.get_data()

    async def send_webapi(self, dts):
        webapi = WebAPI(self.connector_name, self.webapi_opts['webapihost'],
                        self.webapi_opts['webapitoken'], self.logger,
                        int(self.globopts['ConnectionRetry'.lower()]),
                        int(self.globopts['ConnectionTimeout'.lower()]),
                        int(self.globopts['ConnectionSleepRetry'.lower()]),
                        date=self.targetdate)
        await webapi.send(dts, downtimes_component=True)

    async def run(self):
        try:
            write_empty = self.confcust.send_empty(self.connector_name)
            if not write_empty:
                res = await self.fetch_data()
                dts = self.parse_source(res)
            else:
                dts = []

            await write_state(self.connector_name, self.globopts, self.confcust, self.timestamp, True)

            if eval(self.globopts['GeneralPublishWebAPI'.lower()]):
                await self.send_webapi(dts)

            # we don't have multiple tenant definitions in one
            # customer file so we can safely assume one tenant/customer
            if dts or write_empty:
                cust = list(self.confcust.get_customers())[0]
                self.logger.info('Customer:%s Fetched Date:%s Endpoints:%d' %
                                 (self.confcust.get_custname(cust), self.targetdate, len(dts)))

            if eval(self.globopts['GeneralWriteJson'.lower()]):
                write_json(self.logger, self.globopts,
                           self.confcust, dts, self.timestamp)

        except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
            self.logger.error(repr(exc))
            await write_state(self.connector_name, self.globopts, self.confcust, self.timestamp, False)
