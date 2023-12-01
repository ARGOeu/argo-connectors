import os
import asyncio

from urllib.parse import urlparse

from argo_connectors.singleton_config import ConfigClass
from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.webapi import WebAPI
from argo_connectors.parse.flat_downtimes import ParseDowntimes
from argo_connectors.tasks.common import write_state, write_downtimes_json as write_json


class TaskCsvDowntimes(object):
    # def __init__(self, loop, logger, connector_name, globopts, webapi_opts,
    #              confcust, custname, feed, current_date,
    #              uidservtype, targetdate, timestamp):
    #     self.event_loop = loop +
    #     self.logger = logger +
    #     self.connector_name = connector_name +
    #     self.globopts = globopts +
    #     self.webapi_opts = webapi_opts +
    #     self.confcust = confcust + 
    #     self.custname = custname +
    #     self.feed = feed +
    #     self.current_date = current_date + 
    #     self.uidservtype = uidservtype +
    #     self.targetdate = targetdate
    #     self.timestamp = timestamp +

    ###########################################################################

    def __init__(self, cust, current_date, timestamp):
        self.custname = cust
        self.current_date = current_date
        self.timestamp = timestamp

        self.config = ConfigClass()
        self.loop = self.config.get_loop()
        asyncio.set_event_loop(self.loop)  
        self.logger = self.config.get_logger()
        self.connector_name = self.config.get_connector_name()
        self.globopts, self.pass_extensions, self.cglob = self.config.get_globopts_n_pass_ext()
        self.confcust = self.config.get_confcust(self.globopts)
        self.webapi_opts = self.config.get_webapi_opts_data(self.confcust, self.custname)
        self.feed = self.config.get_downtime_feed(self.confcust)
        self.uidservtype = self.config.uidservendp_data(self.confcust)
        self.targetdate = self.config.get_targetdate()


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
                        self.globopts['ConnectionRetryRandom'.lower()],
                        int(self.globopts['ConnectionSleepRandomRetryMax'.lower()]),
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
