import os

from urllib.parse import urlparse

from argo_connectors.io.http import SessionWithRetry
from argo_connectors.parse.gocdb_downtimes import ParseDowntimes
from argo_connectors.io.webapi import WebAPI
from argo_connectors.tasks.common import write_state, write_downtimes_json as write_json


class TaskGocdbDowntimes(object):
    def __init__(self, loop, logger, connector_name, globopts, auth_opts,
                 webapi_opts, confcust, custname, feed, start, end,
                 uidservtype, targetdate, timestamp):
        self.event_loop = loop
        self.logger = logger
        self.connector_name = connector_name
        self.globopts = globopts
        self.auth_opts = auth_opts
        self.webapi_opts = webapi_opts
        self.confcust = confcust
        self.custname = custname
        self.feed = feed
        self.start = start
        self.end = end
        self.uidservtype = uidservtype
        self.targetdate = targetdate
        self.timestamp = timestamp

    async def fetch_data(self):
        feed_parts = urlparse(self.feed)
        start_fmt = self.start.strftime("%Y-%m-%d")
        end_fmt = self.end.strftime("%Y-%m-%d")
        session = SessionWithRetry(self.logger,
                                   os.path.basename(self.connector_name),
                                   self.globopts,
                                   custauth=self.auth_opts)
        if feed_parts.query:
            query_url = \
            '{}://{}{}?{}&windowstart={}&windowend={}'.format(feed_parts.scheme,
                                                              feed_parts.netloc,
                                                              feed_parts.path,
                                                              feed_parts.query,
                                                              start_fmt,
                                                              end_fmt)
        else:
            query_url = \
            '{}://{}{}?windowstart={}&windowend={}'.format(feed_parts.scheme,
                                                           feed_parts.netloc,
                                                           feed_parts.path,
                                                           start_fmt, end_fmt)
        res = await session.http_get(query_url)

        return res

    def parse_source(self, res):
        gocdb = ParseDowntimes(self.logger, res, self.start, self.end,
                               self.uidservtype)
        return gocdb.get_data()

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
        # we don't have multiple tenant definitions in one
        # customer file so we can safely assume one tenant/customer
        write_empty = self.confcust.send_empty(self.connector_name)
        if not write_empty:
            res = await self.fetch_data()
            dts = self.parse_source(res)
        else:
            dts = []

        await write_state(self.connector_name, self.globopts, self.confcust, self.timestamp, True)

        if eval(self.globopts['GeneralPublishWebAPI'.lower()]):
            await self.send_webapi(dts)

        if dts or write_empty:
            cust = list(self.confcust.get_customers())[0]
            self.logger.info('Customer:%s Fetched Date:%s Endpoints:%d' %
                        (self.confcust.get_custname(cust), self.targetdate, len(dts)))

        if eval(self.globopts['GeneralWriteJson'.lower()]):
            write_json(self.logger, self.globopts, self.confcust, dts, self.timestamp)
