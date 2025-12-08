import os

from urllib.parse import urlparse

from argo_connectors.config.glob import Global
from argo_connectors.config.customer import get_custconf
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.parse.gocdb_downtimes import ParseDowntimes
from argo_connectors.io.webapi import WebAPI
from argo_connectors.tasks.common import write_state, write_downtimes_json as write_json
from argo_connectors.utils import module_class_name
from argo_connectors.log import Logger


class TaskGocdbDowntimes(object):
    def __init__(self, start, end, targetdate, timestamp,
                 combuid=None):
        self.globopts = Global.options()
        self.connector_name = Global.caller
        self.Customer = get_custconf(combuid)
        self.globopts = Global.options()
        self.auth_opts = self.Customer.auth_opts.opts
        self.custname = self.Customer.get_custname()
        downtime_feed = self.Customer.opt('DowntimesFeed')
        toposcope = self.Customer.opt('TopoScope')
        if toposcope and '&scope=' not in toposcope:
            downtime_feed += '&scope={}'.format(toposcope)
        elif toposcope and '&scope=' in toposcope:
            downtime_feed += toposcope
        self.feed = downtime_feed
        self.start = start
        self.end = end
        self.targetdate = targetdate
        self.timestamp = timestamp
        self.combuid = combuid

    async def fetch_data(self):
        feed_parts = urlparse(self.feed)
        start_fmt = self.start.strftime("%Y-%m-%d")
        end_fmt = self.end.strftime("%Y-%m-%d")
        session = SessionWithRetry(custauth=self.auth_opts)
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
        gocdb = ParseDowntimes(res, self.start, self.end, self.combuid)
        return gocdb.get_data()

    async def run(self):
        # we don't have multiple tenant definitions in one
        # customer file so we can safely assume one tenant/customer
        write_empty = self.Customer.send_empty(self.connector_name)
        if not write_empty:
            res = await self.fetch_data()
            dts = self.parse_source(res)
        else:
            dts = []

        if not self.combuid:
            await write_state(self.timestamp, True)

        if not self.combuid:
            if self.globopts['GeneralPublishWebAPI'.lower()]:
                webapi = WebAPI(date=self.targetdate, combuid=self.combuid)
                await webapi.send(dts, downtimes_component=True)
                await webapi.session.close()

            if self.globopts['GeneralWriteJson'.lower()]:
                write_json(dts, self.timestamp)

        if dts or write_empty:
            cust = list(self.Customer.get_customers())[0]
            if not self.combuid:
                Logger.info('Customer:%s Fetched Date:%s Endpoints:%d' %
                                 (self.Customer.get_custname(cust),
                                  self.targetdate, len(dts)))
            else:
                Logger.info(module_class_name(self) + ' ID:' + self.combuid + ' Customer:%s Fetched Date:%s Endpoints:%d' %
                                 (self.Customer.get_custname(cust),
                                  self.targetdate, len(dts)))
                return dts
