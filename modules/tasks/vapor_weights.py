import os

from urllib.parse import urlparse

from argo_connectors.config.customer import Customer
from argo_connectors.config.glob import Global
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.webapi import WebAPI
from argo_connectors.parse.vapor import ParseWeights
from argo_connectors.tasks.common import write_weights_metricprofile_state as write_state, write_weights_json as write_json
from argo_connectors.log import Logger


class TaskVaporWeights(object):
    def __init__(self, jobcust, fixed_date, feed=None):
        self.connector_name = Global.caller
        self.globopts = Global.options()
        self.feed = feed or Customer.opt('Vaporpi')
        self.jobcust = jobcust
        self.fixed_date = fixed_date

    async def fetch_data(self):
        feed_parts = urlparse(self.feed)
        session = SessionWithRetry()
        res = await session.http_get('{}://{}{}'.format(feed_parts.scheme,
                                                        feed_parts.netloc,
                                                        feed_parts.path))
        return res

    def parse_source(self, res):
        weights = ParseWeights(res).get_data()
        return weights

    async def run(self):
        for job, cust in self.jobcust:
            Logger.customer = Customer.get_custname(cust)
            Logger.job = job

            write_empty = Customer.send_empty(self.connector_name, cust)

            if write_empty:
                weights = []
            else:
                res = await self.fetch_data()
                weights = self.parse_source(res)

            if self.globopts['GeneralPublishWebAPI'.lower()]:
                webapi = WebAPI(report=Customer.get_jobdir(job),
                                endpoints_group='SITES', date=self.fixed_date)
                await webapi.send(weights)
                await webapi.session.close()

            if self.globopts['GeneralWriteJson'.lower()]:
                write_json(cust, job, self.fixed_date, weights)

            await write_state(cust, job, self.fixed_date, True)

        if weights or write_empty:
            custs = set([cust for job, cust in self.jobcust])
            for cust in custs:
                jobs = [job for job, lcust in self.jobcust if cust == lcust]
                Logger.info('Customer:%s Jobs:%s Sites:%d' %
                                 (Customer.get_custname(cust), jobs[0]
                                     if len(jobs) == 1 else
                                     '({0})'.format(','.join(jobs)),
                                     len(weights)))
