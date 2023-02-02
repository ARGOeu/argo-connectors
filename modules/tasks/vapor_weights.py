import os

from urllib.parse import urlparse

from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.webapi import WebAPI
from argo_connectors.parse.vapor import ParseWeights
from argo_connectors.tasks.common import write_weights_metricprofile_state as write_state, write_weights_json as write_json


class TaskVaporWeights(object):
    def __init__(self, loop, logger, connector_name, globopts, confcust, feed,
                 jobcust, cglob, fixed_date):
        self.event_loop = loop
        self.logger = logger
        self.connector_name = connector_name
        self.globopts = globopts
        self.confcust = confcust
        self.feed = feed
        self.jobcust = jobcust
        self.cglob = cglob
        self.fixed_date = fixed_date

    async def fetch_data(self):
        feed_parts = urlparse(self.feed)
        session = SessionWithRetry(self.logger, os.path.basename(
            self.connector_name), self.globopts)
        res = await session.http_get('{}://{}{}'.format(feed_parts.scheme,
                                                        feed_parts.netloc,
                                                        feed_parts.path))
        return res

    def get_webapi_opts(self, cust, job):
        webapi_custopts = self.confcust.get_webapiopts(cust)
        webapi_opts = self.cglob.merge_opts(webapi_custopts, 'webapi')
        webapi_complete, missopt = self.cglob.is_complete(
            webapi_opts, 'webapi')
        if not webapi_complete:
            self.logger.error('Customer:%s Job:%s %s options incomplete, missing %s' % (
                self.logger.customer, job, 'webapi', ' '.join(missopt)))
        return webapi_opts

    def parse_source(self, res):
        weights = ParseWeights(self.logger, res).get_data()
        return weights

    async def send_webapi(self, weights, webapi_opts, job):
        webapi = WebAPI(self.connector_name, webapi_opts['webapihost'],
                        webapi_opts['webapitoken'], self.logger,
                        int(self.globopts['ConnectionRetry'.lower()]),
                        int(self.globopts['ConnectionTimeout'.lower()]),
                        int(self.globopts['ConnectionSleepRetry'.lower()]),
                        report=self.confcust.get_jobdir(job), endpoints_group='SITES',
                        date=self.fixed_date)
        await webapi.send(weights)

    async def run(self):
        for job, cust in self.jobcust:
            self.logger.customer = self.confcust.get_custname(cust)
            self.logger.job = job

            write_empty = self.confcust.send_empty(self.connector_name, cust)

            if write_empty:
                weights = []
            else:
                res = await self.fetch_data()
                weights = self.parse_source(res)

            webapi_opts = self.get_webapi_opts(cust, job)

            if eval(self.globopts['GeneralPublishWebAPI'.lower()]):
                await self.send_webapi(weights, webapi_opts, job)

            if eval(self.globopts['GeneralWriteJson'.lower()]):
                write_json(self.logger, self.globopts, cust, job,
                           self.confcust, self.fixed_date, weights)

            await write_state(self.connector_name, self.globopts, cust, job, self.confcust, self.fixed_date, True)

        if weights or write_empty:
            custs = set([cust for job, cust in self.jobcust])
            for cust in custs:
                jobs = [job for job, lcust in self.jobcust if cust == lcust]
                self.logger.info('Customer:%s Jobs:%s Sites:%d' %
                                 (self.confcust.get_custname(cust), jobs[0]
                                     if len(jobs) == 1 else
                                     '({0})'.format(','.join(jobs)),
                                     len(weights)))
