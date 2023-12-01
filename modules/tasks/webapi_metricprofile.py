import os
import asyncio

from argo_connectors.singleton_config import ConfigClass
from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.tasks.common import write_weights_metricprofile_state as write_state, write_metricprofile_json as write_json
from argo_connectors.parse.webapi_metricprofile import ParseMetricProfiles

API_PATH = '/api/v2/metric_profiles'


class TaskWebApiMetricProfile(object):
    def __init__(self, cust):
        self.config = ConfigClass()
        self.cust = cust
        self.args = self.config.parse_args()
        self.config = ConfigClass()
        self.loop = self.config.get_loop()
        asyncio.set_event_loop(self.loop)
        self.logger = self.config.get_logger()
        self.connector_name = self.config.get_connector_name()
        self.cglob = self.config.get_cglob(self.args)
        self.globopts= self.config.get_globopts(self.cglob)
        self.confcust = self.config.get_confcust(self.globopts, self.args)
        self.fixed_date = self.config.get_fixed_date(self.args) 

    async def fetch_data(self, host, token):
        session = SessionWithRetry(self.logger,
                                   os.path.basename(self.connector_name),
                                   self.globopts, token=token)
        res = await session.http_get('{}://{}{}'.format('https', host, API_PATH))
        return res

    def parse_source(self, res, profiles):
        metric_profiles = ParseMetricProfiles(self.logger, res, profiles).get_data()
        return metric_profiles

    async def run(self):
        for job in self.confcust.get_jobs(self.cust):
            self.logger.customer = self.confcust.get_custname(self.cust)
            self.logger.job = job

            profiles = self.confcust.get_profiles(job)
            webapi_custopts = self.confcust.get_webapiopts(self.cust)
            webapi_opts = self.cglob.merge_opts(webapi_custopts, 'webapi')
            webapi_complete, missopt = self.cglob.is_complete(webapi_opts, 'webapi')

            if not webapi_complete:
                self.logger.error('Customer:%s Job:%s %s options incomplete, missing %s' % (self.logger.customer, self.logger.job, 'webapi', ' '.join(missopt)))
                continue

            try:
                res = await self.fetch_data(webapi_opts['webapihost'], webapi_opts['webapitoken'])

                fetched_profiles = self.parse_source(res, profiles)

                await write_state(self.connector_name, self.globopts, self.cust, job, self.confcust, self.fixed_date, True)

                if eval(self.globopts['GeneralWriteJson'.lower()]):
                    write_json(self.logger, self.globopts, self.cust, job, self.confcust, self.fixed_date, fetched_profiles)

                self.logger.info('Customer:' + self.logger.customer + ' Job:' + job + ' Profiles:%s Tuples:%d' % (', '.join(profiles), len(fetched_profiles)))

            except (ConnectorHttpError, KeyboardInterrupt, ConnectorParseError) as exc:
                self.logger.error(repr(exc))
                await write_state(self.connector_name, self.globopts, self.cust, job, self.confcust, self.fixed_date, False)
