import asyncio
from urllib.parse import urlparse

from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.webapi import WebAPI
from argo_connectors.parse.agora_topology import ParseAgoraTopo
from argo_connectors.tasks.common import write_topo_json as write_json, write_state
from argo_connectors.exceptions import ConnectorError, ConnectorHttpError


def contains_exception(list):
    for a in list:
        if isinstance(a, Exception):
            return (True, a)

    return (False, None)


class TaskProviderTopology(object):
    def __init__(self, loop, logger, connector_name, globopts, webapi_opts,
                 confcust, uidservendp, fetchtype, fixed_date):
        self.loop = loop
        self.logger = logger
        self.connector_name = connector_name
        self.globopts = globopts
        self.webapi_opts = webapi_opts
        self.confcust = confcust
        self.uidservendp = uidservendp
        self.fixed_date = fixed_date
        self.fetchtype = fetchtype


    def parse_source_topo(self, resources, providers):
        topo = ParseAgoraTopo(self.logger, providers, resources, self.uidservendp)

        return topo.get_group_groups(), topo.get_group_endpoints()


    async def send_webapi(self, webapi_opts, data, topotype, fixed_date=None):
        webapi = WebAPI(self.connector_name, webapi_opts['webapihost'],
                        webapi_opts['webapitoken'], self.logger,
                        int(self.globopts['ConnectionRetry'.lower()]),
                        int(self.globopts['ConnectionTimeout'.lower()]),
                        int(self.globopts['ConnectionSleepRetry'.lower()]),
                        self.globopts['ConnectionRetryRandom'.lower()],
                        int(self.globopts['ConnectionSleepRandomRetryMax'.lower()]),
                        date=fixed_date)

        await webapi.send(data, topotype)


    async def fetch_data(self, feed):
        remote_topo = urlparse(feed)
        session = SessionWithRetry(self.logger, self.logger.customer, self.globopts, handle_session_close=True)
        headers = {
            "Accept": "application/json",
        }

        try:
            res = await session.http_get('{}://{}{}'.format(remote_topo.scheme,
                                                            remote_topo.netloc,
                                                            remote_topo.path),
                                                            headers=headers)

            await session.close()
            return res

        except ConnectorHttpError as exc:
            await session.close()
            raise exc


    async def run(self):
        topofeedproviders = self.confcust.get_topofeedservicegroups()
        topofeedresources = self.confcust.get_topofeedendpoints()

        coros = [
            self.fetch_data(topofeedresources),
            self.fetch_data(topofeedproviders),
        ]

        # fetch topology data concurrently in coroutines
        fetched_data = await asyncio.gather(*coros, return_exceptions=True)

        exc_raised, exc = contains_exception(fetched_data)
        if exc_raised:
            raise ConnectorError(repr(exc))

        fetched_resources, fetched_providers = fetched_data
        if fetched_resources and fetched_providers:
            group_providers, group_resources = self.parse_source_topo(fetched_resources, fetched_providers)

            await write_state(self.connector_name, self.globopts, self.confcust, self.fixed_date, True)

            numgg = len(group_providers)
            numge = len(group_resources)

            # send concurrently to WEB-API in coroutines
            if eval(self.globopts['GeneralPublishWebAPI'.lower()]):
                await asyncio.gather(
                        self.send_webapi(self.webapi_opts, group_resources, 'endpoints', self.fixed_date),
                        self.send_webapi(self.webapi_opts, group_providers, 'groups', self.fixed_date),
                        loop=self.loop
                )

            if eval(self.globopts['GeneralWriteJson'.lower()]):
                write_json(self.logger, self.globopts, self.confcust, group_providers, group_resources, self.fixed_date)

            self.logger.info('Customer:' + self.logger.customer + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (self.fetchtype, numgg))
