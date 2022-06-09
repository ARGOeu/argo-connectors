import json
import asyncio

from urllib.parse import urlparse

from argo_connectors.io.http import SessionWithRetry
from argo_connectors.parse.flat_topology import ParseFlatEndpoints
from argo_connectors.parse.flat_contacts import ParseContacts
from argo_connectors.io.webapi import WebAPI
from argo_connectors.mesh.contacts import attach_contacts_topodata
from argo_connectors.tasks.common import write_state, write_topo_avro as write_avro


class TaskFlatTopology(object):
    def __init__(self, loop, logger, connector_name, globopts, webapi_opts,
                 confcust, custname, topofeed, fetchtype, fixed_date,
                 uidservendp, is_csv=False):
        self.event_loop = loop
        self.logger = logger
        self.connector_name = connector_name
        self.globopts = globopts
        self.webapi_opts = webapi_opts
        self.confcust = confcust
        self.custname = custname
        self.topofeed = topofeed
        self.fetchtype = fetchtype
        self.fixed_date = fixed_date
        self.uidservendp = uidservendp
        self.is_csv = is_csv

    def _is_feed(self, feed):
        data = urlparse(feed)

        if not data.netloc:
            return False
        else:
            return True

    async def fetch_data(self):
        remote_topo = urlparse(self.topofeed)
        session = SessionWithRetry(self.logger, self.custname, self.globopts)
        if remote_topo.query:
            res = await \
            session.http_get('{}://{}{}?{}'.format(remote_topo.scheme,
                                                   remote_topo.netloc,
                                                   remote_topo.path,
                                                            remote_topo.query))
        else:
            res = await session.http_get('{}://{}{}'.format(remote_topo.scheme,
                                                            remote_topo.netloc,
                                                            remote_topo.path))
        return res

    def parse_source_topo(self, res):
        topo = ParseFlatEndpoints(self.logger, res, self.custname,
                                  self.uidservendp, self.fetchtype,
                                  self.is_csv, scope=self.custname)
        group_groups = topo.get_groupgroups()
        group_endpoints = topo.get_groupendpoints()

        return group_groups, group_endpoints

    async def send_webapi(self, data, topotype):
        webapi = WebAPI(self.connector_name, self.webapi_opts['webapihost'],
                        self.webapi_opts['webapitoken'], self.logger,
                        int(self.globopts['ConnectionRetry'.lower()]),
                        int(self.globopts['ConnectionTimeout'.lower()]),
                        int(self.globopts['ConnectionSleepRetry'.lower()]),
                        date=self.fixed_date)
        await webapi.send(data, topotype)

    async def run(self):
        if self._is_feed(self.topofeed):
            res = await self.fetch_data()
            group_groups, group_endpoints = self.parse_source_topo(res)
            contacts = ParseContacts(self.logger, res, self.uidservendp, self.is_csv).get_contacts()
            attach_contacts_topodata(self.logger, contacts, group_endpoints)

        elif not self._is_feed(self.topofeed) and not self.is_csv:
            try:
                with open(self.topofeed) as fp:
                    js = json.load(fp)
                    group_groups, group_endpoints = self.parse_source_topo(js)
            except IOError as exc:
                self.logger.error('Customer:%s : Problem opening %s - %s' % (self.logger.customer, self.topofeed, repr(exc)))

        await write_state(self.connector_name, self.globopts, self.confcust, self.fixed_date, True)

        numge = len(group_endpoints)
        numgg = len(group_groups)

        # send concurrently to WEB-API in coroutines
        if eval(self.globopts['GeneralPublishWebAPI'.lower()]):
            await asyncio.gather(
                self.send_webapi(group_groups, 'groups'),
                self.send_webapi(group_endpoints,'endpoints')
            )

        if eval(self.globopts['GeneralWriteAvro'.lower()]):
            write_avro(self.logger, self.globopts, self.confcust, group_groups, group_endpoints, self.fixed_date)

        self.logger.info('Customer:' + self.custname + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (self.fetchtype, numgg))
