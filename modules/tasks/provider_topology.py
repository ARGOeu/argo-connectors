import asyncio
import json

from collections import Callable
from urllib.parse import urlparse

from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.webapi import WebAPI
from argo_connectors.mesh.contacts import attach_contacts_topodata
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.parse.provider_contacts import ParseResourcesContacts
from argo_connectors.parse.provider_topology import ParseTopo, ParseExtensions, buildmap_id2groupname
from argo_connectors.tasks.common import write_topo_json as write_json, write_state
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError


def contains_exception(list):
    for a in list:
        if isinstance(a, Exception):
            return (True, a)

    return (False, None)


class find_next_paging_cursor_count(ParseHelpers, Callable):
    def __init__(self, logger, res):
        self.res = res
        self.logger = logger

    def __call__(self):
        try:
            return self._parse()
        except (ConnectorParseError, KeyError) as exc:
            self.logger.error(repr(exc))
            self.logger.error("Tried to parse (512 chars): %.512s" % ''.join(self.res.replace('\r\n', '').replace('\n', '')))
            raise ConnectorParseError(exc)

    def _parse(self):
        cursor, count = None, None

        doc = self.parse_json(self.res)
        total = doc['total']
        from_index = doc['from']
        to_index = doc['to']
 
        return total, from_index, to_index


def filter_out_results(data):
    json_data = json.loads(data)['results']
    return json_data


def join_resources(left, right):
    """
        Join default and extras resources leaving out duplicate if found from
        default.
    """
    data_left = json.loads(left)['results']
    data_right = json.loads(right)['results']
    keys = [resource['id'] for resource in data_right]
    new_def = []
    for resource_def in data_left:
        if resource_def['id'] in keys:
            continue
        new_def.append(resource_def)

    return json.dumps({
        'results': new_def + data_right
    })


class TaskProviderTopology(object):
    def __init__(self, loop, logger, connector_name, globopts, webapi_opts,
                 confcust, topofeedpaging, uidservendp, fetchtype, fixed_date):
        self.loop = loop
        self.logger = logger
        self.connector_name = connector_name
        self.globopts = globopts
        self.webapi_opts = webapi_opts
        self.confcust = confcust
        self.topofeedpaging = topofeedpaging
        self.uidservendp = uidservendp
        self.fixed_date = fixed_date
        self.fetchtype = fetchtype

    def parse_source_extensions(self, extensions, groupnames):
        resources_extended = ParseExtensions(self.logger, extensions, groupnames, self.uidservendp, self.logger.customer)

        return resources_extended.get_extensions()
    
    def parse_source_topo(self, resources, providers):
        topo = ParseTopo(self.logger, providers, resources, self.uidservendp, self.logger.customer)

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

    async def fetch_data(self, feed, access_token, paginated):
        fetched_data = list()
        remote_topo = urlparse(feed)
        session = SessionWithRetry(self.logger, self.logger.customer, self.globopts, handle_session_close=True)

        headers = {
            "Accept": "application/json",
            "Authorization": "Bearer {0}".format(access_token)
        }

        try:
            res = await session.http_get('{}://{}{}'.format(remote_topo.scheme,
                                                            remote_topo.netloc,
                                                            remote_topo.path),
                                                            headers=headers)

        except ConnectorHttpError as exc:
            await session.close()
            raise exc

        if paginated:
            try:
                next_cursor = find_next_paging_cursor_count(self.logger, res)
                total, from_index, to_index = next_cursor()
                fetched_results = filter_out_results(res)
                num = to_index - from_index
                from_index = to_index

                while to_index != total:
                    res = await \
                        session.http_get('{}://{}{}?from={}&quantity={}'.format(remote_topo.scheme,
                                                                                remote_topo.netloc,
                                                                                remote_topo.path,
                                                                                from_index,
                                                                                num),
                                                                                headers=headers)
                    fetched_results = fetched_results + filter_out_results(res)
                    next_cursor = find_next_paging_cursor_count(self.logger, res)
                    total, from_index, to_index = next_cursor()
                    num = to_index - from_index
                    from_index = to_index

                await session.close()
                return dict(results=fetched_results)

            except ConnectorParseError as exc:
                await session.close()
                raise exc

        else:
            try:
                next_cursor = find_next_paging_cursor_count(self.logger, res)
                total, from_index, to_index = next_cursor()
                num = total
                from_index = 0

                res = await \
                    session.http_get('{}://{}{}?from={}&quantity={}'.format(remote_topo.scheme,
                                                                            remote_topo.netloc,
                                                                            remote_topo.path,
                                                                            from_index,
                                                                            num),
                                                                            headers=headers)           
                await session.close()
                return res

            except ConnectorParseError as exc:
                await session.close()
                raise exc

    async def token_fetch(self, oidcclientid, oidctoken, oidcapi):
        token_endpoint = urlparse(oidcapi)
        session = SessionWithRetry(self.logger, self.logger.customer, self.globopts, handle_session_close=True)

        data = 'grant_type=refresh_token&refresh_token={0}'.format(oidctoken)
        data += '&client_id={0}&scope=openid%20email%20profile'.format(oidcclientid)

        try:
            res = await session.http_post('{}://{}{}'.format(token_endpoint.scheme,
                                                            token_endpoint.netloc,
                                                            token_endpoint.path), data,
                                        headers={
                                            'content-type': 'application/x-www-form-urlencoded'
                                        })

        except ConnectorHttpError as exc:
            raise exc

        finally:
            await session.close()

        try:
            access_token = json.loads(res).get('access_token', None)
        except (json.decoder.JSONDecodeError, TypeError) as exc:
            msg = "Could not extract OIDC Access token: {}".format(repr(exc))
            raise ConnectorParseError(msg)

        return access_token

    async def run(self):
        topofeedextensions = self.confcust.get_topofeedendpointsextensions()
        topofeedproviders = self.confcust.get_topofeedservicegroups()
        topofeedresources = self.confcust.get_topofeedendpoints()
        oidctoken = self.confcust.get_oidctoken()
        oidctokenapi = self.confcust.get_oidctokenapi()
        oidcclientid = self.confcust.get_oidcclientid()
        topofeedresources = self.confcust.get_topofeedendpoints()

        if oidctoken and oidctokenapi:
            access_token = await self.token_fetch(oidcclientid, oidctoken, oidctokenapi)
        else:
            raise ConnectorError('OIDC token missing')

        coros = [
            self.fetch_data(topofeedresources, access_token, self.topofeedpaging),
            self.fetch_data(topofeedproviders, access_token, self.topofeedpaging),
        ]
        if topofeedextensions:
            coros.append(self.fetch_data(topofeedextensions, access_token, self.topofeedpaging))

        # fetch topology data concurrently in coroutines
        fetched_data = await asyncio.gather(*coros, return_exceptions=True)

        exc_raised, exc = contains_exception(fetched_data)
        if exc_raised:
            raise ConnectorError(repr(exc))

        if topofeedextensions:
            fetched_resources, fetched_providers, fetched_extensions = fetched_data
        else:
            fetched_resources, fetched_providers = fetched_data

        if fetched_resources and fetched_providers:
            group_groups, group_endpoints = self.parse_source_topo(fetched_resources, fetched_providers)
            endpoints_contacts = ParseResourcesContacts(self.logger, fetched_resources).get_contacts()

            if topofeedextensions:
                group_endpoints_extended = self.parse_source_extensions(
                    fetched_extensions, buildmap_id2groupname(group_endpoints)
                )
                group_endpoints = group_endpoints + group_endpoints_extended

            attach_contacts_topodata(self.logger, endpoints_contacts, group_endpoints)

            await write_state(self.connector_name, self.globopts, self.confcust, self.fixed_date, True)

            numge = len(group_endpoints)
            numgg = len(group_groups)

            # send concurrently to WEB-API in coroutines
            if eval(self.globopts['GeneralPublishWebAPI'.lower()]):
                await asyncio.gather(
                        self.send_webapi(self.webapi_opts, group_groups, 'groups', self.fixed_date),
                        self.send_webapi(self.webapi_opts, group_endpoints,'endpoints', self.fixed_date),
                        loop=self.loop
                )

            if eval(self.globopts['GeneralWriteJson'.lower()]):
                write_json(self.logger, self.globopts, self.confcust, group_groups, group_endpoints, self.fixed_date)

            self.logger.info('Customer:' + self.logger.customer + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (self.fetchtype, numgg))
