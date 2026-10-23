import asyncio
import json
import os

from collections.abc import Callable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from argo_connectors.config.customer import get_custconf
from argo_connectors.config.glob import Global
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.webapi import WebAPI
from argo_connectors.log import Logger
from argo_connectors.mesh.contacts import attach_contacts_topodata
from argo_connectors.mesh.topotags import attach_tags
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.parse.provider_contacts import ParseResourcesContacts
from argo_connectors.parse.provider_topology import ParseTopo, ParseExtensions, buildmap_id2groupname
from argo_connectors.tasks.common import write_topo_json as write_json, write_state
from argo_connectors.utils import module_class_name, has_exception


PROVIDER_TOKEN = 'var/spool/provider_token.json'


class find_next_paging_cursor_count(ParseHelpers, Callable):
    def __init__(self, res):
        self.res = res

    def __call__(self):
        try:
            return self._parse()
        except (ConnectorParseError, KeyError) as exc:
            Logger.error(repr(exc))
            Logger.error("Tried to parse (512 chars): %.512s" % ''.join(self.res.replace('\r\n', '').replace('\n', '')))
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


def build_feed_url(feed_parts, params=None):
    query = dict(parse_qsl(feed_parts.query, keep_blank_values=True))
    if params:
        query.update(params)

    return urlunparse((feed_parts.scheme,
                       feed_parts.netloc,
                       feed_parts.path,
                       '',
                       urlencode(query),
                       ''))


class TaskProviderTopology:
    def __init__(self, fixed_date, combuid=None):
        self.connector_name = Global.caller
        self.globopts = Global.options()
        self.Customer = get_custconf(combuid)
        self.paginated = self.Customer.opt('TopoFeedPaging')
        self.uidservendp = self.Customer.opt('TopoUIDServiceEndpoints')
        self.fixed_date = fixed_date
        self.fetchtype = self.Customer.get_topofetchtype()[0]
        self.combuid = combuid
        self.tags_ge = self.Customer.opt('TopoTagServiceEndpoints')
        self.tags_gg = self.Customer.opt('TopoTagServiceGroups') + self.Customer.opt('TopoTagSites')

    def parse_source_extensions(self, extensions, groupnames):
        resources_extended = ParseExtensions(extensions, groupnames, self.combuid)

        return resources_extended.get_extensions()

    def parse_source_topo(self, resources, providers):
        topo = ParseTopo(providers, resources, self.combuid)

        return topo.get_group_groups(), topo.get_group_endpoints()

    def store_refresh_token(self, prevt, newt):
        token_file = f"{os.environ['VIRTUAL_ENV']}/{PROVIDER_TOKEN}"
        with open(token_file, 'w') as fp:
            fp.writelines(json.dumps({"previous": prevt, "next": newt}))

    def read_file_token(self):
        token_file = f"{os.environ['VIRTUAL_ENV']}/{PROVIDER_TOKEN}"

        if os.path.exists(token_file):
            with open(token_file, 'r') as fp:
                file_content = json.loads(fp.read())

            return file_content.get('next', None)

        else:
            return None

    async def fetch_data(self, feed, access_token):
        remote_topo = urlparse(feed)
        session = SessionWithRetry(handle_session_close=True)

        if access_token:
            headers = {
                "Accept": "application/json",
                "Authorization": "Bearer {0}".format(access_token)
            }
        else:
            headers = {
                "Accept": "application/json",
            }

        try:
            res = await session.http_get(build_feed_url(remote_topo), headers=headers)

        except ConnectorHttpError as exc:
            await session.close()
            raise exc

        if self.paginated:
            try:
                next_cursor = find_next_paging_cursor_count(res)
                total, from_index, to_index = next_cursor()
                fetched_results = filter_out_results(res)
                num = to_index - from_index
                from_index = to_index

                while to_index != total:
                    res = await \
                        session.http_get(build_feed_url(remote_topo, {
                            'from': from_index,
                            'quantity': num
                        }), headers=headers)
                    fetched_results = fetched_results + filter_out_results(res)
                    next_cursor = find_next_paging_cursor_count(res)
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
                next_cursor = find_next_paging_cursor_count(res)
                total, from_index, to_index = next_cursor()
                num = total
                from_index = 0

                res = await \
                    session.http_get(build_feed_url(remote_topo, {
                        'from': from_index,
                        'quantity': num
                    }), headers=headers)
                await session.close()
                return res

            except ConnectorParseError as exc:
                await session.close()
                raise exc

    async def token_fetch(self, oidcclientid, oidctoken, oidcapi):
        token_endpoint = urlparse(oidcapi)
        session = SessionWithRetry(handle_session_close=True)

        data = 'grant_type=refresh_token&refresh_token={0}'.format(oidctoken)
        data += '&client_id={0}&scope=openid%20email%20profile'.format(oidcclientid)

        try:
            res = await \
                session.http_post('{}://{}{}'.format(token_endpoint.scheme,
                                                     token_endpoint.netloc,
                                                     token_endpoint.path), data,
                                  headers={'content-type':
                                           'application/x-www-form-urlencoded'})

        except ConnectorHttpError as exc:
            raise exc

        finally:
            await session.close()

        try:
            access_token = json.loads(res).get('access_token', None)
            if not access_token:
                msg = "Could not extract OIDC Access token: {}".format(repr(res))
                raise ConnectorParseError(msg)

        except (json.decoder.JSONDecodeError, TypeError) as exc:
            msg = "Could not extract OIDC Access token: {}".format(repr(exc))
            raise ConnectorParseError(msg)

        try:
            refresh_token = json.loads(res).get('refresh_token', None)
            if not refresh_token:
                msg = "Could not extract OIDC Refresh token: {}".format(repr(res))
                raise ConnectorParseError(msg)

        except (json.decoder.JSONDecodeError, TypeError) as exc:
            msg = "Could not extract OIDC Refresh token: {}".format(repr(exc))
            raise ConnectorParseError(msg)

        return access_token, refresh_token

    async def run(self):
        topofeedextensions = self.Customer.opt('TopoFeedServiceEndpointsExtensions') or self.Customer.opt('TopoFeedEndpointsExtensions')
        topofeedproviders = self.Customer.opt('TopoFeedServiceGroups')
        topofeedresources = self.Customer.opt('TopoFeedServiceEndpoints') or self.Customer.opt('TopoFeedEndpoints')
        oidctoken = self.Customer.opt('OIDCRefreshToken')
        oidctokenapi = self.Customer.opt('OIDCTokenEndpoint')
        oidcclientid = self.Customer.opt('OIDCClientId')

        access_token = None

        if oidctoken and oidctokenapi:
            token_file = self.read_file_token()
            if token_file:
                oidctoken = token_file

            if oidctoken and oidctokenapi:
                access_token, refresh_token = await self.token_fetch(oidcclientid, oidctoken, oidctokenapi)
            else:
                raise ConnectorError('OIDC token missing')

            if refresh_token:
                self.store_refresh_token(oidctoken, refresh_token)

        coros = [
            self.fetch_data(topofeedresources, access_token),
            self.fetch_data(topofeedproviders, access_token),
        ]
        if topofeedextensions:
            coros.append(self.fetch_data(topofeedextensions, access_token))

        # fetch topology data concurrently in coroutines
        fetched_data = await asyncio.gather(*coros, return_exceptions=True)

        exc_raised, exc = has_exception(fetched_data)
        if exc_raised:
            raise ConnectorError(repr(exc))

        if topofeedextensions:
            fetched_resources, fetched_providers, fetched_extensions = fetched_data
        else:
            fetched_resources, fetched_providers = fetched_data

        if fetched_resources and fetched_providers:
            group_groups, group_endpoints = self.parse_source_topo(fetched_resources, fetched_providers)
            endpoints_contacts = ParseResourcesContacts(fetched_resources).get_contacts()

            if topofeedextensions:
                group_endpoints_extended = self.parse_source_extensions(
                    fetched_extensions, buildmap_id2groupname(group_endpoints)
                )
                group_endpoints = group_endpoints + group_endpoints_extended

            attach_contacts_topodata(endpoints_contacts, group_endpoints)

            if not self.combuid:
                await write_state(self.fixed_date, True)

            if self.tags_ge or self.tags_gg:
                group_groups, group_endpoints = await attach_tags(group_groups,
                                                                  group_endpoints,
                                                                  self.tags_gg,
                                                                  self.tags_ge)
            numge = len(group_endpoints)
            numgg = len(group_groups)

            if not self.combuid:
                # send concurrently to WEB-API in coroutines
                if self.globopts['GeneralPublishWebAPI'.lower()]:
                    webapi = WebAPI(date=self.fixed_date, combuid=self.combuid)
                    await asyncio.gather(
                        webapi.send(group_groups, 'groups'),
                        webapi.send(group_endpoints, 'endpoints')
                    )
                    await webapi.session.close()

                if self.globopts['GeneralWriteJson'.lower()]:
                    write_json(group_groups, group_endpoints,
                               self.fixed_date)

            if not self.combuid:
                Logger.info('Customer:' + Logger.customer + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (self.fetchtype, numgg))
            else:
                Logger.info(module_class_name(self) + ' ID:' + self.combuid + ' Customer:' + Logger.customer + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (self.fetchtype, numgg))

                return group_groups, group_endpoints
