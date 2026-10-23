import datetime
import os
import json

from argo_connectors.config.customer import get_custconf
from argo_connectors.config.glob import Global
from argo_connectors.utils import module_class_name
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.exceptions import ConnectorHttpError
from argo_connectors.log import Logger


class WebAPI(object):
    methods = {
        'downtimes-combiner.py': 'downtimes',
        'downtimes-csv-connector.py': 'downtimes',
        'downtimes-gocdb-connector.py': 'downtimes',
        'service-types-combiner.py': 'topology',
        'service-types-csv-connector.py': 'topology',
        'service-types-gocdb-connector.py': 'topology',
        'service-types-json-connector.py': 'topology',
        'service-types-lot1sc-connector.py': 'topology',
        'topology-combiner.py': 'topology',
        'topology-csv-connector.py': 'topology',
        'topology-gocdb-connector.py': 'topology',
        'topology-json-connector.py': 'topology',
        'topology-provider-connector.py': 'topology',
        'weights-vapor-connector.py': 'weights',
    }

    def __init__(self, report=None, endpoints_group=None, date=None,
                 combuid=None):
        Customer = get_custconf(combuid)
        self.connector = os.path.basename(Global.caller)
        self.webapi_method = self.methods[self.connector]
        self.host = Customer.webapi_opts.opts['webapihost']
        self.token = Customer.webapi_opts.opts['webapitoken']
        self.headers = {
            'x-api-key': self.token,
            'Accept': 'application/json'
        }
        self.report = report
        self.endpoints_group = endpoints_group
        self.date = date or self._construct_datenow()
        self.session = SessionWithRetry(verbose_ret=True,
                                        handle_session_close=True)

    def _construct_datenow(self):
        d = datetime.datetime.now()

        return d.strftime('%Y-%m-%d')

    def _format_downtimes(self, data):
        formatted = dict()

        formatted['endpoints'] = data

        return formatted

    def _format_weights(self, data):
        formatted = dict()

        if data:
            formatted['weight_type'] = data[0]['type']
            groups = map(
                lambda s: {'name': s['site'], 'value': float(s['weight'])}, data)
        else:
            formatted['weight_type'] = ''
            groups = []
        formatted['name'] = self.report
        formatted['groups'] = list(groups)
        formatted['name'] = self.report
        formatted['group_type'] = self.endpoints_group

        return formatted

    async def _send(self, api, data_send, connector):
        content, headers, status = await self.session.http_post(api,
                                                                data=json.dumps(
                                                                    data_send),
                                                                headers=self.headers)
        if status != 201:
            if (connector.startswith('topology')
                or connector.startswith('downtimes')
                    or connector.startswith('service-types')):
                jsonret = json.loads(content)
                msg = None
                statusmsg = jsonret.get('status', False)
                if statusmsg:
                    msg = jsonret['status']['message']
                else:
                    msg = jsonret['message']
                Logger.warn('%s %s() Customer:%s - HTTP POST %s' % (module_class_name(self),
                                                                       '_send',
                                                                       Logger.customer,
                                                                       msg))
            else:
                errormsg = json.loads(content)
                if 'errors' in errormsg:
                    errormsg = errormsg['errors'][0]['details']
                elif 'status' in errormsg:
                    errormsg = errormsg['status']['message']
                Logger.warn('%s %s() Customer:%s Job:%s - HTTP POST %s' %
                               (module_class_name(self), '_send',
                                Logger.customer, Logger.job,
                                errormsg))

        return status

    async def _get(self, api, jsonret=False):
        content, headers, status = await self.session.http_get(api, headers=self.headers)
        if jsonret:
            return json.loads(content)
        else:
            return content

    async def _delete(self, api, id=None, date=None):
        from urllib.parse import urlparse
        loc = urlparse(api)
        if id is not None:
            loc = '{}://{}{}/{}'.format(loc.scheme, loc.hostname, loc.path, id)
        else:
            loc = '{}://{}{}'.format(loc.scheme, loc.hostname, loc.path)
        if date is not None:
            loc = '{}?date={}'.format(loc, date)
        content, headers, status = await self.session.http_delete(loc, headers=self.headers)
        return status

    async def _put(self, api, data_send, id):
        from urllib.parse import urlparse
        loc = urlparse(api)
        loc = '{}://{}{}/{}?{}'.format(loc.scheme,
                                       loc.hostname, loc.path, id, loc.query)
        content, headers, status = await self.session.http_put(loc,
                                                               data=json.dumps(
                                                                   data_send),
                                                               headers=self.headers)
        return content, status

    async def _update(self, api, data_send):
        content = await self._get(api)
        content = json.loads(content)
        target = list(
            filter(lambda w: w['name'] == data_send['name'], content['data']))
        if len(target) > 1:
            Logger.error('%s %s() Customer:%s Job:%s - HTTP PUT %s' %
                         (module_class_name(self), '_update',
                          Logger.customer, Logger.job,
                          'Name of resource not unique on WEB-API, cannot proceed with update'))
        else:
            id = target[0]['id']
            content, status = await self._put(api, data_send, id)
            if status == 200:
                Logger.info('Succesfully updated (HTTP PUT) resource')
            else:
                Logger.error('%s %s() Customer:%s Job:%s - HTTP PUT %s' %
                             (module_class_name(self), '_update',
                              Logger.customer, Logger.job,
                              content))
                raise ConnectorHttpError()

    async def _delete_and_resend(self, api, data_send, topo_component, downtimes_component):
        id = None
        content = await self._get(api)
        if not topo_component and not downtimes_component:
            id = content['data'][0]['id']
        status = await self._delete(api, id, self.date)
        if status == 200:
            await self._send(api, data_send, self.connector)
            Logger.info('Succesfully deleted and created new resource')
        else:
            raise ConnectorHttpError()

    async def get(self, api_path, jsonret):
        if api_path:
            webapi_url = '{}/{}'.format(self.webapi_method, api_path)
        else:
            webapi_url = self.webapi_method
        if self.date:
            api = 'https://{}/api/v2/{}?date={}'.format(self.host,
                                                        webapi_url,
                                                        self.date)
        else:
            api = 'https://{}/api/v2/{}'.format(self.host, self.webapi_method)

        try:
            content = await self._get(api, jsonret)
            Logger.info('Data succesfully fetched from WEB-API')
            return content

        except ConnectorHttpError:
            Logger.error('Failed data fetch from WEB-API')

        finally:
            await self.session.close()

    async def send(self, data, topo_component=None, downtimes_component=None):
        if topo_component:
            # /topology/groups, /topology/endpoints
            webapi_url = '{}/{}'.format(self.webapi_method, topo_component)
        else:
            webapi_url = self.webapi_method

        if self.date:
            api = 'https://{}/api/v2/{}?date={}'.format(self.host,
                                                        webapi_url,
                                                        self.date)
        else:
            api = 'https://{}/api/v2/{}'.format(self.host, webapi_url)

        if topo_component:
            data_send = data
        else:
            data_send = dict()

        if self.connector.startswith('downtimes'):
            data_send = self._format_downtimes(data)

        if self.connector.startswith('weights'):
            data_send = self._format_weights(data)

        try:
            status = await self._send(api, data_send, self.connector)

            # delete resource on WEB-API and resend
            if status == 409 and topo_component or downtimes_component:
                await self._delete_and_resend(api, data_send, topo_component, downtimes_component)
            elif status == 409:
                await self._update(api, data_send)

        except ConnectorHttpError:
            Logger.error('Failed sent of data to WEB-API')
