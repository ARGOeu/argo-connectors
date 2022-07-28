import datetime
import os
import json

from argo_connectors.utils import module_class_name
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.exceptions import ConnectorHttpError


class WebAPI(object):
    methods = {
        'downtimes-csv-connector.py': 'downtimes',
        'downtimes-gocdb-connector.py': 'downtimes',
        'topology-gocdb-connector.py': 'topology',
        'topology-csv-connector.py': 'topology',
        'topology-provider-connector.py': 'topology',
        'topology-json-connector.py': 'topology',
        'weights-vapor-connector.py': 'weights',
        'service-types-gocdb-connector.py': 'topology',
        'service-types-csv-connector.py': 'topology',
        'service-types-json-connector.py': 'topology',
    }

    def __init__(self, connector, host, token, logger, retry,
                 timeout=180, sleepretry=60, report=None, endpoints_group=None,
                 date=None):
        self.connector = os.path.basename(connector)
        self.webapi_method = self.methods[self.connector]
        self.host = host
        self.token = token
        self.headers = {
            'x-api-key': self.token,
            'Accept': 'application/json'
        }
        self.report = report
        self.logger = logger
        self.retry = retry
        self.timeout = timeout
        self.sleepretry = sleepretry
        self.retry_options = {
            'ConnectionRetry'.lower(): retry,
            'ConnectionTimeout'.lower(): timeout,
            'ConnectionSleepRetry'.lower(): sleepretry
        }
        self.endpoints_group = endpoints_group
        self.date = date or self._construct_datenow()
        self.session = SessionWithRetry(self.logger, module_class_name(self),
                                        self.retry_options, verbose_ret=True,
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
            groups = map(lambda s: {'name': s['site'], 'value': float(s['weight'])}, data)
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
                                                                data=json.dumps(data_send),
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
                self.logger.error('%s %s() Customer:%s - HTTP POST %s' % (module_class_name(self),
                                                                          '_send',
                                                                          self.logger.customer,
                                                                          msg))
            else:
                errormsg = json.loads(content)
                if 'errors' in errormsg:
                    errormsg = errormsg['errors'][0]['details']
                elif 'status' in errormsg:
                    errormsg = errormsg['status']['message']
                self.logger.error('%s %s() Customer:%s Job:%s - HTTP POST %s' %
                                  (module_class_name(self), '_send',
                                   self.logger.customer, self.logger.job,
                                   errormsg))
        return status

    async def _get(self, api):
        content, headers, status = await self.session.http_get(api, headers=self.headers)
        return json.loads(content)

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
        loc = '{}://{}{}/{}?{}'.format(loc.scheme, loc.hostname, loc.path, id, loc.query)
        content, headers, status = await self.session.http_put(loc,
                                                               data=json.dumps(data_send),
                                                               headers=self.headers)
        return content, status

    async def _update(self, api, data_send):
        content = await self._get(api)
        target = list(filter(lambda w: w['name'] == data_send['name'], content['data']))
        if len(target) > 1:
            self.logger.error('%s %s() Customer:%s Job:%s - HTTP PUT %s' %
                              (module_class_name(self), '_update',
                               self.logger.customer, self.logger.job,
                               'Name of resource not unique on WEB-API, cannot proceed with update'))
        else:
            id = target[0]['id']
            content, status = await self._put(api, data_send, id)
            if status == 200:
                self.logger.info('Succesfully updated (HTTP PUT) resource')
            else:
                self.logger.error('%s %s() Customer:%s Job:%s - HTTP PUT %s' %
                                  (module_class_name(self), '_update',
                                   self.logger.customer, self.logger.job,
                                   content))

    async def _delete_and_resend(self, api, data_send, topo_component, downtimes_component):
        id = None
        content = await self._get(api)
        if not topo_component and not downtimes_component:
            id = content['data'][0]['id']
        status = await self._delete(api, id, self.date)
        if status == 200:
            await self._send(api, data_send, self.connector)
            self.logger.info('Succesfully deleted and created new resource')

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

            self.logger.info('Data succesfully sent to WEB-API')

        except ConnectorHttpError:
            self.logger.error('Failed sent of data to WEB-API')

        finally:
            await self.session.close()
