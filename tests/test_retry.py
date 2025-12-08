import unittest
import mock
import asyncio

from aiohttp import client_exceptions
from aiohttp import http_exceptions

from argo_connectors.config.customer import Customer
from argo_connectors.config.glob import Global
from argo_connectors.exceptions import ConnectorHttpError
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.log import Logger

logger = Logger('test_topofeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


class mockHttpGetEmpty(mock.AsyncMock):
    async def __aenter__(self, *args, **kwargs):
        mock_obj = mock.AsyncMock()
        mock_obj.text.return_value = ''
        return mock_obj

    async def __aexit__(self, *args, **kwargs):
        pass


class mockConnectionProblem(mock.AsyncMock):
    async def __aenter__(self, *args, **kwargs):
        mock_obj = mock.AsyncMock()
        mock_oserror = mock.create_autospec(OSError)
        mock_obj.text.side_effect = client_exceptions.ClientConnectorError('mocked key', mock_oserror)
        return mock_obj

    async def __aexit__(self, *args, **kwargs):
        pass


class mockProtocolProblem(mock.AsyncMock):
    async def __aenter__(self, *args, **kwargs):
        mock_obj = mock.AsyncMock()
        mock_obj.text.side_effect = http_exceptions.HttpBadRequest('mocked bad HTTP request')
        return mock_obj

    async def __aexit__(self, *args, **kwargs):
        pass


class mockHttpAcceptableStatuses(mock.AsyncMock):
    async def __aenter__(self, *args, **kwargs):
        mock_obj = mock.AsyncMock()
        mock_obj.text.return_value = 'mocked response data'
        mock_obj.status.return_value = 202
        return mock_obj

    async def __aexit__(self, *args, **kwargs):
        pass


class mockHttpErroneousStatuses(mock.AsyncMock):
    async def __aenter__(self, *args, **kwargs):
        mock_obj = mock.AsyncMock()
        mock_obj.text.return_value = 'mocked failed response data'
        mock_obj_status = mock.Mock()
        mock_obj.status = mock_obj_status.return_value = 404
        return mock_obj

    async def __aexit__(self, *args, **kwargs):
        pass


class ConnectorsHttpRetry(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        glob = Global('topology-gocdb-connector.py')
        glob.options()['ConnectionSleepRetry'.lower()] = 1
        glob.options()['ConnectionRetry'.lower()] = 3
        _ = Customer('topology-gocdb-connector.py')
        logger = Logger(f'{__name__}.{__class__.__name__}')
        logger.customer = CUSTOMER_NAME

        async def setsession():
            with mock.patch('argo_connectors.io.http.build_ssl_settings'):
                self.session = SessionWithRetry()
        self.loop.run_until_complete(setsession())

    # @unittest.skip("skipping")
    @mock.patch('aiohttp.ClientSession.get', side_effect=mockHttpGetEmpty)
    async def test_ConnectorEmptyRetry(self, mocked_get):
        path = '/url_path'
        with self.assertRaises(ConnectorHttpError) as cm:
            res = await self.session.http_get('{}://{}{}'.format('http', 'localhost', path))
        self.assertTrue(mocked_get.called)
        # defined in connectionretry
        self.assertEqual(mocked_get.call_count, 3)
        self.assertEqual(mocked_get.call_args[0][0], 'http://localhost/url_path')

    # @unittest.skip("skipping")
    @mock.patch('aiohttp.ClientSession.get', side_effect=mockConnectionProblem)
    async def test_ConnectorConnectionRetry(self, mocked_get):
        path = '/url_path'
        with self.assertRaises(ConnectorHttpError) as cm:
            res = await self.session.http_get('{}://{}{}'.format('http', 'localhost', path))
        self.assertTrue(mocked_get.called)
        self.assertEqual(mocked_get.call_count, 3)
        self.assertEqual(mocked_get.call_args[0][0], 'http://localhost/url_path')

    # @unittest.skip("skipping")
    @mock.patch('aiohttp.ClientSession.get', side_effect=mockProtocolProblem)
    async def test_ConnectorProtocolError(self, mocked_protocolerror):
        path = '/url_path'
        with self.assertRaises(ConnectorHttpError) as cm:
            res = await self.session.http_get('{}://{}{}'.format('http', 'localhost', path))
        excep = cm.exception
        self.assertIsInstance(excep, ConnectorHttpError)
        self.assertTrue(mocked_protocolerror.called)
        self.assertEqual(mocked_protocolerror.call_count, 1)

    # @unittest.skip("skipping")
    @mock.patch('aiohttp.ClientSession.get', side_effect=mockHttpAcceptableStatuses)
    async def test_ConnectorHttpAcceptable(self, mocked_httpstatuses):
        path = '/url_path'
        res = await self.session.http_get('{}://{}{}'.format('http', 'localhost', path))
        self.assertTrue(mocked_httpstatuses.called)

    # @unittest.skip("demonstrating skipping")
    @mock.patch('aiohttp.ClientSession.get', side_effect=mockHttpErroneousStatuses)
    async def test_ConnectorHttpErroneous(self, mocked_httperrorstatuses):
        path = '/url_path'
        res = await self.session.http_get('{}://{}{}'.format('http', 'localhost', path))
        self.assertTrue(mocked_httperrorstatuses.called)
        self.assertEqual(mocked_httperrorstatuses.call_count, 1)
        self.assertFalse(mocked_httperrorstatuses.text.called)

    def tearDown(self):
        async def run():
            await self.session.close()
