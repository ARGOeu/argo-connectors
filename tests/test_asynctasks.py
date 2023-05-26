import unittest

import unittest
import asyncio
import datetime

import mock

from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.tasks.flat_downtimes import TaskCsvDowntimes
from argo_connectors.tasks.flat_servicetypes import TaskFlatServiceTypes
from argo_connectors.tasks.gocdb_servicetypes import TaskGocdbServiceTypes
from argo_connectors.tasks.gocdb_topology import TaskGocdbTopology, find_next_paging_cursor_count
from argo_connectors.tasks.provider_topology import TaskProviderTopology
from argo_connectors.tasks.gocdb_downtimes import TaskGocdbDowntimes
from argo_connectors.tasks.vapor_weights import TaskVaporWeights
from argo_connectors.tasks.webapi_metricprofile import TaskWebApiMetricProfile
from argo_connectors.tasks.flat_topology import TaskFlatTopology
from argo_connectors.tasks.agora_topology import AgoraProviderTopology

from argo_connectors.parse.base import ParseHelpers


CUSTOMER_NAME = 'CUSTOMERFOO'


class async_test(object):
    """
    Decorator to create asyncio context for asyncio methods or functions.
    """

    def __init__(self, test_method):
        self.test_method = test_method

    def __call__(self, *args, **kwargs):
        test_obj = args[0]
        test_obj.loop.run_until_complete(self.test_method(*args, **kwargs))


class TopologyGocdb(unittest.TestCase):
    def setUp(self):
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.loop = asyncio.get_event_loop()
        globopts = mock.MagicMock()
        webapiopts = mock.MagicMock()
        authopts = mock.MagicMock()
        bdiiopts = mock.MagicMock()
        bdiiopts.__getitem__.return_value = 'True'
        confcust = mock.Mock()
        topofeedpaging = True
        notification_flag = True
        uidservendp = False
        passext = True
        fixed_date = datetime.datetime.now().strftime('%Y_%m_%d')
        fetchtype = 'ServiceGroups'
        self.topo_gocdb = TaskGocdbTopology(
            self.loop,
            logger,
            'test_asynctasks_topologygocdb',
            'https://gocdb.com/serviceendpoints_api',
            'https://gocdb.com/serviceegroups_api',
            'https://gocdb.com/sites_api',
            globopts,
            authopts,
            webapiopts,
            bdiiopts,
            confcust,
            CUSTOMER_NAME,
            'https://gocdb.com/',
            fetchtype,
            fixed_date,
            uidservendp,
            passext,
            topofeedpaging,
            notification_flag
        )

    @mock.patch.object(ParseHelpers, 'parse_xml')
    @mock.patch('argo_connectors.io.http.build_connection_retry_settings')
    @mock.patch('argo_connectors.io.http.build_ssl_settings')
    @mock.patch('argo_connectors.tasks.gocdb_topology.TaskGocdbTopology.fetch_ldap_data')
    @mock.patch('argo_connectors.tasks.gocdb_topology.SessionWithRetry.http_get')
    @async_test
    async def test_failedNextCursor(self, mock_httpget, mock_fetchldap,
                                    mock_buildsslsettings,
                                    mock_buildconnretry, mock_parsexml):
        mock_httpget.return_value = 'garbled XML data'
        mock_buildsslsettings.return_value = 'SSL settings'
        mock_parsexml.side_effect = [
            ConnectorParseError('failed GOCDB find_next_paging_cursor_count'),
            ConnectorParseError('failed GOCDB find_next_paging_cursor_count'),
            ConnectorParseError('failed GOCDB find_next_paging_cursor_count')
        ]
        mock_buildconnretry.return_value = (1, 2)
        with self.assertRaises(ConnectorError) as cm:
            await self.topo_gocdb.run()
        excep = cm.exception
        self.assertTrue('ConnectorParseError' in excep.msg)
        self.assertTrue('failed GOCDB' in excep.msg)


class TestFindNextPagingCursorCount(unittest.TestCase):
    def setUp(self):
        self.logger = mock.MagicMock()
        with open('tests/sample-topofeedpaging.xml') as tf:
            self.res = tf.read()

    def test_count_n_cursor(self):
        paging = find_next_paging_cursor_count(self.logger, self.res)
        count, cursor = paging()

        self.assertEqual(count, 95)
        self.assertEqual(cursor, '134')


class TopologyProvider(unittest.TestCase):
    def setUp(self):
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.loop = asyncio.get_event_loop()
        globopts = mock.Mock()
        webapiopts = mock.Mock()
        confcust = mock.Mock()
        confcust.get_topofeedservicegroups.return_value = 'http://topo.feed.providers.com'
        confcust.get_topofeedendpoints.return_value = 'http://topo.feed.resources.com'
        confcust.get_oidctoken.return_value = 'oidctoken'
        confcust.get_oidcclientid.return_value = 'clientid'
        confcust.get_oidctokenapi.return_value = 'oidctokenapi'
        topofeedpaging = False
        uidservendp = False
        fixed_date = datetime.datetime.now().strftime('%Y_%m_%d')
        fetchtype = 'ServiceGroups'
        self.topo_provider = TaskProviderTopology(
            self.loop,
            logger,
            'test_asynctasks_topologyprovider',
            globopts,
            webapiopts,
            confcust,
            topofeedpaging,
            uidservendp,
            fixed_date,
            fetchtype
        )

    @mock.patch.object(ParseHelpers, 'parse_json')
    @mock.patch('argo_connectors.tasks.provider_topology.TaskProviderTopology.token_fetch')
    @mock.patch('argo_connectors.io.http.build_connection_retry_settings')
    @mock.patch('argo_connectors.io.http.build_ssl_settings')
    @mock.patch('argo_connectors.tasks.provider_topology.SessionWithRetry.http_post')
    @mock.patch('argo_connectors.tasks.provider_topology.SessionWithRetry.http_get')
    @async_test
    async def test_failedNextCursor(self, mock_httpget, mock_httppost, mock_buildsslsettings,
                                    mock_buildconnretry, mock_tokenfetch, mock_parsejson):
        mock_httpget.return_value = 'garbled JSON data'
        mock_httppost.return_value = 'garbled JSON data'
        mock_buildsslsettings.return_value = 'SSL settings'
        mock_tokenfetch.return_value = 'OIDC token'
        mock_parsejson.side_effect = [
            ConnectorParseError(
                'failed PROVIDER find_next_paging_cursor_count'),
            ConnectorParseError(
                'failed PROVIDER find_next_paging_cursor_count'),
        ]
        mock_buildconnretry.return_value = (1, 2)
        with self.assertRaises(ConnectorError) as cm:
            await self.topo_provider.run()
        excep = cm.exception
        self.assertTrue(type(excep), ConnectorParseError)
        self.assertTrue('failed PROVIDER' in excep.msg)

    @mock.patch('argo_connectors.io.http.build_connection_retry_settings')
    @mock.patch('argo_connectors.io.http.build_ssl_settings')
    @mock.patch('argo_connectors.tasks.provider_topology.SessionWithRetry.http_post')
    @mock.patch.object(TaskProviderTopology, 'fetch_data')
    @async_test
    async def test_failedAccessToken(self, mock_fetchdata, mock_httppost,
                                     mock_buildsslsettings,
                                     mock_buildconnretry):
        mock_fetchdata.return_value = 'OK JSON data'
        mock_httppost.return_value = 'garbled JSON data'
        mock_buildsslsettings.return_value = 'SSL settings'
        mock_buildconnretry.return_value = (1, 2)
        with self.assertRaises(ConnectorError) as cm:
            await self.topo_provider.run()
        excep = cm.exception
        self.assertTrue(type(excep), ConnectorParseError)
        self.assertTrue('JSONDecodeError' in excep.msg)


class ServiceTypesGocdb(unittest.TestCase):
    def setUp(self):
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.loop = asyncio.get_event_loop()
        mocked_globopts = dict(generalpublishwebapi='True')
        globopts = mocked_globopts
        webapiopts = mock.Mock()
        authopts = mock.Mock()
        confcust = mock.Mock()
        custname = CUSTOMER_NAME
        initsync = False
        feed = 'https://service-types.com/api/fetch'
        timestamp = datetime.datetime.now().strftime('%Y_%m_%d')
        self.services_gocdb = TaskGocdbServiceTypes(
            self.loop,
            logger,
            'test_asynctasks_servicetypesgocdb',
            globopts,
            authopts,
            webapiopts,
            confcust,
            custname,
            feed,
            timestamp,
            initsync
        )
        self.maxDiff = None

    @mock.patch('argo_connectors.tasks.gocdb_servicetypes.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate):
        self.services_gocdb.fetch_data = mock.AsyncMock()
        self.services_gocdb.fetch_data.side_effect = ['data_servicetypes']
        self.services_gocdb.fetch_webapi = mock.AsyncMock()
        self.services_gocdb.fetch_webapi.side_effect = [
            'data_webapi_servicetypes']
        self.services_gocdb.send_webapi = mock.AsyncMock()
        self.services_gocdb.parse_source = mock.MagicMock()
        self.services_gocdb.parse_webapi_poem = mock.MagicMock()
        await self.services_gocdb.run()
        self.assertTrue(self.services_gocdb.fetch_webapi.called)
        self.assertTrue(self.services_gocdb.fetch_data.called)
        self.assertTrue(self.services_gocdb.parse_source.called)
        self.services_gocdb.parse_source.assert_called_with(
            'data_servicetypes')
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_servicetypesgocdb')
        self.assertEqual(
            mock_writestate.call_args[0][3], self.services_gocdb.timestamp)
        self.assertTrue(mock_writestate.call_args[0][4])
        self.assertTrue(self.services_gocdb.send_webapi.called)
        self.assertTrue(self.services_gocdb.logger.info.called)

    @mock.patch('argo_connectors.tasks.gocdb_servicetypes.write_state')
    @async_test
    async def test_StepsCombinedServiceTypes(self, mock_writestate):
        self.services_gocdb.fetch_data = mock.AsyncMock()
        self.services_gocdb.fetch_webapi = mock.AsyncMock()
        self.services_gocdb.parse_source = mock.Mock()
        self.services_gocdb.parse_webapi_poem = mock.Mock()
        self.services_gocdb.parse_source.return_value = [
            {
                'name': 'service.type.one',
                'description': 'service description one',
                'tags': ['topology']
            },
            {
                'name': 'service.type.two',
                'description': 'service description two',
                'tags': ['topology']
            },
            {
                'name': 'service.type.three',
                'description': 'service description three',
                'tags': ['topology']
            }
        ]
        self.services_gocdb.parse_webapi_poem.return_value = [
            {
                'name': 'service.type.four',
                'description': 'service description four',
                'tags': ['poem']
            },
            {
                'name': 'service.type.five',
                'description': 'service description five',
                'tags': ['poem']
            }
        ]
        self.services_gocdb.send_webapi = mock.AsyncMock()
        await self.services_gocdb.run()
        self.assertTrue(self.services_gocdb.send_webapi.called)
        self.services_gocdb.send_webapi.assert_called_with([
            {
                'name': 'service.type.five',
                'description': 'service description five',
                'tags': ['poem']
            },
            {
                'name': 'service.type.four',
                'description': 'service description four',
                'tags': ['poem']
            },
            {
                'name': 'service.type.one',
                'description': 'service description one',
                'tags': ['topology']
            },
            {
                'name': 'service.type.three',
                'description': 'service description three',
                'tags': ['topology']
            },
            {
                'name': 'service.type.two',
                'description': 'service description two',
                'tags': ['topology']
            }
        ])

    @mock.patch('argo_connectors.tasks.gocdb_servicetypes.write_state')
    @async_test
    async def test_StepsFailedRun(self, mock_writestate):
        self.services_gocdb.fetch_data = mock.AsyncMock()
        self.services_gocdb.fetch_data.side_effect = [
            ConnectorHttpError('fetch_data failed')]
        self.services_gocdb.send_webapi = mock.AsyncMock()
        self.services_gocdb.parse_source = mock.MagicMock()
        self.services_gocdb.fetch_webapi = mock.AsyncMock()
        self.services_gocdb.fetch_webapi.side_effect = [
            'data_webapi_servicetypes']
        await self.services_gocdb.run()
        self.assertTrue(self.services_gocdb.fetch_data.called)
        self.assertFalse(self.services_gocdb.parse_source.called)
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_servicetypesgocdb')
        self.assertEqual(
            mock_writestate.call_args[0][3], self.services_gocdb.timestamp)
        self.assertFalse(mock_writestate.call_args[0][4])
        self.assertTrue(self.services_gocdb.logger.error.called)
        self.assertEqual(self.services_gocdb.logger.error.call_args[0][0], repr(
            ConnectorError("ConnectorHttpError('fetch_data failed',)")))
        self.assertFalse(self.services_gocdb.send_webapi.called)

        self.services_gocdb.fetch_data = mock.AsyncMock()
        self.services_gocdb.fetch_data.side_effect = ['data_servicetypes']
        self.services_gocdb.send_webapi = mock.AsyncMock()
        self.services_gocdb.parse_source = mock.MagicMock()
        self.services_gocdb.fetch_webapi = mock.AsyncMock()
        self.services_gocdb.fetch_webapi.side_effect = [
            ConnectorHttpError('fetch_webapi_data failed')]
        await self.services_gocdb.run()
        self.assertTrue(self.services_gocdb.fetch_data.called)
        self.assertFalse(self.services_gocdb.parse_source.called)
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_servicetypesgocdb')
        self.assertEqual(
            mock_writestate.call_args[0][3], self.services_gocdb.timestamp)
        self.assertFalse(mock_writestate.call_args[0][4])
        self.assertTrue(self.services_gocdb.logger.error.called)
        self.assertEqual(self.services_gocdb.logger.error.call_args[0][0], repr(
            ConnectorError("ConnectorHttpError('fetch_webapi_data failed',)")))
        self.assertFalse(self.services_gocdb.send_webapi.called)


class ServiceTypesFlat(unittest.TestCase):
    def setUp(self):
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.loop = asyncio.get_event_loop()
        mocked_globopts = dict(generalpublishwebapi='True')
        globopts = mocked_globopts
        webapiopts = mock.Mock()
        authopts = mock.Mock()
        confcust = mock.Mock()
        custname = CUSTOMER_NAME
        feed = 'https://service-types.com/api/fetch'
        timestamp = datetime.datetime.now().strftime('%Y_%m_%d')
        self.services_flat = TaskFlatServiceTypes(
            self.loop,
            logger,
            'test_asynctasks_servicetypesflat',
            globopts,
            authopts,
            webapiopts,
            confcust,
            custname,
            feed,
            timestamp
        )
        self.maxDiff = None

    @mock.patch('argo_connectors.tasks.flat_servicetypes.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate):
        self.services_flat.fetch_data = mock.AsyncMock()
        self.services_flat.fetch_data.side_effect = ['data_servicetypes']
        self.services_flat.send_webapi = mock.AsyncMock()
        self.services_flat.fetch_webapi = mock.AsyncMock()
        self.services_flat.fetch_webapi.side_effect = [
            'data_webapi_servicetypes']
        self.services_flat.send_webapi = mock.AsyncMock()
        self.services_flat.parse_source = mock.MagicMock()
        self.services_flat.parse_webapi_poem = mock.MagicMock()
        await self.services_flat.run()
        self.assertTrue(self.services_flat.fetch_data.called)
        self.assertTrue(self.services_flat.parse_source.called)
        self.services_flat.parse_source.assert_called_with('data_servicetypes')
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_servicetypesflat')
        self.assertEqual(
            mock_writestate.call_args[0][3], self.services_flat.timestamp)
        self.assertTrue(mock_writestate.call_args[0][4])
        self.assertTrue(self.services_flat.send_webapi.called)
        self.assertTrue(self.services_flat.logger.info.called)

    @mock.patch('argo_connectors.tasks.flat_servicetypes.write_state')
    @async_test
    async def test_StepsFailedRun(self, mock_writestate):
        self.services_flat.fetch_data = mock.AsyncMock()
        self.services_flat.fetch_data.side_effect = [
            ConnectorHttpError('fetch_data failed')]
        self.services_flat.send_webapi = mock.AsyncMock()
        self.services_flat.parse_source = mock.MagicMock()
        self.services_flat.fetch_webapi = mock.AsyncMock()
        self.services_flat.fetch_webapi.side_effect = [
            'data_webapi_servicetypes']
        await self.services_flat.run()
        self.assertTrue(self.services_flat.fetch_data.called)
        self.assertFalse(self.services_flat.parse_source.called)
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_servicetypesflat')
        self.assertEqual(
            mock_writestate.call_args[0][3], self.services_flat.timestamp)
        self.assertFalse(mock_writestate.call_args[0][4])
        self.assertTrue(self.services_flat.logger.error.called)
        self.assertTrue(self.services_flat.logger.error.call_args[0][0], repr(
            ConnectorHttpError('fetch_data failed')))
        self.assertFalse(self.services_flat.send_webapi.called)


class DowntimesCsv(unittest.TestCase):
    def setUp(self):
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.loop = asyncio.get_event_loop()
        mocked_globopts = dict(generalpublishwebapi='True',
                               generalwritejson='True',
                               outputdowntimes='downtimes_DATE.json',
                               )
        globopts = mocked_globopts
        webapiopts = mock.Mock()
        authopts = mock.Mock()
        confcust = mock.Mock()
        confcust.send_empty.return_value = False
        confcust.get_customers.return_value = ['CUSTOMERFOO', 'CUSTOMERBAR']
        confcust.get_custdir.return_value = '/some/path'
        custname = CUSTOMER_NAME
        feed = 'https://downtimes-csv.com/api/fetch'
        timestamp = datetime.datetime.now().strftime('%Y_%m_%d')
        current_date = datetime.datetime.now()
        self.downtimes_flat = TaskCsvDowntimes(
            self.loop,
            logger,
            'test_asynctasks_downtimesflat',
            globopts,
            webapiopts,
            confcust,
            custname,
            feed,
            current_date,
            True,
            current_date,
            timestamp
        )
        self.maxDiff = None

    @mock.patch('argo_connectors.tasks.flat_downtimes.write_json')
    @mock.patch('argo_connectors.tasks.flat_downtimes.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate, mock_writejson):
        self.downtimes_flat.fetch_data = mock.AsyncMock()
        self.downtimes_flat.fetch_data.side_effect = ['data_downtimes']
        self.downtimes_flat.send_webapi = mock.AsyncMock()
        self.downtimes_flat.parse_source = mock.MagicMock()
        await self.downtimes_flat.run()
        self.assertTrue(self.downtimes_flat.fetch_data.called)
        self.assertTrue(self.downtimes_flat.parse_source.called)
        self.downtimes_flat.parse_source.assert_called_with('data_downtimes')
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_downtimesflat')
        self.assertEqual(
            mock_writestate.call_args[0][3], self.downtimes_flat.timestamp)
        self.assertTrue(mock_writestate.call_args[0][4])
        self.assertTrue(mock_writejson.called, True)
        self.assertEqual(
            mock_writejson.call_args[0][4], datetime.datetime.now().strftime('%Y_%m_%d'))
        self.assertTrue(self.downtimes_flat.send_webapi.called)
        self.assertTrue(self.downtimes_flat.logger.info.called)

    @mock.patch('argo_connectors.tasks.flat_downtimes.write_state')
    @async_test
    async def test_StepsFailedRun(self, mock_writestate):
        self.downtimes_flat.fetch_data = mock.AsyncMock()
        self.downtimes_flat.fetch_data.side_effect = [
            ConnectorHttpError('fetch_data failed')]
        self.downtimes_flat.send_webapi = mock.AsyncMock()
        self.downtimes_flat.parse_source = mock.MagicMock()
        await self.downtimes_flat.run()
        self.assertTrue(self.downtimes_flat.fetch_data.called)
        self.assertFalse(self.downtimes_flat.parse_source.called)
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_downtimesflat')
        self.assertEqual(
            mock_writestate.call_args[0][3], self.downtimes_flat.timestamp)
        self.assertFalse(mock_writestate.call_args[0][4])
        self.assertTrue(self.downtimes_flat.logger.error.called)
        self.assertTrue(self.downtimes_flat.logger.error.call_args[0][0], repr(
            ConnectorHttpError('fetch_data failed')))
        self.assertFalse(self.downtimes_flat.send_webapi.called)


class GocdbDowntimes(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        mocked_globopts = dict(generalpublishwebapi='True',
                               generalwritejson='True',
                               outputdowntimes='downtimes_DATE.json',
                               inputstatesavedir='/some/mock/path/',
                               inputstatedays=3
                               )
        globopts = mocked_globopts
        authopts = mock.Mock()
        webapiopts = mock.Mock()
        confcust = mock.Mock()
        confcust.send_empty.return_value = False
        confcust.get_customers.return_value = ['CUSTOMERFOO', 'CUSTOMERBAR']
        custname = CUSTOMER_NAME
        downtime_feed = 'https://gocdb-downtimes.com/api/fetch'
        now = datetime.datetime.now().strftime('%Y-%m-%d')
        start = datetime.datetime.strptime(now, '%Y-%m-%d')
        end = datetime.datetime.strptime(now, '%Y-%m-%d')
        timestamp = start.strftime('%Y_%m_%d')
        start = start.replace(hour=0, minute=0, second=0)
        end = end.replace(hour=23, minute=59, second=59)

        self.gocdb_downtimes = TaskGocdbDowntimes(self.loop, logger, 'test_asynctasks_gocdbdowntimes', globopts,
                                                  authopts, webapiopts, confcust,
                                                  custname, downtime_feed, start,
                                                  end, False, timestamp, timestamp)

    @mock.patch('argo_connectors.tasks.gocdb_downtimes.write_json')
    @mock.patch('argo_connectors.tasks.gocdb_downtimes.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate, mock_writejson):
        self.gocdb_downtimes.fetch_data = mock.AsyncMock()
        self.gocdb_downtimes.fetch_data.side_effect = ['downtimes-ok']
        self.gocdb_downtimes.send_webapi = mock.AsyncMock()
        self.gocdb_downtimes.parse_source = mock.MagicMock()
        await self.gocdb_downtimes.run()
        self.assertTrue(self.gocdb_downtimes.fetch_data.called)
        self.assertTrue(self.gocdb_downtimes.parse_source.called)
        self.gocdb_downtimes.parse_source.assert_called_with('downtimes-ok')
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_gocdbdowntimes')
        self.assertEqual(
            mock_writestate.call_args[0][3], self.gocdb_downtimes.timestamp)
        self.assertTrue(mock_writestate.call_args[0][4])
        self.assertTrue(mock_writejson.called, True)
        self.assertEqual(
            mock_writejson.call_args[0][4], datetime.datetime.now().strftime('%Y_%m_%d'))
        self.assertTrue(self.gocdb_downtimes.send_webapi.called)
        self.assertTrue(self.gocdb_downtimes.logger.info.called)

    @mock.patch('argo_connectors.tasks.gocdb_downtimes.write_state')
    @async_test
    async def test_StepsFailedRun(self, mock_writestate):
        self.gocdb_downtimes.fetch_data = mock.AsyncMock()
        self.gocdb_downtimes.fetch_data.side_effect = [
            ConnectorHttpError('fetch_data failed')]
        self.gocdb_downtimes.send_webapi = mock.AsyncMock()
        self.gocdb_downtimes.parse_source = mock.MagicMock()
        await self.gocdb_downtimes.run()
        self.assertTrue(self.gocdb_downtimes.fetch_data.called)
        self.assertFalse(self.gocdb_downtimes.parse_source.called)
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_gocdbdowntimes')
        self.assertEqual(
            mock_writestate.call_args[0][3], self.gocdb_downtimes.timestamp)
        self.assertFalse(mock_writestate.call_args[0][4])
        self.assertTrue(self.gocdb_downtimes.logger.error.called)
        self.assertTrue(self.gocdb_downtimes.logger.error.call_args[0][0], repr(
            ConnectorHttpError('fetch_data failed')))
        self.assertFalse(self.gocdb_downtimes.send_webapi.called)


class WaporWeights(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        mocked_globopts = dict(generalpublishwebapi='True',
                               generalwritejson='True',
                               outputdowntimes='downtimes_DATE.json',
                               inputstatesavedir='/some/mock/path/',
                               inputstatedays=3
                               )
        globopts = mocked_globopts
        confcust = mock.Mock()
        confcust.send_empty.return_value = False
        confcust.get_customers.return_value = ['CUSTOMERFOO', 'CUSTOMERBAR']
        VAPORPI = 'https://foo-portal.eu/'
        jobcust = [('Critical', 'CUSTOMER_FOO')]
        cglob = mock.Mock()

        self.vapor_weights = TaskVaporWeights(self.loop, logger, 'test_asynctasks_weights', globopts,
                                              confcust, VAPORPI, jobcust, cglob,
                                              fixed_date=None)

    @mock.patch('argo_connectors.tasks.vapor_weights.write_json')
    @mock.patch('argo_connectors.tasks.vapor_weights.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate, mock_writejson):
        self.vapor_weights.fetch_data = mock.AsyncMock()
        self.vapor_weights.fetch_data.side_effect = ['weights-ok']
        self.vapor_weights.get_webapi_opts = mock.MagicMock()
        self.vapor_weights.parse_source = mock.MagicMock()
        self.vapor_weights.send_webapi = mock.AsyncMock()
        await self.vapor_weights.run()
        self.assertTrue(self.vapor_weights.fetch_data.called)
        self.assertTrue(self.vapor_weights.parse_source.called)
        self.vapor_weights.parse_source.assert_called_with('weights-ok')
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_weights')
        self.assertEqual(
            mock_writestate.call_args[0][3], 'Critical')
        self.assertTrue(mock_writestate.call_args[0][4])
        self.assertTrue(mock_writejson.called, True)
        self.assertTrue(self.vapor_weights.send_webapi.called)
        self.assertTrue(self.vapor_weights.logger.info.called)

    @mock.patch('argo_connectors.tasks.vapor_weights.write_state')
    @async_test
    async def test_StepsFailedRun(self, mock_writestate):
        self.vapor_weights.fetch_data = mock.AsyncMock()
        self.vapor_weights.fetch_data.side_effect = [
            ConnectorHttpError('fetch_data failed')]
        self.vapor_weights.get_webapi_opts = mock.MagicMock()
        self.vapor_weights.parse_source = mock.MagicMock()
        self.vapor_weights.send_webapi = mock.AsyncMock()
        await self.vapor_weights.run()
        self.assertTrue(self.vapor_weights.fetch_data.called)
        self.assertFalse(self.vapor_weights.parse_source.called)
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_weights')
        self.assertTrue(self.vapor_weights.logger.error.called)
        self.assertTrue(self.vapor_weights.logger.error.call_args[0][0], repr(
            ConnectorHttpError('fetch_data failed')))
        self.assertFalse(self.vapor_weights.send_webapi.called)


class MetricprofileWebapi(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        logger = mock.Mock()
        mocked_globopts = dict(generalpublishwebapi='True',
                               generalwritejson='True',
                               outputdowntimes='downtimes_DATE.json',
                               inputstatesavedir='/some/mock/path/',
                               inputstatedays=3
                               )
        globopts = mocked_globopts
        confcust = mock.Mock()
        confcust.send_empty.return_value = False
        confcust.get_jobs.return_value = ['job1', 'job2']
        confcust.get_custname.return_value = 'CUSTOMER_NAME'
        confcust.get_profiles.return_value = ['FOO_CUSTOMER_CRITICAL']
        cglob = mock.Mock()
        cglob.is_complete.return_value = True, None
        cglob.merge_opts.return_value = dict(
            webapitoken='505c3be00e9e30400b72dbfb0c06268aa73f694b', webapihost='api.devel.argo.grnet.gr')

        self.webapi_metricprofile = TaskWebApiMetricProfile(
            self.loop, logger, 'test_asynctasks_metricprofile', globopts, cglob, confcust, cust='CUSTOMER_FOO', fixed_date=None
        )

    @mock.patch('argo_connectors.tasks.webapi_metricprofile.write_json')
    @mock.patch('argo_connectors.tasks.webapi_metricprofile.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate, mock_writejson):
        self.webapi_metricprofile.fetch_data = mock.AsyncMock()
        self.webapi_metricprofile.parse_source = mock.MagicMock()
        await self.webapi_metricprofile.run()
        self.assertTrue(self.webapi_metricprofile.fetch_data.called)
        self.assertTrue(self.webapi_metricprofile.parse_source.called)
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_metricprofile')
        self.assertEqual(
            mock_writestate.call_args[0][3], 'job2')
        self.assertTrue(mock_writestate.call_args[0][4])
        self.assertTrue(mock_writejson.called, True)
        self.assertTrue(self.webapi_metricprofile.logger.info.called)

    @mock.patch('argo_connectors.tasks.webapi_metricprofile.write_state')
    @async_test
    async def test_StepsFailedRun(self, mock_writestate):
        self.webapi_metricprofile.fetch_data = mock.AsyncMock()
        self.webapi_metricprofile.fetch_data.side_effect = [
            ConnectorHttpError('fetch_data failed')]
        self.webapi_metricprofile.parse_source = mock.MagicMock()
        await self.webapi_metricprofile.run()
        self.assertTrue(self.webapi_metricprofile.fetch_data.called)
        self.assertFalse(self.webapi_metricprofile.parse_source.called)
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_metricprofile')
        self.assertFalse(mock_writestate.call_args[0][6])
        self.assertTrue(self.webapi_metricprofile.logger.error.called)
        self.assertTrue(self.webapi_metricprofile.logger.error.call_args[0][0], repr(
            ConnectorHttpError('fetch_data failed')))


class TopologyCsv(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        with open('tests/sample-csv-topology.csv') as tf:
            self.res = tf.read()

        logger = mock.Mock()
        mocked_globopts = dict(generalpublishwebapi='True',
                               generalwritejson='True',
                               outputdowntimes='downtimes_DATE.json',
                               inputstatesavedir='/some/mock/path/',
                               inputstatedays=3
                               )
        globopts = mocked_globopts
        webapi_opts = mock.Mock()
        confcust = mock.Mock()
        confcust.send_empty.return_value = False
        confcust.get_customers.return_value = ['CUSTOMERFOO', 'CUSTOMERBAR']
        custname = CUSTOMER_NAME
        topofeed = 'https://topo-csv-foo.com/api/fetch'
        fetchtype = 'foo-groups'

        self.flat_topology = TaskFlatTopology(self.loop, logger, 'test_asynctasks_topology_csv', globopts, webapi_opts,
                                              confcust, custname, topofeed, fetchtype, fixed_date=None,
                                              uidservendp=True, is_csv=True)

    @mock.patch('argo_connectors.tasks.flat_topology.write_json')
    @mock.patch('argo_connectors.tasks.flat_topology.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate, mock_writejson):
        self.flat_topology.fetch_data = mock.AsyncMock()
        self.flat_topology.fetch_data.return_value = self.res
        self.flat_topology.parse_source_topo = mock.MagicMock()
        self.flat_topology.parse_source_topo.return_value = {'type': 'FOO_PROJECT'}, {
            'type': 'FOO_SERVICEGROUPS', 'group': 'mock_group'}
        self.flat_topology.send_webapi = mock.AsyncMock()
        await self.flat_topology.run()
        self.assertTrue(self.flat_topology.fetch_data.called)
        self.assertTrue(self.flat_topology.parse_source_topo.called)
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_topology_csv')
        self.assertTrue(mock_writestate.call_args[0][4])
        self.assertTrue(mock_writejson.called, True)
        self.assertTrue(self.flat_topology.send_webapi.called)
        self.assertTrue(self.flat_topology.logger.info.called)

    @mock.patch('argo_connectors.tasks.flat_topology.write_state')
    @async_test
    async def test_FailedSuccessRun(self, mock_writestate):
        self.flat_topology.fetch_data = mock.AsyncMock()
        self.flat_topology.fetch_data.side_effect = [
            ConnectorHttpError('fetch_data failed')]
        self.flat_topology.parse_source_topo = mock.MagicMock()
        self.flat_topology.send_webapi = mock.AsyncMock()
        await self.flat_topology.run()
        self.assertTrue(self.flat_topology.fetch_data.called)
        self.assertFalse(self.flat_topology.parse_source_topo.called)
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_topology_csv')
        self.assertFalse(mock_writestate.call_args[0][4])
        self.assertTrue(self.flat_topology.logger.error.called)
        self.assertTrue(self.flat_topology.logger.error.call_args[0][0], repr(
            ConnectorHttpError('fetch_data failed')))


class TopologyAgora(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        mocked_globopts = dict(generalpublishwebapi='True',
                               generalwritejson='True',
                               outputdowntimes='downtimes_DATE.json',
                               inputstatesavedir='/some/mock/path/',
                               inputstatedays=3
                               )
        globopts = mocked_globopts
        webapi_opts = mock.Mock()
        confcust = mock.Mock()
        confcust.send_empty.return_value = False
        confcust.get_customers.return_value = ['CUSTOMERFOO', 'CUSTOMERBAR']
        confcust.get_topofeedservicegroups.return_value = 'https://agora.ni4os.eu/api/v2/public/providers/'
        confcust.get_topofeedendpoints.return_value = 'https://agora.ni4os.eu/api/v2/public/resources/'

        uidservendp = False
        fetchtype = 'foo-servicegroups'

        self.agora_topology = AgoraProviderTopology(self.loop, logger, 'test_asynctasks_agora_topology', globopts, webapi_opts,
                                                    confcust, uidservendp, fetchtype, fixed_date=None)

    @mock.patch('argo_connectors.tasks.agora_topology.contains_exception')
    @mock.patch('argo_connectors.tasks.agora_topology.write_json')
    @mock.patch('argo_connectors.tasks.agora_topology.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate, mock_writejson, mock_contains_exception):
        self.agora_topology.fetch_data = mock.AsyncMock()
        self.agora_topology.fetch_data.side_effect = ['weights-ok']
        mock_contains_exception.return_value = False, None
        self.agora_topology.parse_source_topo = mock.MagicMock()
        self.agora_topology.parse_source_topo.return_value = [{'group': 'NI4OS Providers', 'type': 'PROVIDERS', 'subgroup': 'UoB_IBISS', 'tags': {'info_ext_catalog_id': '02dc5b9a-99ba-4924-ab80-aa51b9c86b1e'}}], [
            {'group': 'UoB-RCUB', 'type': 'SERVICEGROUPS', 'service': 'catalog.service.entry', 'hostname': 'agora.ni4os.eu_uob_nardus', 'tags': {'hostname': 'agora.ni4os.eu'}}]
        self.agora_topology.send_webapi = mock.AsyncMock()
        await self.agora_topology.run()
        self.assertTrue(self.agora_topology.fetch_data.called)
        self.assertTrue(self.agora_topology.parse_source_topo.called)
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_agora_topology')
        self.assertTrue(mock_writestate.call_args[0][4])
        self.assertTrue(mock_writejson.called, True)
        self.assertTrue(self.agora_topology.send_webapi.called)
        self.assertTrue(self.agora_topology.logger.info.called)

    @mock.patch('argo_connectors.tasks.agora_topology.write_state')
    @async_test
    async def test_FailedSuccessRun(self, mock_writestate):
        self.agora_topology.fetch_data = mock.AsyncMock()
        self.agora_topology.fetch_data.side_effect = [
            ConnectorHttpError('fetch_data failed')]
        self.agora_topology.parse_source_topo = mock.MagicMock()
        self.agora_topology.send_webapi = mock.AsyncMock()
        await self.agora_topology.run()
        self.assertTrue(self.agora_topology.fetch_data.called)
        self.assertFalse(self.agora_topology.parse_source_topo.called)
        self.assertEqual(
            mock_writestate.call_args[0][0], 'test_asynctasks_agora_topology')
        self.assertFalse(mock_writestate.call_args[0][4])
        self.assertTrue(self.agora_topology.logger.error.called)
        self.assertTrue(self.agora_topology.logger.error.call_args[0][0], repr(
            ConnectorHttpError('fetch_data failed')))
