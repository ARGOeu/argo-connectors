import unittest
import asyncio

import mock

from argo_connectors.log import Logger
from argo_connectors.tasks.gocdb_topology import TaskGocdbTopology

logger = Logger('test_tasktopogocdb.py')


class async_test(object):
    """
    Decorator to create asyncio context for asyncio methods or functions.
    """
    def __init__(self, test_method):
        self.test_method = test_method

    def __call__(self, *args, **kwargs):
        test_obj = args[0]
        test_obj.loop.run_until_complete(self.test_method(*args, **kwargs))


class TaskGocdbTopo(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()

    @mock.patch('argo_connectors.io.http')
    @async_test
    async def test_stepCalled(self, mocked_http):
        globopts = dict()
        authopts = dict()
        webapiopts = dict()
        bdiiopts = dict()
        confcust = mock.Mock()
        self.task = TaskGocdbTopology(self.loop, logger, 'test_tasktopogocdb',
                                      '/api/sitecontacts',
                                      '/api/service-group-contacts',
                                      '/api/service-endpoints-api',
                                      '/api/service-groups-api',
                                      '/api/sites-api', globopts, authopts,
                                      webapiopts, bdiiopts, confcust, 'TESTCUSTOMER',
                                      'https://topofeed.com', 'SITES', True,
                                      True, True, True)
        self.task.fetch_data = mock.AsyncMock()
        self.task.fetch_data.side_effect = ['data1', 'data2']
        # await self.task.run()
