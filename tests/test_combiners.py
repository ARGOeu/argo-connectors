import unittest

from unittest import mock
from unittest.mock import patch

from argo_connectors.exe.combiner import ExecCombiner
from argo_connectors.tasks.combine_servicetypes import TaskCombineServiceTypes


class CombinerTopology(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.maxDiff = None
        self.patcher1 = patch('argo_connectors.exe.combiner.argparse.ArgumentParser.parse_args')
        self.patcher2 = patch('argo_connectors.config.customer._CustomerConf.make_dirstruct')
        argmock = mock.Mock()
        argmock.yamlconf = 'tests/sample-combine.yml'
        mock_parseargs = self.patcher1.start()
        mock_parseargs.return_value = argmock
        _ = self.patcher2.start()
        exec_combiner = ExecCombiner(
            description="""Combiner tests""",
            exe_script="topology-combiner.py",
            combiner="topology",
        )
        from argo_connectors.tasks.combine_topology import TaskCombineTopology
        self.topo_combine = TaskCombineTopology(exec_combiner)

    @mock.patch('argo_connectors.tasks.combine_topology.write_state')
    @mock.patch('argo_connectors.tasks.lot1sc_topology.attach_tags')
    @mock.patch('argo_connectors.tasks.combine_topology.TaskLot1ScTopology.parse_source_topo')
    @mock.patch('argo_connectors.tasks.combine_topology.TaskGocdbTopology.fetch_ldap_data')
    @mock.patch('argo_connectors.tasks.combine_topology.TaskGocdbTopology.fetch_data')
    @mock.patch('argo_connectors.tasks.combine_topology.TaskLot1ScTopology.fetch_data')
    async def test_ParseTasks(self, mock_fetchlot1sc, mock_fetchgocdb,
                              mock_fetchgocdbldap, mock_parselot1sc,
                              mock_attachtagslot1sc, mock_writestate):
        mock_fetchlot1sc.return_value = 'LOT1SC data'
        mock_fetchgocdb.return_value = 'GOCDB data'
        mock_fetchgocdbldap.return_value = 'GOCDB LDAP data'
        mock_parselot1sc.return_value = ['parsed LOT1SC group groups'], ['parsed LOT1SC group endpoints']
        self.assertListEqual(
            [
                {'type': 'gocdb', 'id': '1-gocdb'},
                {'type': 'lot1sc', 'id': '2-lot1sc'}
            ],
            self.topo_combine.combine_exec.tasks
        )
        mock_attachtagslot1sc.return_value = (
            'parsed LOT1SC group groups with tags',
            'parsed LOT1SC group endpoints with tags'
        )
        await self.topo_combine.run()
        mock_attachtagslot1sc.assert_called_with(
            ['parsed LOT1SC group groups', 'parsed LOT1SC group groups'],
            ['parsed LOT1SC group endpoints', 'parsed LOT1SC group endpoints'],
            [{'scope': 'EXCHANGE'}], [{'scope': 'EXCHANGE'}]
        )
        self.assertEqual(self.topo_combine.combine_exec.globopts.options()['webapitoken'], 'SAMPLE_TOKEN1')
        self.assertEqual(self.topo_combine.combine_exec.globopts.options()['webapihost'], 'API.HOST.HR')
        self.assertTrue(mock_writestate.called)
        self.assertFalse(mock_writestate.call_args[0][1])

    def tearDown(self):
        self.patcher1.stop()
        self.patcher2.stop()


class CombinerServiceTypes(unittest.TestCase):
    def test_combine_keeps_first_duplicate_service_name(self):
        combiner = TaskCombineServiceTypes(mock.Mock())
        servicetypes = combiner.combine([
            [
                {
                    'name': 'service.type.1',
                    'description': 'from first feed',
                    'tags': ['topology']
                },
                {
                    'name': 'service.type.2',
                    'description': 'from first feed',
                    'tags': ['topology']
                }
            ],
            [
                {
                    'name': 'service.type.1',
                    'description': 'from second feed',
                    'tags': ['topology']
                },
                {
                    'name': 'service.type.3',
                    'description': 'from second feed',
                    'tags': ['topology']
                }
            ]
        ])

        self.assertEqual(servicetypes, [
            {
                'name': 'service.type.1',
                'description': 'from first feed',
                'tags': ['topology']
            },
            {
                'name': 'service.type.2',
                'description': 'from first feed',
                'tags': ['topology']
            },
            {
                'name': 'service.type.3',
                'description': 'from second feed',
                'tags': ['topology']
            }
        ])


if __name__ == '__main__':
    unittest.main()
