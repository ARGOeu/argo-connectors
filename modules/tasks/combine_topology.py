import asyncio

from argo_connectors.io.webapi import WebAPI
from argo_connectors.log import Logger
from argo_connectors.tasks.common import write_state, write_topo_json as write_json
from argo_connectors.tasks.flat_topology import TaskFlatTopology
from argo_connectors.tasks.gocdb_topology import TaskGocdbTopology
from argo_connectors.tasks.lot1sc_topology import TaskLot1ScTopology
from argo_connectors.tasks.provider_topology import TaskProviderTopology
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError


class TaskCombineTopology:
    def __init__(self, combine_exec):
        self.combine_exec = combine_exec

    def combine(self, topologies):
        joint_gg, joint_ge = list(), list()

        for topo in topologies:
            group_groups, group_endpoints = topo
            joint_gg += group_groups
            joint_ge += group_endpoints

        return joint_gg, joint_ge

    async def run(self):
        coros = list()

        for task in self.combine_exec.tasks:
            if task['type'] == 'gocdb':
                coros.append(TaskGocdbTopology(None, task['id']).run())
            elif task['type'] == 'lot1sc':
                coros.append(TaskLot1ScTopology(None, task['id']).run())
            elif task['type'] == 'provider':
                coros.append(TaskProviderTopology(None, task['id']).run())
            elif task['type'] == 'csv':
                coros.append(TaskFlatTopology(None, True,
                                              combuid=task['id']).run())
            elif task['type'] == 'json':
                coros.append(TaskFlatTopology(None, False,
                                              combuid=task['id']).run())

        try:
            data_fetched = await asyncio.gather(*coros)
            group_groups, group_endpoints = self.combine(data_fetched)

            await write_state(None, True, task['id'])

            numge = len(group_endpoints)
            numgg = len(group_groups)

            Logger.info('Customer:' + self.combine_exec.tenant_name + ' Joined Endpoints:%d' % (numge) + ' Groups:%d' % (numgg))

            if self.combine_exec.globopts.options()['GeneralWriteJson'.lower()]:
                write_json(group_groups, group_endpoints, None, task['id'])

            if self.combine_exec.globopts.options()['GeneralPublishWebAPI'.lower()]:
                webapi = WebAPI(combuid=task['id'])
                await asyncio.gather(
                    webapi.send(group_groups, 'groups'),
                    webapi.send(group_endpoints, 'endpoints')
                )
                await webapi.session.close()

        except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
            Logger.error(repr(exc))
            await write_state(None, False, task['id'])
