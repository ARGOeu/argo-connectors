import asyncio

from argo_connectors.io.webapi import WebAPI
from argo_connectors.log import Logger
from argo_connectors.tasks.flat_servicetypes import TaskFlatServiceTypes
from argo_connectors.tasks.gocdb_servicetypes import TaskGocdbServiceTypes
from argo_connectors.tasks.common import write_state, write_servicetypes_json as write_json
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError


class TaskCombineServiceTypes:
    def __init__(self, combine_exec):
        self.combine_exec = combine_exec
        pass

    def combine(self, servicetypes):
        joint_servicetypes = list()

        for st in servicetypes:
            joint_servicetypes += st

        return joint_servicetypes

    async def run(self):
        coros = list()

        for task in self.combine_exec.tasks:
            if task['type'] == 'gocdb':
                coros.append(TaskGocdbServiceTypes(None, initsync=False,
                                                   combuid=task['id']).run())
            elif task['type'] == 'csv':
                coros.append(TaskFlatServiceTypes(None, True, initsync=False,
                                                  combuid=task['id']).run())
            elif task['type'] == 'json':
                coros.append(TaskFlatServiceTypes(None, False, initsync=False,
                                                  combuid=task['id']).run())

        try:
            data_fetched = await asyncio.gather(*coros)
            servicetypes = self.combine(data_fetched)

            await write_state(None, True, task['id'])

            numst = len(servicetypes)

            Logger.info('Customer:' + self.combine_exec.tenant_name + ' Joined ServiceTypes:%d' % (numst))

            if self.combine_exec.globopts.options()['GeneralWriteJson'.lower()]:
                write_json(servicetypes, None, task['id'])

            if self.combine_exec.globopts.options()['GeneralPublishWebAPI'.lower()]:
                webapi = WebAPI(combuid=task['id'])
                await webapi.send(servicetypes, 'service-types')
                await webapi.session.close()

        except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
            Logger.error(repr(exc))
            await write_state(None, False, task['id'])
