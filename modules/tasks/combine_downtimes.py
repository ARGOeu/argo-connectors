import datetime
import asyncio

from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.io.webapi import WebAPI
from argo_connectors.log import Logger
from argo_connectors.tasks.common import write_state, write_downtimes_json as write_json
from argo_connectors.tasks.flat_downtimes import TaskCsvDowntimes
from argo_connectors.tasks.gocdb_downtimes import TaskGocdbDowntimes


class TaskCombineDowntimes:
    def __init__(self, combine_exec):
        self.combine_exec = combine_exec

    def combine(self, downtimes):
        joint_downtimes = list()

        for dt in downtimes:
            joint_downtimes += dt

        return joint_downtimes

    async def run(self):
        coros = list()

        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        # calculate start and end times
        try:
            start = datetime.datetime.strptime(current_date, '%Y-%m-%d')
            end = datetime.datetime.strptime(current_date, '%Y-%m-%d')
            timestamp = start.strftime('%Y_%m_%d')
            start = start.replace(hour=0, minute=0, second=0)
            end = end.replace(hour=23, minute=59, second=59)

        except ValueError as exc:
            Logger.error(exc)
            raise SystemExit(1)

        for task in self.combine_exec.tasks:
            if task['type'] == 'gocdb':
                coros.append(TaskGocdbDowntimes(start, end, current_date,
                                                timestamp,
                                                combuid=task['id']).run())
            elif task['type'] == 'csv':
                coros.append(TaskCsvDowntimes(start, end,
                                              current_date, timestamp,
                                              combuid=task['id']).run())

        try:
            data_fetched = await asyncio.gather(*coros)
            downtimes = self.combine(data_fetched)
            await write_state(None, True, task['id'])

            numst = len(downtimes)

            Logger.info('Customer:' + self.combine_exec.tenant_name + ' Joined Downtimes:%d' % (numst))

            if self.combine_exec.globopts.options()['GeneralWriteJson'.lower()]:
                write_json(downtimes, None, task['id'])

            if self.combine_exec.globopts.options()['GeneralPublishWebAPI'.lower()]:
                webapi = WebAPI(combuid=task['id'])
                await webapi.send(downtimes, downtimes_component=True)
                await webapi.session.close()

        except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
            Logger.error(repr(exc))
            await write_state(None, False, task['id'])
