#!/usr/bin/env python

import asyncio
import sys

from argo_connectors.exe.combiner import ExecCombiner

from argo_connectors.tasks.combine_servicetypes import TaskCombineServiceTypes
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.log import Logger
from argo_connectors.tasks.common import write_state


def main():
    combine_exec = ExecCombiner(
        description="""Combiner that calls service-types tasks specified in YAML file, joins their data, record it in JSON file and push it to WEB-API""",
        exe_script=sys.argv[0],
        combiner='service-types'
    )

    try:
        task = TaskCombineServiceTypes(combine_exec)
        asyncio.run(task.run())

    except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        Logger.error(repr(exc))
        asyncio.run(write_state(None, False, combine_exec.tasks[0]['id']))


if __name__ == '__main__':
    main()
