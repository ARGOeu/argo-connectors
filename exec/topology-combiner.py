#!/usr/bin/env python

import asyncio
import sys

from argo_connectors.tasks.combine_topology import TaskCombineTopology
from argo_connectors.exe.combiner import ExecCombiner
from argo_connectors.log import Logger
from argo_connectors.tasks.common import write_state
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError


def main():
    combine_exec = ExecCombiner(
        description="""Combiner that calls topology tasks specified in YAML file, joins their data, record it in JSON file and push it to WEB-API""",
        exe_script=sys.argv[0],
        combiner='topology'
    )

    try:
        task = TaskCombineTopology(combine_exec)
        asyncio.run(task.run())

    except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        Logger.error(repr(exc))
        asyncio.run(write_state(None, False, combine_exec.tasks[0]['id']))


if __name__ == '__main__':
    main()
