#!/usr/bin/env python

import sys
import asyncio

from argo_connectors.exceptions import ConnectorParseError, ConnectorHttpError
from argo_connectors.exe.connector import ExecConnector
from argo_connectors.log import Logger
from argo_connectors.tasks.common import write_state
from argo_connectors.tasks.flat_topology import TaskFlatTopology


def main():
    conn_exec = ExecConnector(
        description="""Fetch entities (ServiceGroups, Sites, Endpoints) \
                       from CSV topology feed, record it in JSON files and send it to WEB-API""",
        exe_script=sys.argv[0]
    )

    try:
        task = TaskFlatTopology(conn_exec.fixed_date, is_csv=True)
        asyncio.run(task.run())

    except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        Logger.error(repr(exc))
        asyncio.run(write_state(conn_exec.fixed_date, False))


if __name__ == '__main__':
    main()
