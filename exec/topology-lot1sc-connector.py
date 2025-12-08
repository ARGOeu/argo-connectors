#!/usr/bin/env python

import sys
import asyncio

from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError, ConnectorError
from argo_connectors.exe.connector import ExecConnector
from argo_connectors.log import Logger
from argo_connectors.tasks.common import write_state
from argo_connectors.tasks.lot1sc_topology import TaskLot1ScTopology


def main():
    conn_exec = ExecConnector(
        description="""Fetch entities (ServiceGroups, Sites, Endpoints) \
                       from LOT1 Service Catalogue topology feed, record it in JSON files and send it to WEB-API""",
        exe_script=sys.argv[0]
    )

    try:
        task = TaskLot1ScTopology(conn_exec.fixed_date)
        asyncio.run(task.run())

    except (ConnectorError, ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        Logger.error(repr(exc))
        asyncio.run(write_state(conn_exec.fixed_date, False))


if __name__ == '__main__':
    main()
