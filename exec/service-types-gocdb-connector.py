#!/usr/bin/env python

import sys
import asyncio

from argo_connectors.exe.connector import ExecConnector
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.tasks.gocdb_servicetypes import TaskGocdbServiceTypes
from argo_connectors.tasks.common import write_state
from argo_connectors.log import Logger


def main():
    conn_exec = ExecConnector(
        description="Fetch service types from GOCDB and send it to WEB-API",
        initial_arg=True,
        initial_arg_help="Initial sync of service types",
        exe_script=sys.argv[0]
    )

    try:
        task = TaskGocdbServiceTypes(conn_exec.fixed_date, conn_exec.args.initsync)
        asyncio.run(task.run())

    except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        Logger.error(repr(exc))
        asyncio.run(write_state(conn_exec.fixed_date, False))


if __name__ == '__main__':
    main()
