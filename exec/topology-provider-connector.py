#!/usr/bin/env python

import sys
import asyncio

from argo_connectors.exceptions import ConnectorError, ConnectorHttpError, ConnectorParseError
from argo_connectors.exe.connector import ExecConnector
from argo_connectors.log import Logger
from argo_connectors.tasks.common import write_state
from argo_connectors.tasks.provider_topology import TaskProviderTopology


def main():
    conn_exec = ExecConnector(
        description="Fetch and construct entities from EOSC Beyond Provider \
                     record it in JSON files and send it to WEB-API",
        exe_script=sys.argv[0]
    )

    try:
        task = TaskProviderTopology(conn_exec.fixed_date)
        asyncio.run(task.run())

    except (ConnectorError, ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        Logger.error(repr(exc))
        asyncio.run(write_state(conn_exec.fixed_date, False))


if __name__ == '__main__':
    main()
