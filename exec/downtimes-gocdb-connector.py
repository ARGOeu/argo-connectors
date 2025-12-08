#!/usr/bin/env python

import datetime
import sys
import asyncio

from argo_connectors.exe.connector import ExecConnector
from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_connectors.tasks.gocdb_downtimes import TaskGocdbDowntimes
from argo_connectors.tasks.common import write_state
from argo_connectors.log import Logger


def main():
    conn_exec = ExecConnector(
        description='Fetch downtimes from GOCDB for given date, record it in JSON files and send it to WEB-API',
        exe_script=sys.argv[0],
        date_required=True,
    )

    # calculate start and end times
    try:
        start = datetime.datetime.strptime(conn_exec.args.date, '%Y-%m-%d')
        end = datetime.datetime.strptime(conn_exec.args.date, '%Y-%m-%d')
        timestamp = start.strftime('%Y_%m_%d')
        start = start.replace(hour=0, minute=0, second=0)
        end = end.replace(hour=23, minute=59, second=59)

    except ValueError as exc:
        Logger.error(exc)
        raise SystemExit(1)

    try:
        task = TaskGocdbDowntimes(start, end, conn_exec.args.date, timestamp)
        asyncio.run(task.run())

    except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        Logger.error(repr(exc))
        asyncio.run(write_state(timestamp, False))


if __name__ == '__main__':
    main()
