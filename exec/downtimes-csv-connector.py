#!/usr/bin/env python

import datetime
import sys
import asyncio

from argo_connectors.exe.connector import ExecConnector
from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_connectors.tasks.flat_downtimes import TaskCsvDowntimes
from argo_connectors.tasks.common import write_state
from argo_connectors.log import Logger


def main():
    conn_exec = ExecConnector(
        description='Fetch downtimes from CSV for given date, record it in JSON files and send it to WEB-API',
        exe_script=sys.argv[0],
        date_required=True,
    )

    # calculate start and end times
    try:
        current_date = datetime.datetime.strptime(conn_exec.args.date, '%Y-%m-%d')
        timestamp = current_date.strftime('%Y_%m_%d')
        current_date = current_date.replace(hour=0, minute=0, second=0)

    except ValueError as exc:
        Logger.error(exc)
        raise SystemExit(1)

    try:
        task = TaskCsvDowntimes(current_date, conn_exec.args.date, timestamp)
        asyncio.run(task.run())

    except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        Logger.error(repr(exc))
        asyncio.run(write_state(timestamp, False))


if __name__ == '__main__':
    main()
