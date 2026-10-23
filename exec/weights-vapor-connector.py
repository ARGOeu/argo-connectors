#!/usr/bin/env python

import sys
import asyncio

from argo_connectors.exe.connector import ExecConnector
from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_connectors.tasks.vapor_weights import TaskVaporWeights
from argo_connectors.tasks.common import write_weights_metricprofile_state as write_state
from argo_connectors.log import Logger


def main():
    conn_exec = ExecConnector(
        description="""Fetch weights information from Gstat provider \
                       for every job listed in customer.conf""",
        exe_script=sys.argv[0]
    )

    vaporpi = conn_exec.config_customer.opt('Vaporpi')
    feeds = conn_exec.config_customer.get_mapfeedjobs(sys.argv[0], deffeed=vaporpi)

    for feed, jobcust in feeds.items():
        customers = set(map(lambda jc: conn_exec.config_customer.get_custname(jc[1]), jobcust))
        customers = customers.pop() if len(
            customers) == 1 else '({0})'.format(','.join(customers))
        sjobs = set(map(lambda jc: jc[0], jobcust))
        jobs = list(sjobs)[0] if len(
            sjobs) == 1 else '({0})'.format(','.join(sjobs))
        Logger.job = jobs
        Logger.customer = customers

        try:
            task = TaskVaporWeights(jobcust, conn_exec.fixed_date, feed)
            asyncio.run(task.run())

        except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
            Logger.error(repr(exc))
            for job, cust in jobcust:
                asyncio.run(write_state(conn_exec.fixed_date, True))


if __name__ == '__main__':
    main()
