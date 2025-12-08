import datetime
import os
import aiofiles

from argo_connectors.utils import datestamp
from argo_connectors.config.glob import Global


daysback = 1


async def state_write(statedir, state, date=None):
    filenamenew = ''
    savedays = Global.options()['InputStateDays'.lower()]

    if 'topology' in Global.caller:
        filenamebase = 'topology-ok'
    elif 'weights' in Global.caller:
        filenamebase = 'weights-ok'
    elif 'downtimes' in Global.caller:
        filenamebase = 'downtimes-ok'
    elif 'service-types' in Global.caller:
        filenamebase = 'services-ok'

    if date:
        datebackstamp = date
    else:
        datebackstamp = datestamp(daysback)

    filenamenew = filenamebase + '_' + datebackstamp
    db = datetime.datetime.strptime(datebackstamp, '%Y_%m_%d')

    datestart = db - datetime.timedelta(days=int(savedays))
    i = 0
    while i < int(savedays) * 2:
        d = datestart - datetime.timedelta(days=i)
        filenameold = filenamebase + '_' + d.strftime('%Y_%m_%d')
        if os.path.exists(statedir + '/' + filenameold):
            os.remove(statedir + '/' + filenameold)
        i += 1

    async with aiofiles.open(statedir + '/' + filenamenew, mode='w') as fp:
        await fp.write(str(state))
