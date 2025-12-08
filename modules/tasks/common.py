from argo_connectors.io.statewrite import state_write
from argo_connectors.utils import filename_date
from argo_connectors.io.jsonwrite import JsonWriter
from argo_connectors.config.glob import Global
from argo_connectors.config.customer import get_custconf, Customer
from argo_connectors.log import Logger

# TODO: remove Customer import and rely only on get_custconf for weights and metricprofile


async def write_state(fixed_date, state, combuid=None):
    customer = get_custconf(combuid)
    cust = list(customer.get_customers())[0]
    jobstatedir = customer.get_fullstatedir(
        Global.options()['InputStateSaveDir'.lower()], cust)
    if fixed_date:
        await state_write(jobstatedir, state, fixed_date.replace('-', '_'))
    else:
        await state_write(jobstatedir, state)


async def write_weights_metricprofile_state(cust, job, fixed_date, state):
    jobstatedir = Customer.get_fullstatedir(
        Global.options()['InputStateSaveDir'.lower()], cust, job)
    if fixed_date:
        await state_write(jobstatedir, state, fixed_date.replace('-', '_'))
    else:
        await state_write(jobstatedir, state)


def write_downtimes_json(dts, timestamp, combuid=None):
    customer = get_custconf(combuid)
    custdir = customer.get_custdir()
    filename = filename_date(
        Global.options()['OutputDowntimes'.lower()], custdir, stamp=timestamp)
    json_writer = JsonWriter(dts, filename, Global.options()['generalcompressjson'])
    ret, excep = json_writer.write_json()
    if not ret:
        Logger.error('Customer:{} {}'.format(Logger.customer, repr(excep)))
        raise SystemExit(1)


def write_servicetypes_json(servicetypes, timestamp, combuid=None):
    customer = get_custconf(combuid)
    custdir = customer.get_custdir()
    filename = filename_date(
        Global.options()['OutputServiceTypes'.lower()], custdir, stamp=timestamp)
    json_writer = JsonWriter(servicetypes, filename, Global.options()['generalcompressjson'])
    ret, excep = json_writer.write_json()
    if not ret:
        Logger.error('Customer:{} {}'.format(Logger.customer, repr(excep)))
        raise SystemExit(1)


def write_weights_json(cust, job, fixed_date, weights):
    jobdir = Customer.get_fulldir(cust, job)
    if fixed_date:
        filename = filename_date(Global.options()['OutputWeights'.lower()],
                                 jobdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(Global.options()['OutputWeights'.lower()],
                                 jobdir)

    json_writer = JsonWriter(weights, filename, Global.options()['generalcompressjson'])
    ret, excep = json_writer.write_json()
    if not ret:
        Logger.error('Customer:%s Job:%s %s' %
                     (Logger.customer, Logger.job, repr(excep)))
        raise SystemExit(1)


def write_topo_json(group_groups, group_endpoints, fixed_date, combuid=None):
    customer = get_custconf(combuid)
    custdir = customer.get_custdir()

    if fixed_date:
        filename = filename_date(Global.options()['OutputTopologyGroupOfGroups'.lower()],
                                 custdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(Global.options()['OutputTopologyGroupOfGroups'.lower()],
                                 custdir)
    json_writer = JsonWriter(group_groups, filename, Global.options()['GeneralCompressJson'.lower()])
    ret, excep = json_writer.write_json()
    if not ret:
        Logger.error('Customer:%s : %s' % (Logger.customer, repr(excep)))
        raise SystemExit(1)

    if fixed_date:
        filename = filename_date(Global.options()['OutputTopologyGroupOfEndpoints'.lower()],
                                 custdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(Global.options()['OutputTopologyGroupOfEndpoints'.lower()],
                                 custdir)
    json_writer = JsonWriter(group_endpoints, filename,
                             Global.options()['GeneralCompressJson'.lower()])
    ret, excep = json_writer.write_json()
    if not ret:
        Logger.error('Customer:%s : %s' % (Logger.customer, repr(excep)))
        raise SystemExit(1)
