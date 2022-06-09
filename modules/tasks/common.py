from argo_connectors.io.statewrite import state_write
from argo_connectors.utils import filename_date, datestamp, date_check
from argo_connectors.io.avrowrite import AvroWriter


async def write_state(connector_name, globopts, confcust, fixed_date, state):
    cust = list(confcust.get_customers())[0]
    jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
    if fixed_date:
        await state_write(connector_name, jobstatedir, state,
                          globopts['InputStateDays'.lower()],
                          fixed_date.replace('-', '_'))
    else:
        await state_write(connector_name, jobstatedir, state,
                          globopts['InputStateDays'.lower()])


async def write_weights_metricprofile_state(connector_name, globopts, cust, job, confcust, fixed_date, state):
    jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust, job)
    if fixed_date:
        await state_write(connector_name, jobstatedir, state,
                          globopts['InputStateDays'.lower()],
                          fixed_date.replace('-', '_'))
    else:
        await state_write(connector_name, jobstatedir, state,
                          globopts['InputStateDays'.lower()])


def write_metricprofile_avro(logger, globopts, cust, job, confcust, fixed_date, fetched_profiles):
    jobdir = confcust.get_fulldir(cust, job)
    if fixed_date:
        filename = filename_date(logger, globopts['OutputMetricProfile'.lower()], jobdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(logger, globopts['OutputMetricProfile'.lower()], jobdir)
    avro = AvroWriter(globopts['AvroSchemasMetricProfile'.lower()], filename)
    ret, excep = avro.write(fetched_profiles)
    if not ret:
        logger.error('Customer:%s Job:%s %s' % (logger.customer, logger.job, repr(excep)))
        raise SystemExit(1)


def write_downtimes_avro(logger, globopts, confcust, dts, timestamp):
    custdir = confcust.get_custdir()
    filename = filename_date(logger, globopts['OutputDowntimes'.lower()], custdir, stamp=timestamp)
    avro = AvroWriter(globopts['AvroSchemasDowntimes'.lower()], filename)
    ret, excep = avro.write(dts)
    if not ret:
        logger.error('Customer:{} {}'.format(logger.customer, repr(excep)))
        raise SystemExit(1)


def write_weights_avro(logger, globopts, cust, job, confcust, fixed_date, weights):
    jobdir = confcust.get_fulldir(cust, job)
    if fixed_date:
        filename = filename_date(logger, globopts['OutputWeights'.lower()], jobdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(logger, globopts['OutputWeights'.lower()], jobdir)
    avro = AvroWriter(globopts['AvroSchemasWeights'.lower()], filename)
    ret, excep = avro.write(weights)
    if not ret:
        logger.error('Customer:%s Job:%s %s' % (logger.customer, logger.job, repr(excep)))
        raise SystemExit(1)


def write_topo_avro(logger, globopts, confcust, group_groups, group_endpoints, fixed_date):
    custdir = confcust.get_custdir()
    if fixed_date:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], custdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], custdir)
    avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfGroups'.lower()], filename)
    ret, excep = avro.write(group_groups)
    if not ret:
        logger.error('Customer:%s : %s' % (logger.customer, repr(excep)))
        raise SystemExit(1)

    if fixed_date:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], custdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], custdir)
    avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()], filename)
    ret, excep = avro.write(group_endpoints)
    if not ret:
        logger.error('Customer:%s : %s' % (logger.customer, repr(excep)))
        raise SystemExit(1)
