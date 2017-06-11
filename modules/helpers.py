import datetime
import os
import re

strerr = ''
num_excp_expand = 0
daysback = 1

def error_message(exception):
    global strerr, num_excp_expand
    if isinstance(exception, Exception) and getattr(exception, 'args', False):
        num_excp_expand += 1
        if not error_message(exception.args):
            return strerr
    elif isinstance(exception, dict):
        for s in exception.iteritems():
            error_message(s)
    elif isinstance(exception, list):
        for s in exception:
            error_message(s)
    elif isinstance(exception, tuple):
        for s in exception:
            error_message(s)
    elif isinstance(exception, str):
        if num_excp_expand <= 1:
            strerr += exception + ' '

def gen_fname_timestamp(daysback):
    dateback = datetime.datetime.now() - datetime.timedelta(days=daysback)
    return str(dateback.strftime('%Y_%m_%d'))

def gen_fname_repdate(logger, option, path, datestamp=None):
    datestamp = datestamp if datestamp else gen_fname_timestamp(daysback)
    if re.search(r'DATE(.\w+)$', option):
        filename = path + re.sub(r'DATE(.\w+)$', r'%s\1' % datestamp, option)
    else:
        logger.error('No DATE placeholder in %s' % option)
        raise SystemExit(1)

    return filename

def module_class_name(obj):
    name = repr(obj.__class__.__name__)
    return name.replace("'",'')

