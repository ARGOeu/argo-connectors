import datetime
import re
from urllib.parse import urlparse
from unidecode import unidecode

strerr = ''
num_excp_expand = 0
daysback = 1


def date_check(arg):
    if re.search("[0-9]{4}-[0-9]{2}-[0-9]{2}", arg):
        return True
    else:
        return False


def construct_fqdn(http_endpoint):
    hostname = urlparse(http_endpoint).netloc
    if type(hostname) == bytes:
        hostname = hostname.decode('utf-8')
    if ':' in hostname:
        hostname = hostname.split(':')[0]
    return hostname


def datestamp(daysback=None):
    if daysback:
        dateback = datetime.datetime.now() - datetime.timedelta(days=daysback)
    else:
        dateback = datetime.datetime.now()

    return str(dateback.strftime('%Y_%m_%d'))


def filename_date(logger, option, path, stamp=None):
    stamp = stamp if stamp else datestamp(daysback)
    filename = path + re.sub(r'DATE(.\w+)$', r'%s\1' % stamp, option)

    return filename


def module_class_name(obj):
    name = repr(obj.__class__.__name__)

    return name.replace("'", '')


def remove_non_utf(string):
    if '+' in string:
        string = string.replace("+", '_plus_')
    
    if '@' in string:
        string = string.replace('@', '_at_')

    if ' ' in string:
        string = string.replace(' ', '_')

    if '"' in string:
        string = string.replace('"', '')

    return unidecode(string)