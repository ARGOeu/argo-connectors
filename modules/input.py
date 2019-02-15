import base64
import json
import requests
import socket
import xml.dom.minidom

from argo_egi_connectors.helpers import retry

from ssl import SSLError
from xml.parsers.expat import ExpatError
from urlparse import urlparse


class ConnectorError(Exception):
    pass


@retry
def connection(logger, msgprefix, globopts, scheme, host, url, custauth=None):
    try:
        buf = None

        headers = {}
        if custauth and eval(custauth['AuthenticationUsePlainHttpAuth'.lower()]):
            userpass = base64.b64encode(custauth['AuthenticationHttpUser'.lower()] + ':' \
                                        + custauth['AuthenticationHttpPass'.lower()])
            headers={'Authorization': 'Basic ' + userpass}
        elif custauth and msgprefix == 'PoemReader':
            headers = {'x-api-key': custauth['AuthenticationPoemToken'.lower()]}

        if scheme.startswith('https'):
            response = requests.get('https://'+ host + url, headers=headers,
                                    cert=(globopts['AuthenticationHostCert'.lower()],
                                          globopts['AuthenticationHostKey'.lower()]),
                                    verify=eval(globopts['AuthenticationVerifyServerCert'.lower()]),
                                    timeout=int(globopts['ConnectionTimeout'.lower()]))
            response.raise_for_status()
        else:
            response = requests.get('http://'+ host + url, headers=headers,
                                    timeout=int(globopts['ConnectionTimeout'.lower()]))


        if response.status_code >= 300 and response.status_code < 400:
            headers = response.headers
            location = filter(lambda h: 'location' in h[0], headers)
            if location:
                redir = urlparse(location[0][1])
            else:
                raise requests.exceptions.RequestException('No Location header set for redirect')

            return connection(logger, msgprefix, globopts, scheme, redir.netloc, redir.path + '?' + redir.query, custauth=custauth)

        elif response.status_code == 200:
            buf = response.content
            if not buf:
                raise requests.exceptions.RequestException('Empty response')

        else:
            raise requests.exceptions.RequestException('response: %s %s' % (response.status_code, response.reason))

        return buf

    except SSLError as e:
        if (getattr(e, 'args', False) and type(e.args) == tuple
            and type(e.args[0]) == str
            and 'timed out' in e.args[0]):
            raise e
        else:
            logger.critical('%sCustomer:%s Job:%s SSL Error %s - %s' % (msgprefix + ' ' if msgprefix else '',
                                                                        logger.customer, logger.job,
                                                                        scheme + '://' + host + url,
                                                                        repr(e)))
        return False

    except(socket.error, socket.timeout) as e:
        logger.warn('%sCustomer:%s Job:%s Connection error %s - %s' % (msgprefix + ' ' if msgprefix else '',
                                                                       logger.customer, logger.job,
                                                                       scheme + '://' + host + url,
                                                                       repr(e)))
        raise e

    except requests.exceptions.RequestException as e:
        logger.warn('%sCustomer:%s Job:%s HTTP error %s - %s' % (msgprefix + ' ' if msgprefix else '',
                                                                 logger.customer, logger.job,
                                                                 scheme + '://' + host + url,
                                                                 repr(e)))
        raise e

    except Exception as e:
        logger.critical('%sCustomer:%s Job:%s Error %s - %s' % (msgprefix + ' ' if msgprefix else '',
                                                                logger.customer, logger.job,
                                                                scheme + '://' + host + url,
                                                                repr(e)))
        return False


def parse_xml(logger, objname, globopts, buf, method):
    try:
        doc = xml.dom.minidom.parseString(buf)

    except ExpatError as e:
        logger.error(objname + ' Customer:%s Job:%s : Error parsing XML feed %s - %s' % (logger.customer, logger.job, method, repr(e)))
        raise ConnectorError()

    except Exception as e:
        logger.error(objname + ' Customer:%s Job:%s : Error %s - %s' % (logger.customer, logger.job, method, repr(e)))
        raise e

    else:
        return doc


def parse_json(logger, objname, globopts, buf, method):
    try:
        doc = json.loads(buf)

    except ValueError as e:
        logger.error(objname + ' Customer:%s Job:%s : Error parsing JSON feed %s - %s' % (logger.customer, logger.job, method, repr(e)))
        raise ConnectorError()

    except Exception as e:
        logger.error(objname + ' Customer:%s Job:%s : Error %s - %s' % (logger.customer, logger.job, method, repr(e)))
        raise e

    else:
        return doc
