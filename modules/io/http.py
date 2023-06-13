import ssl
import asyncio
import aiohttp
import random

from aiohttp import client_exceptions, http_exceptions, ClientSession
from argo_connectors.utils import module_class_name
from argo_connectors.exceptions import ConnectorHttpError


def build_ssl_settings(globopts):
    try:
        sslcontext = ssl.create_default_context(capath=globopts['AuthenticationCAPath'.lower()],
                                                cafile=globopts['AuthenticationCAFile'.lower()])
        sslcontext.load_cert_chain(globopts['AuthenticationHostCert'.lower()],
                                   globopts['AuthenticationHostKey'.lower()])

        return sslcontext

    except KeyError:
        return None

def build_connection_retry_settings(globopts):
    retry = int(globopts['ConnectionRetry'.lower()])
    timeout = int(globopts['ConnectionTimeout'.lower()])
    return (retry, timeout)


class SessionWithRetry(object):
    def __init__(self, logger, msgprefix, globopts, token=None, custauth=None,
                 verbose_ret=False, handle_session_close=False):
        self.ssl_context = build_ssl_settings(globopts)
        n_try, client_timeout = build_connection_retry_settings(globopts)
        client_timeout = aiohttp.ClientTimeout(total=client_timeout,
                                               connect=client_timeout, sock_connect=client_timeout,
                                               sock_read=client_timeout)
        self.session = ClientSession(timeout=client_timeout)
        self.n_try = n_try
        self.logger = logger
        self.token = token
        if custauth:
            self.custauth = aiohttp.BasicAuth(
                custauth['AuthenticationHttpUser'.lower()],
                custauth['AuthenticationHttpPass'.lower()]
            )
        else:
            self.custauth = None
        self.verbose_ret = verbose_ret
        self.handle_session_close = handle_session_close
        self.globopts = globopts
        self.erroneous_statuses = [404]

    async def _http_method(self, method, url, data=None, headers=None):
        method_obj = getattr(self.session, method)
        raised_exc = None
        n = 1
        if self.token:
            headers = headers or {}
            headers.update({
                'x-api-key': self.token,
                'Accept': 'application/json'
            })
        try:
            connct_rty_rnd = self.globopts['ConnectionRetryRandom'.lower()]
            if connct_rty_rnd == 'True':
                sleepsecs =  float(random.randint(0, float(self.globopts['ConnectionSleepRandomRetryMax'.lower()])))
            else:
                sleepsecs = float(self.globopts['ConnectionSleepRetry'.lower()])

            while n <= self.n_try:
                if n > 1:
                    if getattr(self.logger, 'job', False):
                        self.logger.info(f"{module_class_name(self)} Customer:{self.logger.customer} Job:{self.logger.job} : HTTP Connection try - {n} after sleep {sleepsecs} seconds")
                    else:
                        self.logger.info(f"{module_class_name(self)} Customer:{self.logger.customer} : HTTP Connection try - {n} after sleep {sleepsecs} seconds")
                try:
                    async with method_obj(url, data=data, headers=headers,
                                          ssl=self.ssl_context, auth=self.custauth) as response:
                        if response.status in self.erroneous_statuses:
                            if getattr(self.logger, 'job', False):
                                self.logger.error('{}.http_{}({}) Customer:{} Job:{} - Erroneous HTTP status: {} {}'.\
                                                  format(module_class_name(self),
                                                         method, url,
                                                         self.logger.customer,
                                                         self.logger.job,
                                                         response.status,
                                                         response.reason))
                            else:
                                self.logger.error('{}.http_{}({}) Customer:{} - Erroneus HTTP status: {} {}'.\
                                                  format(module_class_name(self),
                                                         method, url,
                                                         self.logger.customer,
                                                         response.status,
                                                         response.reason))
                            break
                        content = await response.text()
                        if content:
                            if self.verbose_ret:
                                return (content, response.headers, response.status)
                            return content

                        if getattr(self.logger, 'job', False):
                            self.logger.warn("{} Customer:{} Job:{} : HTTP Empty response".format(module_class_name(self),
                                                                                                  self.logger.customer, self.logger.job))
                        else:
                            self.logger.warn("{} Customer:{} : HTTP Empty response".format(module_class_name(self),
                                                                                           self.logger.customer))

                # do not retry on SSL errors
                # raise exc that will be handled in outer try/except clause
                except ssl.SSLError as exc:
                    raise exc

                # retry on client errors
                except (client_exceptions.ClientError,
                        client_exceptions.ServerTimeoutError,
                        asyncio.TimeoutError) as exc:
                    if getattr(self.logger, 'job', False):
                        self.logger.error('{}.http_{}({}) Customer:{} Job:{} - {}'.format(module_class_name(self),
                                                                                          method, url, self.logger.customer,
                                                                                          self.logger.job, repr(exc)))
                    else:
                        self.logger.error('{}.http_{}({}) Customer:{} - {}'.format(module_class_name(self),
                                                                                   method, url, self.logger.customer,
                                                                                   repr(exc)))
                    raised_exc = exc

                # do not retry on HTTP protocol errors
                # raise exc that will be handled in outer try/except clause
                except (http_exceptions.HttpProcessingError) as exc:
                    if getattr(self.logger, 'job', False):
                        self.logger.error('{}.http_{}({}) Customer:{} Job:{} - {}'.format(module_class_name(self),
                                                                                          method, url, self.logger.customer,
                                                                                          self.logger.job, repr(exc)))
                    else:
                        self.logger.error('{}.http_{}({}) Customer:{} - {}'.format(module_class_name(self),
                                                                                   method, url, self.logger.customer,
                                                                                   repr(exc)))
                    raise exc

                await asyncio.sleep(sleepsecs)
                n += 1

            else:
                if getattr(self.logger, 'job', False):
                    self.logger.info("{} Customer:{} Job:{} : HTTP Connection retry exhausted".format(module_class_name(self),
                                                                                                      self.logger.customer, self.logger.job))
                else:
                    self.logger.info("{} Customer:{} : HTTP Connection retry exhausted".format(module_class_name(self),
                                                                                               self.logger.customer))
                raise raised_exc

        except Exception as exc:
            if getattr(self.logger, 'job', False):
                self.logger.error('{}.http_{}({}) Customer:{} Job:{} - {}'.format(module_class_name(self),
                                                                                  method, url, self.logger.customer,
                                                                                  self.logger.job, repr(exc)))
            else:
                self.logger.error('{}.http_{}({}) Customer:{} - {}'.format(module_class_name(self),
                                                                           method, url, self.logger.customer,
                                                                           repr(exc)))
            raise exc

        finally:
            if not self.handle_session_close:
                await self.session.close()

    async def http_get(self, url, headers=None):
        try:
            content = await self._http_method('get', url, headers=headers)
            return content

        except Exception as exc:
            raise ConnectorHttpError(repr(exc)) from exc

    async def http_put(self, url, data, headers=None):
        try:
            content = await self._http_method('put', url, data=data,
                                              headers=headers)
            return content

        except Exception as exc:
            raise ConnectorHttpError(repr(exc)) from exc

    async def http_post(self, url, data, headers=None):
        try:
            content = await self._http_method('post', url, data=data,
                                              headers=headers)
            return content

        except Exception as exc:
            raise ConnectorHttpError(repr(exc)) from exc

    async def http_delete(self, url, headers=None):
        try:
            content = await self._http_method('delete', url, headers=headers)
            return content

        except Exception as exc:
            raise ConnectorHttpError(repr(exc)) from exc


    async def close(self):
        return await self.session.close()
