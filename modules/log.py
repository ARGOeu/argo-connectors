import logging
import logging.handlers
import sys
import socket
import os
import re


def _logfile(tenant=None):
    basename = 'connectors.log'

    if tenant:
        safe_tenant = re.sub(r'[^A-Za-z0-9_.-]+', '-', tenant).lower()
        basename = f'connectors-{safe_tenant}.log'
    return f"{os.environ['VIRTUAL_ENV']}/var/log/{basename}"


LOGFILE = _logfile()


class _Logger:
    def __init__(self, connector):
        lfs = '%(name)s[%(process)s]: %(levelname)s %(message)s'
        logformat = logging.Formatter(lfs)
        logverbose = logging.INFO
        self.connector = connector

        logging.basicConfig(format=lfs, level=logging.INFO, stream=sys.stdout)
        self.logger = logging.getLogger(connector)
        for handler in list(self.logger.handlers):
            if getattr(handler, '_argo_connectors_syslog', False):
                self.logger.removeHandler(handler)
                handler.close()

        try:
            sysloghandle = logging.handlers.SysLogHandler('/dev/log', logging.handlers.SysLogHandler.LOG_USER)
        except socket.error:
            sysloghandle = logging.StreamHandler()
        sysloghandle.setFormatter(logformat)
        sysloghandle.setLevel(logverbose)
        sysloghandle._argo_connectors_syslog = True
        self.logger.addHandler(sysloghandle)

        self._set_filehandler(LOGFILE, logverbose)

    def _set_filehandler(self, logfile, loglevel=logging.INFO):
        for handler in list(self.logger.handlers):
            if getattr(handler, '_argo_connectors_filelog', False):
                self.logger.removeHandler(handler)
                handler.close()

        try:
            lffs = '%(asctime)s %(name)s[%(process)s]: %(levelname)s %(message)s'
            lff = logging.Formatter(lffs)
            filehandle = logging.handlers.RotatingFileHandler(logfile, maxBytes=512 * 1024, backupCount=5)
            filehandle.setFormatter(lff)
            filehandle.setLevel(loglevel)
            filehandle._argo_connectors_filelog = True
            self.logger.addHandler(filehandle)
        except Exception:
            pass

    def set_filelog(self, tenant_logs=False, tenant=None):
        logfile = _logfile(tenant if tenant_logs else None)
        self._set_filehandler(logfile)

    def __call__(self, connector):
        self.__init__(connector)
        return self

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['logger']
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.logger = logging.getLogger(self.connector)

    def warn(self, msg):
        self.logger.warning(msg)

    def critical(self, msg):
        self.logger.critical(msg)

    def error(self, msg):
        self.logger.error(msg)

    def info(self, msg):
        self.logger.info(msg)


Logger = _Logger('config/log.py')
