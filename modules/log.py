import logging
import logging.handlers
import sys
import socket
import os


class Logger:
    def __init__(self, connector, tenant_name):
        lfs = '%(name)s[%(process)s]: %(levelname)s %(message)s'
        logformat = logging.Formatter(lfs)
        logverbose = logging.INFO
        self.connector = connector
        self.tenant_name = tenant_name

        LOGFILE = f"/var/log/argo-connectors/{self.tenant_name}/connectors.log"

        if self.tenant_name is not None and not os.path.exists(LOGFILE):
            os.makedirs(f'/var/log/argo-connectors/{self.tenant_name}/')

        logging.basicConfig(format=lfs, level=logging.INFO, stream=sys.stdout)
        self.logger = logging.getLogger(connector)

        try:
            sysloghandle = logging.handlers.SysLogHandler('/dev/log', logging.handlers.SysLogHandler.LOG_USER)
        except socket.error:
            sysloghandle = logging.StreamHandler()
        sysloghandle.setFormatter(logformat)
        sysloghandle.setLevel(logverbose)
        self.logger.addHandler(sysloghandle)

        try:
            lffs = '%(asctime)s %(name)s[%(process)s]: %(levelname)s %(message)s'
            lff = logging.Formatter(lffs)
            filehandle = logging.handlers.RotatingFileHandler(LOGFILE, maxBytes=512*1024, backupCount=5)
            filehandle.setFormatter(lff)
            filehandle.setLevel(logverbose)
            self.logger.addHandler(filehandle)
        except Exception:
            pass

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
