import argparse
import os

from argo_connectors.config.customer import Customer
from argo_connectors.config.glob import Global
from argo_connectors.log import Logger
from argo_connectors.utils import date_check
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError, ConnectorConfError


class ExecConnector:
    def __init__(self, description, initial_arg=False, initial_arg_help='',
                 exe_script='', date_required=False):
        self.description = description
        self.initial_arg = initial_arg
        self.initial_arg_help = initial_arg_help
        self.exe_script = exe_script
        self.args = None
        self.fixed_date = None
        self.logger = None
        self.config_customer = None
        self.config_global = None
        self.date_required = date_required
        self._main()

    def _setargs(self):
        parser = argparse.ArgumentParser(description=self.description)
        parser.add_argument('-c', dest='custconf', metavar='customer.conf',
                            default=None, help='path to customer configuration file',
                            type=str, required=False)
        if self.initial_arg:
            parser.add_argument('--initial', dest='initsync',
                                help=self.initial_arg_help,
                                action='store_true', default=False,
                                required=False)
        parser.add_argument('-g', dest='gloconf', metavar='global.conf',
                            default=None, help='path to global configuration file',
                            type=str, required=False)
        parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY',
                            help='write data for this date', type=str, required=self.date_required)
        self.args = args = parser.parse_args()

        if args.date and date_check(args.date):
            self.fixed_date = args.date

    def _main(self):
        self._setargs()
        self._logger = Logger(os.path.basename(self.exe_script))

        try:
            globopts = Global(self.exe_script, self.args.gloconf).options()
            confcust = Customer(self.exe_script, self.args.custconf)
            confcust.valid()
            self.config_customer = confcust
            self.config_global = globopts

        except ConnectorConfError as exc:
            self._logger.error(exc)
            raise SystemExit(1)

        confcust.make_dirstruct()
        confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
        self._logger.customer = confcust.get_custname()
