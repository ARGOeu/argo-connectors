import argparse
import os

from argo_connectors.log import Logger
from argo_connectors.config.combine import CombineConf
from argo_connectors.config.glob import Global
from argo_connectors.config.customer import CombinerCustomer
from argo_connectors.exceptions import ConnectorConfError


class ExecCombiner:
    def __init__(self, description, exe_script='', combiner=''):
        self.description = description
        self.combiner = combiner
        self.exe_script = exe_script
        self.args = None
        self._logger = None
        self.tasks = list()
        self.globopts = None
        self.tenant_name = None
        self._main()

    def _setargs(self):
        parser = argparse.ArgumentParser(description=self.description)
        parser.add_argument('-c', dest='yamlconf', metavar='combine.yml',
                            help='path to YAML file', type=str, required=True)
        self.args = parser.parse_args()

    def _main(self):
        self._setargs()

        self._logger = Logger(os.path.basename(self.exe_script))

        try:
            combopts = CombineConf(self.exe_script, self.args.yamlconf).parse()

        except ConnectorConfError as exc:
            self._logger.error(exc)
            raise SystemExit(1)

        for comb in combopts:
            try:
                comb_globopts = comb.get('config', None)
                self.globopts = Global(self.exe_script)
                self.tenant_name = comb['tenant']
                if comb_globopts:
                    self.globopts.configure(comb_globopts)
                confs = comb.get('combine')
                n = 1
                if confs:
                    for conf in confs:
                        which = conf.get('type', None)
                        if not which:
                            raise ConnectorConfError(f'type is mandatory in {self.combiner} combine')
                        combuid = f'{n}-{which}'
                        confcust = CombinerCustomer(self.exe_script, combuid, self.tenant_name)
                        confcust.configure(conf)
                        confcust.valid()
                        confcust.make_dirstruct(jobdir=False)
                        confcust.make_dirstruct(self.globopts.options()['InputStateSaveDir'.lower()], jobdir=False)
                        self._logger.customer = self.tenant_name
                        key = which.lower()
                        self.tasks.append({
                            'type': key,
                            'id': combuid
                        })
                        n += 1
                else:
                    raise ConnectorConfError('combine key mandatory')

            except ConnectorConfError as exc:
                self._logger.error(exc)
                raise SystemExit(1)
