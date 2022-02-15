from argo_egi_connectors.utils import module_class_name
from argo_egi_connectors.log import Logger
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.parse.base import ParseHelpers


class ParseWeights(ParseHelpers):
    def __init__(self, logger, data):
        self.data = data
        self.logger = logger

    def _reformat(self, data):
        datawr = []
        for key in data:
            w = data[key]
            datawr.append({'type': 'computationpower', 'site': key, 'weight': w})
        return datawr

    def get_data(self):
        try:
            weights = dict()
            for ngi in self.parse_json(self.data):
                for site in ngi['site']:
                    key = site['id']
                    if 'ComputationPower' in site:
                        val = site['ComputationPower']
                    else:
                        self.logger.warn(module_class_name(self) + ': No ComputationPower value for NGI:%s Site:%s' % (ngi['ngi'], site['id']))
                        val = '0'
                    weights[key] = val

            return self._reformat(weights)

        except (KeyError, IndexError, ValueError) as exc:
            raise ConnectorParseError()

        except Exception as exc:
            if getattr(self.logger, 'job', False):
                self.logger.error('{} Customer:{} Job:{} : Error - {}'.format(module_class_name(self), self.logger.customer, self.logger.job, repr(exc)))
            else:
                self.logger.error('{} Customer:{} : Error - {}'.format(module_class_name(self), self.logger.customer, repr(exc)))
            raise exc
