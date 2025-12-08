from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.log import Logger
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import module_class_name


class ParseWeights(ParseHelpers):
    def __init__(self, data):
        self.data = data

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
                        Logger.warn(module_class_name(self) + ': No ComputationPower value for NGI:%s Site:%s' % (ngi['ngi'], site['id']))
                        val = '0'
                    weights[key] = val

            return self._reformat(weights)

        except (KeyError, IndexError, ValueError) as exc:
            raise ConnectorParseError()

        except Exception as exc:
            if getattr(Logger, 'job', False):
                Logger.error('{} Customer:{} Job:{} : Error - {}'.format(module_class_name(self), Logger.customer, Logger.job, repr(exc)))
            else:
                Logger.error('{} Customer:{} : Error - {}'.format(module_class_name(self), Logger.customer, repr(exc)))
            raise exc
