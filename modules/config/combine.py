import yaml
import os

from argo_connectors.exceptions import ConnectorConfError
from argo_connectors.log import Logger


class CombineConf(object):
    def __init__(self, caller, confpath, **kwargs):
        self.confpath = confpath
        self.caller = caller

    def parse(self):
        if not os.path.exists(self.confpath):
            Logger.error('Could not find %s' % self.confpath)
            raise ConnectorConfError('Could not find %s' % self.confpath)

        yaml_data = None
        with open(self.confpath, 'r') as file:
            yaml_data = yaml.safe_load(file)

        target_combine = list()
        for data in yaml_data:
            combine_type = data.get('type', None)
            if combine_type in self.caller:
                target_combine.append(data)

        return target_combine
