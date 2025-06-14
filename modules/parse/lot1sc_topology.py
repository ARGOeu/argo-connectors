from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import construct_fqdn


class ParseLot1ScEndpoints(ParseHelpers):
    def __init__(self, logger, data, uidservendp=False,
                 fetchtype='ServiceGroups'):
        self.uidservendp = uidservendp
        self.fetchtype = fetchtype
        self.logger = logger
        self.data = data
        if type(data) == str:
            self.data = self.parse_json(self.data)
        else:
            self.data = data

    def get_group_groups(self):
        gg = list()
        providers = self.data.get('result', None)
        if providers:
            for provider in providers:
                gge = dict()
                prname = provider.get('providerId', '')

                for service in provider.get('serviceMonitorings', list()):
                    srname = service.get('name', '')

                    gge['type'] = 'PROJECT'
                    gge['group'] = prname
                    gge['subgroup'] = srname
                    gge['tags'] = dict()
                    gg.append(gge)

        return gg

    def get_group_endpoints(self):
        pass
