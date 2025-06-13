from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import construct_fqdn


class ParseLot1ScEndpoints(ParseHelpers):
    def __init__(self, logger, data, uidservendp=False,
                 fetchtype='ServiceGroups'):
        self.uidservendp = uidservendp
        self.fetchtype = fetchtype
        self.logger = logger

    def get_group_groups(self):
        pass

    def get_group_endpoints(self):
        pass
