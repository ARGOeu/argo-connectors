from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import construct_fqdn


class ParseFlatEndpoints(ParseHelpers):
    def __init__(self, logger, data, project, uidservendp=False,
                 fetchtype='ServiceGroups'):
        self.uidservendp = uidservendp
        self.fetchtype = fetchtype
        self.logger = logger
        self.project = project

    def get_groupgroups(self):
        pass

    def get_groupendpoints(self):
        pass
