from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import construct_fqdn, remove_non_utf


class ParseLot1ScEndpoints(ParseHelpers):
    def __init__(self, logger, data, uidservendp=False,
                 fetchtype='ServiceGroups', tier=1):
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
        ge = list()
        providers = self.data.get('result', None)

        if providers:
            for provider in providers:
                for service in provider.get('serviceMonitorings', list()):
                    gee = dict()
                    srname = service.get('name', '')

                    sites = service.get('sites', list())
                    if sites:
                        for site in sites:
                            site_name = site.get('name', '')
                            endpoints = site.get('endpoints', [])
                            if endpoints:
                                for endpoint in endpoints:
                                    service_types = endpoint.get('monitoringServiceTypes', [])
                                    if service_types:
                                        for service in service_types:
                                            gee['type'] = self.fetchtype.upper()
                                            gee['group'] = srname
                                            gee['tags'] = dict()
                                            gee['tags']['site_name'] = site_name
                                            gee['tags']['service_name'] = endpoint.get('name', '')
                                            gee['tags']['info_URL'] = endpoint.get('url', '')
                                            gee['service'] = service
                                            gee['hostname'] = construct_fqdn(endpoint.get('url', ''))
                                            ge.append(gee)

        return ge
