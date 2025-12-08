from argo_connectors.config.customer import get_custconf
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import construct_fqdn

import uuid


def build_service_endpoint_id(service_name, service_type):
    if service_name and service_type:
        return str(uuid.uuid3(uuid.NAMESPACE_DNS, service_name + service_type))
    else:
        return 'FALSEID'


class ParseLot1ScEndpoints(ParseHelpers):
    def __init__(self, data, tier, combuid=None):
        self.Customer = get_custconf(combuid)
        self.uidservendp = self.Customer.opt('TopoUIDServiceEndpoints')
        self.tier = tier
        self.data = data
        if type(data) == str:
            self.data = self.parse_json(self.data)
        else:
            self.data = data
        self.gg = list()
        self.ge = list()
        self._service_name_exist = set()
        self._parse()

    def _parse(self):
        self._build_group_endpoints()
        self._build_group_groups()

    def _build_group_groups(self):
        providers = self.data.get('result', None)
        if providers:
            for provider in providers:
                prname = provider.get('providerId', '').replace('/', '-')

                for service in provider.get('serviceMonitorings', list()):
                    gge = dict()
                    srname = service.get('name', '').replace('/', '-')

                    if srname not in self._service_name_exist:
                        continue

                    gge['type'] = self.topo_type('gg')
                    gge['group'] = prname
                    gge['subgroup'] = srname
                    gge['tags'] = dict()
                    gge['tags']['tier'] = self.tier
                    self.gg.append(gge)

    def _build_group_endpoints(self):
        providers = self.data.get('result', None)

        if providers:
            for provider in providers:
                for service in provider.get('serviceMonitorings', list()):
                    srname = service.get('name', '').replace('/', '-')
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
                                            gee = dict()
                                            gee['type'] = self.topo_type('ge')
                                            gee['group'] = srname
                                            gee['tags'] = dict()
                                            gee['tags']['site_name'] = site_name
                                            gee['tags']['service_name'] = endpoint.get('name', '')
                                            gee['tags']['info_URL'] = endpoint.get('url', '')
                                            gee['tags']['tier'] = self.tier
                                            gee['service'] = service
                                            if self.uidservendp:
                                                se_uid = build_service_endpoint_id(endpoint.get('name', ''), service)
                                                gee['tags']['info_ID'] = se_uid
                                                gee['tags']['hostname'] = construct_fqdn(endpoint.get('url', ''))
                                                gee['hostname'] = '{}_{}'.format(construct_fqdn(endpoint.get('url', '')), se_uid)
                                            else:
                                                gee['hostname'] = construct_fqdn(endpoint.get('url', ''))
                                            self.ge.append(gee)
                                            self._service_name_exist.add(srname)

    def get_group_endpoints(self):
        return self.ge

    def get_group_groups(self):
        return self.gg
