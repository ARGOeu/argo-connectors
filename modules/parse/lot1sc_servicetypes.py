from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.log import Logger
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import module_class_name


class ParseLot1ScServiceTypes(ParseHelpers):
    def __init__(self, data):
        self.data = data
        if type(data) == str:
            self.data = self.parse_json(data)

    def get_data(self):
        try:
            service_types = set()

            for provider in self.data.get('result', list()):
                for service in provider.get('serviceMonitorings', list()):
                    for site in service.get('sites', list()):
                        for endpoint in site.get('endpoints', list()):
                            for service_type in endpoint.get('monitoringServiceTypes', list()):
                                if service_type:
                                    service_types.add(service_type)

            return [
                {
                    'name': service_type,
                    'description': '',
                    'tags': ['topology']
                }
                for service_type in sorted(service_types, key=lambda s: s.lower())
            ]

        except (KeyError, IndexError, AttributeError, TypeError, AssertionError) as exc:
            msg = '{} Customer:{} : Error parsing service types feed - {}'.format(
                module_class_name(self), Logger.customer, repr(exc))
            raise ConnectorParseError(msg)
