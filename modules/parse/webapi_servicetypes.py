from urllib.parse import urlparse
from argo_connectors.utils import filename_date, module_class_name
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers


class ParseWebApiServiceTypes(ParseHelpers):
    def __init__(self, logger, data):
        self.data = data
        self.logger = logger
        self.service_types = self._parse()

    def _parse(self):
        all_service_type = list()

        try:
            service_types = self.parse_json(self.data)['data']
            for st in service_types:
                all_service_type.append({
                    'name': st['name'],
                    'description': st['description'],
                    'tags': st['tags'] if st.get('tags', False) else ['poem']
                })

            return sorted(all_service_type,  key=lambda s: s['name'].lower())

        except (KeyError, IndexError, AttributeError, TypeError, AssertionError) as exc:
            msg = '{} Customer:{} : Error parsing service types feed - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc

    def get_data(self, tag=None):
        if not tag:
            return self.service_types
        else:
            return list(filter(lambda st: tag in st['tags'] , self.service_types))
