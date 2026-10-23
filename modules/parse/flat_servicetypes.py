from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.log import Logger
from argo_connectors.parse.base import ParseHelpers


class ParseFlatServiceTypes(ParseHelpers):
    def __init__(self, data, is_csv=False):
        self.data = data
        self.is_csv = is_csv
        try:
            if is_csv:
                self.data = self.csv_to_json(data)
            else:
                self.data = self.parse_json(data)

        except ConnectorParseError as exc:
            raise exc

    def get_data(self):
        try:
            services = list()
            already_added = list()

            for entity in self.data:
                target_key = None
                tmp_dict = dict()

                service_type = entity['SERVICE_TYPE']
                if service_type:
                    tmp_dict['name'] = service_type
                else:
                    continue
                for key in entity.keys():
                    if key.lower().startswith('Service Description'.lower()):
                        target_key = key
                tmp_dict['description'] = entity[target_key]
                tmp_dict['tags'] = ['topology']

                if tmp_dict['name'] in already_added:
                    continue
                else:
                    services.append(tmp_dict)
                already_added.append(tmp_dict['name'])

            return services

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            feedtype = 'CSV' if self.is_csv else 'JSON'
            msg = 'Customer:%s : Error parsing %s feed - %s' % (Logger.customer, feedtype, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)
