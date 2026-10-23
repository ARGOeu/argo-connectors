from argo_connectors.config.customer import get_custconf
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.log import Logger
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import construct_fqdn


class ParseFlatEndpoints(ParseHelpers):
    def __init__(self, data, is_csv=False, combuid=None):
        self.Customer = get_custconf(combuid)
        self.uidservendp = self.Customer.opt('TopoUIDServiceEndpoints')
        self.project = self.Customer.get_custname()
        self.scope = self.Customer.get_custname()
        self.is_csv = is_csv
        try:
            if is_csv:
                self.data = self.csv_to_json(data)
            else:
                self.data = self.parse_json(data)

        except ConnectorParseError as exc:
            raise exc

    def get_groupgroups(self):
        try:
            groups = list()
            already_added = list()

            for entity in self.data:
                tmp_dict = dict()

                tmp_dict['type'] = self.topo_type('gg')
                tmp_dict['group'] = self.project
                subgroup = entity['SITENAME-SERVICEGROUP']
                if subgroup:
                    tmp_dict['subgroup'] = subgroup
                else:
                    continue
                tmp_dict['tags'] = {'monitored': '1', 'scope': self.scope}

                if tmp_dict['subgroup'] in already_added:
                    continue
                else:
                    groups.append(tmp_dict)
                already_added.append(tmp_dict['subgroup'])

            return groups

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            feedtype = 'CSV' if self.is_csv else 'JSON'
            msg = 'Customer:%s : Error parsing %s feed - %s' % (Logger.customer, feedtype, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

    def get_groupendpoints(self):
        try:
            groups = list()

            for entity in self.data:
                tmp_dict = dict()

                tmp_dict['type'] = self.topo_type('ge')
                group = entity['SITENAME-SERVICEGROUP']
                if group:
                    tmp_dict['group'] = group
                else:
                    continue
                service = entity['SERVICE_TYPE']
                if service:
                    tmp_dict['service'] = service
                else:
                    continue
                url = entity['URL']
                if url:
                    info_url = url
                else:
                    continue
                if self.uidservendp:
                    tmp_dict['hostname'] = '{1}_{0}'.format(entity['Service Unique ID'], construct_fqdn(info_url))
                else:
                    tmp_dict['hostname'] = construct_fqdn(entity['URL'])

                tmp_dict['tags'] = {'scope': self.project,
                                    'monitored': '1',
                                    'info_URL': info_url}
                if self.uidservendp:
                    tmp_dict['tags'].update({'hostname': construct_fqdn(entity['URL'])})

                tmp_dict['tags'].update({'info_ID': str(entity['Service Unique ID'])})
                groups.append(tmp_dict)

            return groups

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            feedtype = 'CSV' if self.is_csv else 'JSON'
            msg = 'Customer:%s : Error parsing %s feed - %s' % (Logger.customer, feedtype, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)
