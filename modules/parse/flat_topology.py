from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import  construct_fqdn


class ParseFlatEndpoints(ParseHelpers):
    def __init__(self, logger, data, project, uidservendp=False,
                 fetchtype='ServiceGroups', is_csv=False, scope=None):
        self.uidservendp = uidservendp
        self.fetchtype = fetchtype
        self.logger = logger
        self.project = project
        self.is_csv = is_csv
        self.scope = scope if scope else project
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

                tmp_dict['type'] = 'PROJECT'
                tmp_dict['group'] = self.project
                tmp_dict['subgroup'] = entity['SITENAME-SERVICEGROUP']
                tmp_dict['tags'] = {'monitored': '1', 'scope': self.scope}

                if tmp_dict['subgroup'] in already_added:
                    continue
                else:
                    groups.append(tmp_dict)
                already_added.append(tmp_dict['subgroup'])

            return groups

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            feedtype = 'CSV' if self.is_csv else 'JSON'
            msg = 'Customer:%s : Error parsing %s feed - %s' % (self.logger.customer, feedtype, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

    def get_groupendpoints(self):
        try:
            groups = list()

            for entity in self.data:
                tmp_dict = dict()

                tmp_dict['type'] = self.fetchtype.upper()
                tmp_dict['group'] = entity['SITENAME-SERVICEGROUP']
                tmp_dict['service'] = entity['SERVICE_TYPE']
                info_url = entity['URL']
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
            msg = 'Customer:%s : Error parsing %s feed - %s' % (self.logger.customer, feedtype, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)
