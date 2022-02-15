from urllib.parse import urlparse
from argo_egi_connectors.utils import filename_date, module_class_name
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.parse.base import ParseHelpers


def construct_fqdn(http_endpoint):
    return urlparse(http_endpoint).netloc


class ParseContacts(ParseHelpers):
    def __init__(self, logger, data, uidservtype=False, is_csv=False):
        self.logger = logger
        self.uidservtype = uidservtype
        if is_csv:
            self.data = self.csv_to_json(data)
        else:
            self.data = self.parse_json(data)

    def get_contacts(self):
        contacts = list()

        for entity in self.data:
            if self.uidservtype:
                key = '{}_{}+{}'.format(construct_fqdn(entity['URL']), entity['Service Unique ID'], entity['SERVICE_TYPE'])
            else:
                key = '{}+{}'.format(construct_fqdn(entity['URL']), entity['SERVICE_TYPE'])

            value = entity['CONTACT_EMAIL']
            contacts.append({
                'name': key,
                'contacts': [value] if not type(value) == list else value
            })

        return contacts


class ParseFlatEndpoints(ParseHelpers):
    def __init__(self, logger, data, project, uidservtype=False,
                 fetchtype='ServiceGroups', is_csv=False, scope=None):
        self.uidservtype = uidservtype
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
                if self.uidservtype:
                    tmp_dict['hostname'] = '{1}_{0}'.format(entity['Service Unique ID'], construct_fqdn(info_url))
                else:
                    tmp_dict['hostname'] = construct_fqdn(entity['URL'])

                tmp_dict['tags'] = {'scope': self.project,
                                    'monitored': '1',
                                    'info_URL': info_url,
                                    'hostname': construct_fqdn(entity['URL'])}

                tmp_dict['tags'].update({'info_ID': str(entity['Service Unique ID'])})
                groups.append(tmp_dict)

            return groups

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            feedtype = 'CSV' if self.is_csv else 'JSON'
            msg = 'Customer:%s : Error parsing %s feed - %s' % (self.logger.customer, feedtype, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)
