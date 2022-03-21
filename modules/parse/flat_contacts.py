from argo_egi_connectors.parse.base import ParseHelpers
from urllib.parse import urlparse


def construct_fqdn(http_endpoint):
    return urlparse(http_endpoint).netloc


class ParseContacts(ParseHelpers):
    def __init__(self, logger, data, uidservendp=False, is_csv=False):
        self.logger = logger
        self.uidservendp = uidservendp
        if is_csv:
            self.data = self.csv_to_json(data)
        else:
            self.data = self.parse_json(data)

    def get_contacts(self):
        contacts = list()

        for entity in self.data:
            if self.uidservendp:
                key = '{}_{}+{}'.format(construct_fqdn(entity['URL']), entity['Service Unique ID'], entity['SERVICE_TYPE'])
            else:
                key = '{}+{}'.format(construct_fqdn(entity['URL']), entity['SERVICE_TYPE'])

            value = entity['CONTACT_EMAIL']
            contacts.append({
                'name': key,
                'contacts': [value] if not type(value) == list else value
            })

        return contacts
