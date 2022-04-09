from argo_egi_connectors.parse.base import ParseHelpers
from urllib.parse import urlparse


def construct_fqdn(http_endpoint):
    return urlparse(http_endpoint).netloc


class ParseProvidersContacts(ParseHelpers):
    def __init__(self, logger, data):
        self.logger = logger
        self.data = data

        self._provider_contacts = list()
        self._parse_data()

    def _parse_data(self):
        if type(self.data) == str:
            json_data = self.parse_json(self.data)
        else:
            json_data = self.data
        for provider in json_data['results']:
            key = provider['abbreviation']
            contacts = [contact['email'] for contact in provider['publicContacts']]
            if contacts:
                self._provider_contacts.append({
                    'name': key,
                    'contacts': contacts
                })

    def get_contacts(self):
        return self._provider_contacts


class ParseResourcesContacts(ParseHelpers):
    def __init__(self, logger, data):
        self.logger = logger
        self.data = data

        self._resource_contacts = list()
        self._parse_data()

    def _parse_data(self):
        if type(self.data) == str:
            json_data = self.parse_json(self.data)
        else:
            json_data = self.data
        for resource in json_data['results']:
            key = '{}+{}'.format(construct_fqdn(resource['webpage']),
                                 resource['id'])
            contacts = [contact['email'] for contact in resource['publicContacts']]
            if contacts:
                self._resource_contacts.append({
                    'name': key,
                    'contacts': contacts
                })

    def get_contacts(self):
        return self._resource_contacts
