from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import construct_fqdn, remove_non_utf
from urllib.parse import urlparse


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
        for feeddata in json_data['results']:
            provider = feeddata['provider']
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

        self._resource_contacts = dict()
        self._parse_data()

    def _parse_data(self):
        if type(self.data) == str:
            json_data = self.parse_json(self.data)
        else:
            json_data = self.data
        for feeddata in json_data['results']:
            resource = feeddata['service']
            if not resource.get('webpage', False):
                continue
            key = '{}+{}'.format(construct_fqdn(resource['webpage']), remove_non_utf(resource['id']))
            contacts = [contact['email'] for contact in resource['publicContacts']]
            if contacts:
                self._resource_contacts[key] = contacts

    def get_contacts(self):
        return self._resource_contacts
