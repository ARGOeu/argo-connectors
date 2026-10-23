from argo_connectors.parse.base import ParseHelpers
from argo_connectors.parse.provider_topology import unwrap_provider, unwrap_resource
from argo_connectors.utils import construct_fqdn, remove_non_utf


def contact_emails(contacts):
    emails = []
    for contact in contacts or []:
        if isinstance(contact, dict):
            email = contact.get('email')
        else:
            email = contact
        if email:
            emails.append(email)
    return emails


class ParseProvidersContacts(ParseHelpers):
    def __init__(self, data):
        self.data = data

        self._provider_contacts = list()
        self._parse_data()

    def _parse_data(self):
        if type(self.data) == str:
            json_data = self.parse_json(self.data)
        else:
            json_data = self.data
        for feeddata in json_data['results']:
            feeddata = unwrap_provider(feeddata)
            contacts = contact_emails(feeddata.get('publicContacts', []))
            if contacts:
                self._provider_contacts.append({
                    'name': feeddata['abbreviation'],
                    'contacts': contacts
                })

    def get_contacts(self):
        return self._provider_contacts


class ParseResourcesContacts(ParseHelpers):
    def __init__(self, data):
        self.data = data

        self._resource_contacts = dict()
        self._parse_data()

    def _parse_data(self):
        if type(self.data) == str:
            json_data = self.parse_json(self.data)
        else:
            json_data = self.data
        for feeddata in json_data['results']:
            feeddata = unwrap_resource(feeddata)
            if not feeddata.get('webpage', False):
                continue
            key = '{}+{}'.format(construct_fqdn(feeddata['webpage']), remove_non_utf(feeddata['id']))
            contacts = contact_emails(feeddata.get('publicContacts', []))

            if contacts:
                self._resource_contacts[key] = contacts

    def get_contacts(self):
        return self._resource_contacts
