import xml.dom.minidom
from xml.parsers.expat import ExpatError

from argo_connectors.parse.base import ParseHelpers
from argo_connectors.parse.gocdb_topology import ParseServiceEndpoints
from argo_connectors.utils import filename_date, module_class_name
from argo_connectors.exceptions import ConnectorParseError


class ParseContacts(ParseHelpers):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _parse_contact(self, contacts_node, *attrs):
        values = list()
        for xml_attr in attrs:
            value = contacts_node.getElementsByTagName(xml_attr)
            if value and value[0].childNodes:
                values.append(value[0].childNodes[0].nodeValue)
            else:
                values.append('')
        return values

    def parse_contacts(self, data, root_node, child_node, topo_node):
        interested = ('EMAIL', 'FORENAME', 'SURNAME', 'CERTDN', 'ROLE_NAME')

        try:
            data = dict()
            xml_data = self.parse_xml(self.data)
            entities = xml_data.getElementsByTagName(root_node)
            for entity in entities:
                if entity.nodeName == root_node:
                    emails = list()
                    for entity_node in entity.childNodes:
                        if entity_node.nodeName == child_node:
                            contact = entity_node
                            email, name, surname, certdn, role = self._parse_contact(contact, *interested)
                            emails.append({
                                'email': email,
                                'forename': name,
                                'surname': surname,
                                'certdn': certdn,
                                'role': role
                            })
                        if entity_node.nodeName == topo_node:
                            entity_name = entity_node.childNodes[0].nodeValue
                data[entity_name] = emails

            return data

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + ' Customer:%s : Error parsing - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc

    def parse_sites_with_contacts(self, data):
        try:
            sites_contacts = dict()
            xml_data = self.parse_xml(data)
            elements = xml_data.getElementsByTagName('SITE')
            for element in elements:
                sitename, contact = None, None
                for child in element.childNodes:
                    if child.nodeName == 'CONTACT_EMAIL' and child.childNodes:
                        contact = child.childNodes[0].nodeValue
                    if child.nodeName == 'SHORT_NAME' and child.childNodes:
                        sitename = child.childNodes[0].nodeValue
                if contact:
                    if ';' in contact:
                        lcontacts = list()
                        for single_contact in contact.split(';'):
                            lcontacts.append(single_contact)
                        sites_contacts[sitename] = lcontacts
                    else:
                        sites_contacts[sitename] = [contact]
            return sites_contacts

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + ' Customer:%s : Error parsing - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc

    def parse_servicegroups_with_contacts(self, data):
        return self.parse_servicegroup_contacts(data)

    def parse_servicegroup_contacts(self, data):
        try:
            endpoints_contacts = dict()
            xml_data = self.parse_xml(data)
            elements = xml_data.getElementsByTagName('SERVICE_GROUP')
            for element in elements:
                name, contact = None, None
                for child in element.childNodes:
                    if child.nodeName == 'NAME' and child.childNodes:
                        name = child.childNodes[0].nodeValue
                    if child.nodeName == 'CONTACT_EMAIL' and child.childNodes:
                        contact = child.childNodes[0].nodeValue
                if contact and name:
                    endpoints_contacts[name] = [contact]
            return endpoints_contacts

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + ' Customer:%s : Error parsing - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc

    def parse_serviceendpoint_contacts(self, data):
        try:
            endpoints_contacts = dict()
            xml_data = self.parse_xml(data)
            elements = xml_data.getElementsByTagName('SERVICE_ENDPOINT')
            for element in elements:
                fqdn, contact, servtype = None, None, None
                for child in element.childNodes:
                    if child.nodeName == 'HOSTNAME' and child.childNodes:
                        fqdn = child.childNodes[0].nodeValue
                    if child.nodeName == 'CONTACT_EMAIL' and child.childNodes:
                        contact = child.childNodes[0].nodeValue
                    if child.nodeName == 'SERVICE_TYPE' and child.childNodes:
                        servtype = child.childNodes[0].nodeValue
                if contact:
                    if ';' in contact:
                        lcontacts = list()
                        for single_contact in contact.split(';'):
                            lcontacts.append(single_contact)
                        endpoints_contacts['{}+{}'.format(fqdn, servtype)] = lcontacts
                    else:
                        endpoints_contacts['{}+{}'.format(fqdn, servtype)] = [contact]
            return endpoints_contacts

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + ' Customer:%s : Error parsing - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc


class ParseRocContacts(ParseContacts):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.data = data
        self.logger = logger
        self.data = data
        self._parse_data()

    def _parse_data(self):
        try:
            return self.parse_contacts(self.data, 'ROC', 'CONTACT', 'ROCNAME')

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError) as exc:
            raise ConnectorParseError

    def get_contacts(self):
        return self._parse_data()


class ParseSiteContacts(ParseContacts):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.logger = logger
        self.data = data
        self._parse_data()

    def _parse_data(self):
        try:
            return self.parse_contacts(self.data, 'SITE', 'CONTACT', 'SHORT_NAME')

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError) as exc:
            raise ConnectorParseError

    def get_contacts(self):
        return self._parse_data()


class ParseSitesWithContacts(ParseContacts):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.logger = logger
        self.data = data
        self._parse_data()

    def _parse_data(self):
        try:
            return self.parse_sites_with_contacts(self.data)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError) as exc:
            raise ConnectorParseError

    def get_contacts(self):
        return self._parse_data()


class ParseServiceEndpointContacts(ParseContacts):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.logger = logger
        self.data = data

    def _parse_data(self):
        try:
            return self.parse_serviceendpoint_contacts(self.data)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError) as exc:
            raise ConnectorParseError

    def get_contacts(self):
        return self._parse_data()


class ParseProjectContacts(object):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.data = data

    def _parse_data(self):
        pass

    def contacts(self):
        pass


class ParseServiceGroupWithContacts(ParseContacts):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.data = data
        self.logger = logger

    def _parse_data(self):
        try:
            return self.parse_servicegroups_with_contacts(self.data)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError) as exc:
            raise ConnectorParseError

    def get_contacts(self):
        return self._parse_data()


class ParseServiceGroupRoles(ParseContacts):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.data = data
        self.logger = logger

    def _parse_data(self):
        try:
            return self.parse_servicegroup_contacts(self.data)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError) as exc:
            raise ConnectorParseError

    def get_contacts(self):
        return self._parse_data()
