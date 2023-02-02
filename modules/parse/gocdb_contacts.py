from lxml import etree
from lxml.etree import XMLSyntaxError
from xml.parsers.expat import ExpatError

from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import module_class_name
from argo_connectors.exceptions import ConnectorParseError


class ParseContacts(ParseHelpers):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _parse_contact(self, contacts_node, *attrs):
        values = list()
        for xml_attr in attrs:
            value = contacts_node.find(xml_attr)
            if value != None:
                values.append(value.text)
            else:
                values.append('')
        return values

    def parse_contacts(self, data, root_node, child_node, topo_node):
        interested = ('EMAIL', 'FORENAME', 'SURNAME', 'CERTDN', 'ROLE_NAME')

        try:
            data = dict()

            xml_bytes = self.data.encode("utf-8")
            entities = etree.fromstring(xml_bytes)

            for entity in entities:
                if entity.tag == root_node:
                    emails = list()
                    for entity_node in entity:
                        if entity_node.tag == child_node:
                            contact = entity_node
                            email, name, surname, certdn, role = self._parse_contact(
                                contact, *interested)
                            emails.append({
                                'email': email,
                                'forename': name,
                                'surname': surname,
                                'certdn': certdn,
                                'role': role
                            })
                        if entity_node.tag == topo_node:
                            entity_name = entity_node.text

                data[entity_name] = emails

            return data

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, XMLSyntaxError) as exc:
            self.logger.error(module_class_name(self) + ' Customer:%s : Error parsing - %s' %
                              (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc

    def parse_sites_with_contacts(self, data):
        try:
            sites_contacts = dict()

            xml_bytes = data.encode("utf-8")
            elements = etree.fromstring(xml_bytes)

            for element in elements:
                sitename, contact = None, None
                for child in element:
                    if child.tag == 'CONTACT_EMAIL':
                        contact = child.text
                    if child.tag == 'SHORT_NAME':
                        sitename = child.text

                if contact:
                    if ';' in contact:
                        lcontacts = list()
                        for single_contact in contact.split(';'):
                            lcontacts.append(single_contact)
                        sites_contacts[sitename] = lcontacts
                    else:
                        sites_contacts[sitename] = [contact]

            return sites_contacts

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, XMLSyntaxError) as exc:
            self.logger.error(module_class_name(self) + ' Customer:%s : Error parsing - %s' %
                              (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc

    def parse_servicegroups_with_contacts(self, data):
        return self.parse_servicegroup_contacts(data)

    def parse_servicegroup_contacts(self, data):
        try:
            endpoints_contacts = dict()

            xml_bytes = data.encode("utf-8")
            elements = etree.fromstring(xml_bytes)

            for element in elements:

                for child in element:
                    if child.tag == 'NAME':
                        name = child.text

                    if child.tag == 'CONTACT_EMAIL':
                        contact = child.text

                if contact and name:
                    endpoints_contacts[name] = [contact]

            return endpoints_contacts

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, XMLSyntaxError) as exc:
            self.logger.error(module_class_name(self) + ' Customer:%s : Error parsing - %s' %
                              (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc

    def parse_serviceendpoint_contacts(self, data):
        try:
            endpoints_contacts = dict()

            xml_bytes = data.encode("utf-8")
            elements = etree.fromstring(xml_bytes)

            for element in elements:

                fqdn, contact, servtype = None, None, None

                for child in element:
                    if child.tag == 'HOSTNAME':
                        fqdn = child.text

                    if child.tag == 'CONTACT_EMAIL':
                        contact = child.text

                    if child.tag == 'SERVICE_TYPE':
                        servtype = child.text

                if contact != None:
                    if ';' in contact:
                        lcontacts = list()
                        for single_contact in contact.split(';'):
                            lcontacts.append(single_contact)
                        endpoints_contacts['{}+{}'.format(
                            fqdn, servtype)] = lcontacts
                    else:
                        endpoints_contacts['{}+{}'.format(fqdn, servtype)] = [
                            contact]

            return endpoints_contacts

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, XMLSyntaxError) as exc:
            self.logger.error(module_class_name(self) + ' Customer:%s : Error parsing - %s' %
                              (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
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

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError, XMLSyntaxError) as exc:
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

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError, XMLSyntaxError) as exc:
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

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError, XMLSyntaxError) as exc:
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

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError, XMLSyntaxError) as exc:
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

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError, XMLSyntaxError) as exc:
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

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError, XMLSyntaxError) as exc:
            raise ConnectorParseError

    def get_contacts(self):
        return self._parse_data()
