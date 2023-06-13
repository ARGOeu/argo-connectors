from lxml import etree
from lxml.etree import XMLSyntaxError

from argo_connectors.utils import module_class_name
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers


class ParseGocdbServiceTypes(ParseHelpers):
    def __init__(self, logger, data):
        self.data = data
        self.logger = logger

    def get_data(self):
        all_service_type = list()

        try:
            doc = self.parse_xml(self.data)
            xml_bytes = doc.encode("utf-8")
            service_types = etree.fromstring(xml_bytes)

            for service in service_types:
                name, desc = None, None
                for type_name in service.xpath('.//SERVICE_TYPE_NAME'):
                    name = type_name.text

                for type_desc in service.xpath('.//SERVICE_TYPE_DESC'):
                    desc = type_desc.text

                if name:
                    all_service_type.append({
                        "name": name,
                        "description": desc if desc else '',
                        "tags": ["topology"]
                    })

            return sorted(all_service_type,  key=lambda s: s['name'].lower())

        except (KeyError, IndexError, AttributeError, TypeError, AssertionError, XMLSyntaxError) as exc:
            msg = '{} Customer:{} : Error parsing service types feed - {}'.format(
                module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc
