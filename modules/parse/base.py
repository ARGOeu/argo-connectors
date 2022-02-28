import xml.dom.minidom

from xml.parsers.expat import ExpatError
from io import StringIO

from argo_egi_connectors.utils import module_class_name
from argo_egi_connectors.exceptions import ConnectorParseError

import json
import csv


class ParseHelpers(object):
    def __init__(self, logger, *args, **kwargs):
        self.logger = logger

    def parse_xmltext(self, nodelist):
        rc = []
        for node in nodelist:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)

    def parse_extensions(self, extensions_node):
        extensions_dict = dict()

        for extension in extensions_node:
            if extension.nodeName == 'EXTENSION':
                key, value = None, None
                for ext_node in extension.childNodes:
                    if ext_node.nodeName == 'KEY':
                        key = ext_node.childNodes[0].nodeValue
                    if ext_node.nodeName == 'VALUE':
                        value = ext_node.childNodes[0].nodeValue
                    if key and value:
                        extensions_dict.update({key: value})

        return extensions_dict

    def parse_url_endpoints(self, endpoints_node):
        endpoints_urls = list()

        for endpoint in endpoints_node:
            if endpoint.nodeName == 'ENDPOINT':
                url = None
                for endpoint_node in endpoint.childNodes:
                    if endpoint_node.nodeName == 'ENDPOINT_MONITORED':
                        value = endpoint_node.childNodes[0].nodeValue
                        if value.lower() == 'y':
                            for url_node in endpoint.childNodes:
                                if url_node.nodeName == 'URL' and url_node.childNodes:
                                    url = url_node.childNodes[0].nodeValue
                                    endpoints_urls.append(url)

        if endpoints_urls:
            return ', '.join(endpoints_urls)
        else:
            return None

    def parse_scopes(self, xml_node):
        scopes = list()

        for elem in xml_node.childNodes:
            if elem.nodeName == 'SCOPES':
                for subelem in elem.childNodes:
                    if subelem.nodeName == 'SCOPE':
                        scopes.append(subelem.childNodes[0].nodeValue)

        return scopes

    def parse_json(self, data):
        try:
            if data is None:
                if getattr(self.logger, 'job', False):
                    raise ConnectorParseError("{} Customer:{} Job:{} : No JSON data fetched".format(module_class_name(self), self.logger.customer, self.logger.job))
                else:
                    raise ConnectorParseError("{} Customer:{} : No JSON data fetched".format(module_class_name(self), self.logger.customer))

            return json.loads(data)

        except ValueError as exc:
            msg = '{} Customer:{} : Error parsing JSON feed - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

        except Exception as exc:
            msg = '{} Customer:{} : Error - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

    def parse_xml(self, data):
        try:
            if data is None:
                if getattr(self.logger, 'job', False):
                    raise ConnectorParseError("{} Customer:{} Job:{} : No XML data fetched".format(module_class_name(self), self.logger.customer, self.logger.job))
                else:
                    raise ConnectorParseError("{} Customer:{} : No XML data fetched".format(module_class_name(self), self.logger.customer))


            return xml.dom.minidom.parseString(data)

        except ExpatError as exc:
            msg = '{} Customer:{} : Error parsing XML feed - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

        except Exception as exc:
            msg = '{} Customer:{} : Error - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

    def csv_to_json(self, data):
        data = StringIO(data)
        reader = csv.reader(data, delimiter=',')

        num_row = 0
        results = []
        header = []
        for row in reader:
            if num_row == 0:
                header = row
                num_row = num_row + 1
                continue
            num_item = 0
            datum = {}
            for item in header:
                datum[item] = row[num_item]
                num_item = num_item + 1
            results.append(datum)

        if not results:
            msg = '{} Customer:{} : Error parsing CSV feed - empty data'.format(module_class_name(self), self.logger.customer)
            raise ConnectorParseError(msg)
        return results
