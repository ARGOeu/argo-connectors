import csv
import json
from io import StringIO
from lxml.etree import XMLSyntaxError

from argo_connectors.utils import module_class_name
from argo_connectors.exceptions import ConnectorParseError


class ParseHelpers(object):
    def __init__(self, logger, *args, **kwargs):
        self.logger = logger

    def parse_extensions(self, extensions_node):
        extensions_dict = dict()

        for extension in extensions_node:
            key, value = None, None
            for ext_node in extension:
                if ext_node.tag == 'KEY':
                    key = ext_node.text
                if ext_node.tag == 'VALUE':
                    value = ext_node.text
                if key and value:
                    extensions_dict.update({key: value})

        return extensions_dict

    def parse_url_endpoints(self, endpoints_node):
        endpoints_urls = list()

        for endpoint in endpoints_node:
            for endpoint_monitored in endpoint.iterchildren():
                if endpoint_monitored.tag == 'ENDPOINT_MONITORED':
                    value = endpoint_monitored.text
                    if value.lower() == 'y':
                        for url_node in endpoint.iterchildren():
                            if url_node.tag == 'URL':
                                url = url_node.text
                                endpoints_urls.append(url)

        if endpoints_urls:
            return ', '.join(endpoints_urls)
        else:
            return None

    def parse_scopes(self, xml_node):
        scopes_list = list()

        scopes = xml_node.find('SCOPES')
        if scopes != None:
            for scope in scopes.iterchildren():
                scopes_list.append(scope.text)

        return scopes_list

    def parse_xmltext(self, node):
        try:
            value = node.text
            if value is not None:
                return value
            else:
                return ''
        except AttributeError:
            return ''

    def parse_xml(self, data):
        try:
            if data is None:
                if getattr(self.logger, 'job', False):
                    raise ConnectorParseError("{} Customer:{} Job:{} : No XML data fetched".format(
                        module_class_name(self), self.logger.customer, self.logger.job))
                else:
                    raise ConnectorParseError("{} Customer:{} : No XML data fetched".format(
                        module_class_name(self), self.logger.customer))

            return data

        except XMLSyntaxError:
            msg = '{} Customer:{} : Error parsing XML feed - {}'.format(
                module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

        except Exception as exc:
            msg = '{} Customer:{} : Error - {}'.format(
                module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

    def parse_json(self, data):
        try:
            if data is None:
                if getattr(self.logger, 'job', False):
                    raise ConnectorParseError("{} Customer:{} Job:{} : No JSON data fetched".format(
                        module_class_name(self), self.logger.customer, self.logger.job))
                else:
                    raise ConnectorParseError("{} Customer:{} : No JSON data fetched".format(
                        module_class_name(self), self.logger.customer))

            return json.loads(data)

        except ValueError as exc:
            msg = '{} Customer:{} : Error parsing JSON feed - {}'.format(
                module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

        except Exception as exc:
            msg = '{} Customer:{} : Error - {}'.format(
                module_class_name(self), self.logger.customer, repr(exc))
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
            msg = '{} Customer:{} : Error parsing CSV feed - empty data'.format(
                module_class_name(self), self.logger.customer)
            raise ConnectorParseError(msg)
        return results
