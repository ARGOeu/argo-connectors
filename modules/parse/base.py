import csv
import json
from io import StringIO
from lxml.etree import XMLSyntaxError

from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.log import Logger
from argo_connectors.utils import module_class_name


class ParseHelpers():
    def topo_type(self, ask, explicit_type=None):
        if self.Customer.opt('TopoSetType'):
            topotype = [self.Customer.opt('TopoSetType').lower()]
        else:
            topotype = [ft.lower() for ft in self.Customer.get_topofetchtype()]

        if len(topotype) == 1:
            if 'Sites'.lower() in topotype and ask == 'gg':
                return 'ngi'.upper()
            elif 'Sites'.lower() in topotype and ask == 'ge':
                return 'sites'.upper()
            elif 'ServiceGroups'.lower() in topotype and ask == 'gg':
                return 'project'.upper()
            elif 'ServiceGroups'.lower() in topotype and ask == 'ge':
                return 'servicegroups'.upper()

        elif len(topotype) == 2 and explicit_type:
            if explicit_type.lower() == 'Sites'.lower() and ask == 'gg':
                return 'ngi'.upper()
            elif explicit_type.lower() == 'ServiceGroups'.lower() and ask == 'gg':
                return 'project'.upper()
            elif explicit_type.lower() in ['Sites'.lower(), 'ServiceGroups'.lower()] and ask == 'ge':
                return explicit_type.upper()

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
                if getattr(Logger, 'job', False):
                    raise ConnectorParseError("{} Customer:{} Job:{} : No XML data fetched".format(
                        module_class_name(self), Logger.customer, Logger.job))
                else:
                    raise ConnectorParseError("{} Customer:{} : No XML data fetched".format(
                        module_class_name(self), Logger.customer))

            return data

        except XMLSyntaxError:
            msg = '{} Customer:{} : Error parsing XML feed - {}'.format(
                module_class_name(self), Logger.customer, repr(exc))
            raise ConnectorParseError(msg)

        except Exception as exc:
            msg = '{} Customer:{} : Error - {}'.format(
                module_class_name(self), Logger.customer, repr(exc))
            raise ConnectorParseError(msg)

    def parse_json(self, data):
        try:
            if data is None:
                if getattr(Logger, 'job', False):
                    raise ConnectorParseError("{} Customer:{} Job:{} : No JSON data fetched".format(
                        module_class_name(self), Logger.customer, Logger.job))
                else:
                    raise ConnectorParseError("{} Customer:{} : No JSON data fetched".format(
                        module_class_name(self), Logger.customer))

            return json.loads(data)

        except ValueError as exc:
            msg = '{} Customer:{} : Error parsing JSON feed - {}'.format(
                module_class_name(self), Logger.customer, repr(exc))
            raise ConnectorParseError(msg)

        except Exception as exc:
            msg = '{} Customer:{} : Error - {}'.format(
                module_class_name(self), Logger.customer, repr(exc))
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
                module_class_name(self), Logger.customer)
            raise ConnectorParseError(msg)
        return results
