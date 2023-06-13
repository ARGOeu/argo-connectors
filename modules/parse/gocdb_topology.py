from lxml import etree
from lxml.etree import XMLSyntaxError

from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import module_class_name
from argo_connectors.exceptions import ConnectorParseError


class ParseSites(ParseHelpers):
    def __init__(self, logger, data, custname, uid=False,
                 pass_extensions=False, notification_flag=False):
        super().__init__(logger)
        self.logger = logger
        self.data = data
        self.uidservendp = uid
        self.custname = custname
        self.pass_extensions = pass_extensions
        self._sites = dict()
        self.notification_flag = notification_flag
        self._parse_data()

    def _parse_data(self):
        try:
            doc = self.parse_xml(self.data)
            xml_bytes = doc.encode("utf-8")
            sites = etree.fromstring(xml_bytes)

            for site in sites:
                if site.tag != 'meta':
                    site_name = site.attrib["NAME"]
                    if site_name not in self._sites:
                        self._sites[site_name] = {'site': site_name}

                    for prod_in in site.xpath('.//PRODUCTION_INFRASTRUCTURE'):
                        production_infra = prod_in.text
                        if production_infra:
                            self._sites[site_name]['infrastructure'] = production_infra

                    for cert_st in site.xpath('.//CERTIFICATION_STATUS'):
                        certification_status = cert_st.text
                        if certification_status:
                            self._sites[site_name]['certification'] = certification_status

                    for roc in site.xpath('.//ROC'):
                        if roc != None:
                            self._sites[site_name]['ngi'] = roc.text

                    try:
                        if site.attrib["ROC"] != None:
                            self._sites[site_name]['ngi'] = site.attrib["ROC"]
                    except:
                        pass

                    self._sites[site_name]['scope'] = ', '.join(
                        self.parse_scopes(site))

                    if self.notification_flag:
                        try:
                            for notification in site.xpath('NOTIFICATIONS'):
                                notification = notification.text
                                notification = True if notification.lower(
                                ) == 'true' or notification.lower() == 'y' else False

                                self._sites[site_name]['notification'] = notification
                        except IndexError:
                            self._sites[site_name]['notification'] = True

                    # biomed feed does not have extensions
                    if self.pass_extensions:
                        try:
                            for ext in site:
                                if ext.tag == 'EXTENSIONS':
                                    self._sites[site_name]['extensions'] = self.parse_extensions(
                                        ext)
                        except IndexError:
                            pass

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, XMLSyntaxError) as exc:
            msg = module_class_name(self) + ' Customer:%s : Error parsing sites feed - %s' % (
                self.logger.customer, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc

    def get_group_groups(self):
        group_list, groupofgroups = list(), list()
        group_list = group_list + \
            sorted([value for _, value in self._sites.items()],
                   key=lambda s: s['ngi'])

        for group in group_list:
            tmpg = dict()
            tmpg['type'] = 'NGI'
            tmpg['group'] = group['ngi']
            tmpg['subgroup'] = group['site']
            if self.notification_flag:
                tmpg['notifications'] = {
                    'contacts': [], 'enabled': group['notification']}
            tmpg['tags'] = {'certification': group.get('certification', ''),
                            'scope': group.get('scope', ''),
                            'infrastructure': group.get('infrastructure', '')}

            if self.pass_extensions and 'extensions' in group:
                for key, value in group['extensions'].items():
                    tmpg['tags'].update({'info_ext_' + key: value})

            groupofgroups.append(tmpg)

        return groupofgroups


class ParseServiceEndpoints(ParseHelpers):
    def __init__(self, logger, data=None, custname=None, uid=False,
                 pass_extensions=False, notification_flag=False):
        super().__init__(logger)
        self.data = data
        self.uidservendp = uid
        self.custname = custname
        self.pass_extensions = pass_extensions
        self.notification_flag = notification_flag
        self._service_endpoints = dict()
        self._parse_data()
        self.maxDiff = None

    def _parse_data(self):
        try:
            doc = self.parse_xml(self.data)
            xml_bytes = doc.encode("utf-8")
            services = etree.fromstring(xml_bytes)

            for service in services:
                if service.tag != 'meta':
                    service_id = service.attrib["PRIMARY_KEY"]
                    if service_id not in self._service_endpoints:
                        self._service_endpoints[service_id] = {}

                    for serv_endpnts in service.xpath('.//HOSTNAME'):
                        self._service_endpoints[service_id]['hostname'] = self.parse_xmltext(serv_endpnts)

                    for serv_types in service.xpath('.//SERVICE_TYPE'):
                        self._service_endpoints[service_id]['type'] = self.parse_xmltext(serv_types)

                    for hostdn in service.xpath('.//HOSTDN'):
                        self._service_endpoints[service_id]['hostdn'] = self.parse_xmltext(hostdn)

                    for node_mon in service.xpath('.//NODE_MONITORED'):
                        self._service_endpoints[service_id]['monitored'] = self.parse_xmltext(node_mon)

                    for in_prod in service.xpath('.//IN_PRODUCTION'):
                        self._service_endpoints[service_id]['production'] = self.parse_xmltext(in_prod)

                    for site_name in service.xpath('.//SITENAME'):
                        self._service_endpoints[service_id]['site'] = self.parse_xmltext(site_name)

                    for roc_name in service.xpath('.//ROC_NAME'):
                        self._service_endpoints[service_id]['roc'] = self.parse_xmltext(roc_name)

                    self._service_endpoints[service_id]['service_id'] = service_id

                    self._service_endpoints[service_id]['scope'] = ', '.join(
                        self.parse_scopes(service))

                    self._service_endpoints[service_id]['sortId'] = self._service_endpoints[service_id]['hostname'] + \
                        '-' + self._service_endpoints[service_id]['type'] + \
                        '-' + self._service_endpoints[service_id]['site']

                    self._service_endpoints[service_id]['url'] = service.find(
                        'URL').text

                    if self.notification_flag:
                        for notification in service.xpath('NOTIFICATIONS'):
                            if notification is not None:
                                notification = notification.text
                                notification = True if notification.lower() == 'true' or notification.lower() == 'y' else False
                                self._service_endpoints[service_id]['notification'] = notification
                            else:
                                self._service_endpoints[service_id]['notification'] = True

                    if self.pass_extensions:
                        extension_node = None
                        extnodes = service.xpath('.//EXTENSIONS')
                        for node in extnodes:
                            parent = node.getparent()
                            if parent.tag == 'SERVICE_ENDPOINT':
                                extension_node = node
                        extensions = self.parse_extensions(extension_node)
                        self._service_endpoints[service_id]['extensions'] = extensions

                    url = service.find('.//ENDPOINTS')
                    self._service_endpoints[service_id]['endpoint_urls'] = self.parse_url_endpoints(
                        url)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, XMLSyntaxError) as exc:
            msg = module_class_name(self) + ' Customer:%s : Error parsing topology service endpoint feed - %s' % (
                self.logger.customer, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc

    def get_group_endpoints(self):
        group_list, groupofendpoints = list(), list()
        group_list = group_list + \
            sorted([value for _, value in self._service_endpoints.items()],
                   key=lambda s: s['site'])

        for group in group_list:
            tmpg = dict()
            tmpg['type'] = 'SITES'
            tmpg['group'] = group['site']
            tmpg['service'] = group['type']
            if self.notification_flag:
                tmpg['notifications'] = {
                    'contacts': [], 'enabled': group['notification']}
            if self.uidservendp:
                tmpg['hostname'] = '{1}_{0}'.format(
                    group['service_id'], group['hostname'])
            else:
                tmpg['hostname'] = group['hostname']
            tmpg['tags'] = {'scope': group.get('scope', ''),
                            'monitored': '1' if group['monitored'] == 'Y' or
                            group['monitored'] == 'True' else '0',
                            'production': '1' if group['production'] == 'Y' or
                            group['production'] == 'True' else '0'}
            tmpg['tags'].update({'info_ID': str(group['service_id'])})
            if self.uidservendp:
                tmpg['tags'].update({'hostname': group['hostname']})

            if 'hostdn' in group:
                tmpg['tags'].update({'info_HOSTDN': group['hostdn']})

            if group['url']:
                tmpg['tags'].update({
                    'info_URL': group['url']
                })
            if group['endpoint_urls']:
                tmpg['tags'].update({
                    'info_service_endpoint_URL': group['endpoint_urls']
                })
            if self.pass_extensions:
                for key, value in group['extensions'].items():
                    tmpg['tags'].update({
                        'info_ext_' + key: value
                    })

            groupofendpoints.append(tmpg)

        return groupofendpoints


class ParseServiceGroups(ParseHelpers):
    def __init__(self, logger, data, custname, uid=False,
                 pass_extensions=False, notification_flag=False):
        super().__init__(logger)
        self.data = data
        self.uidservendp = uid
        self.custname = custname
        self.pass_extensions = pass_extensions
        self.notification_flag = notification_flag
        # group_groups and group_endpoints components for ServiceGroup topology
        self._service_groups = dict()
        self._parse_data()

    def _parse_data(self):
        try:
            doc = self.parse_xml(self.data)
            xml_bytes = doc.encode("utf-8")
            groups = etree.fromstring(xml_bytes)
            for group in groups:
                if group.tag != 'meta':
                    group_id = group.attrib["PRIMARY_KEY"]
                    if group_id not in self._service_groups:
                        self._service_groups[group_id] = {}

                    self._service_groups[group_id]['name'] = self.parse_xmltext(group.find('NAME'))

                    self._service_groups[group_id]['monitored'] =  self.parse_xmltext(group.find('MONITORED'))

                    self._service_groups[group_id]['services'] = []
                    self._service_groups[group_id]['scope'] = ', '.join(self.parse_scopes(group))

                    if self.notification_flag:
                        try:
                            notification = self.parse_xmltext(group.find('NOTIFICATIONS'))
                            notification = True if notification.lower() == 'true' \
                                or notification.lower() == 'y' else False
                            self._service_groups[group_id]['notification'] = notification
                        except:
                            try:
                                notification = self.parse_xmltext(group.find('NOTIFY'))
                                notification = True if notification.lower() == 'true' \
                                    or notification.lower() == 'y' else False
                                self._service_groups[group_id]['notification'] = notification

                            except:
                                self._service_groups[group_id]['notification'] = True

                    for service in group.iter("SERVICE_ENDPOINT"):
                        tmps = dict()

                        tmps['hostname'] = self.parse_xmltext(service.find('HOSTNAME'))
                        try:
                            tmps['service_id'] = self.parse_xmltext(service.find('PRIMARY_KEY'))
                        except AttributeError:
                            tmps['service_id'] = service.attrib["PRIMARY_KEY"]

                        tmps['type'] = self.parse_xmltext(service.find('SERVICE_TYPE'))
                        tmps['monitored'] = self.parse_xmltext(service.find('NODE_MONITORED'))
                        tmps['production'] = self.parse_xmltext(service.find('IN_PRODUCTION'))
                        tmps['scope'] = ', '.join(self.parse_scopes(service))

                        endpoint_urls = self.parse_xmltext(service.find('ENDPOINTS/ENDPOINT/URL'))
                        tmps['endpoint_urls'] = endpoint_urls

                        if self.notification_flag:
                            try:
                                notification = self.parse_xmltext(service.find('NOTIFICATIONS'))
                                notification = True if notification.lower() == 'true' or \
                                    notification.lower() == 'y' else False
                                tmps['notification'] = notification

                            except:
                                tmps['notification'] = True

                        if self.pass_extensions:
                            extensions = self.parse_extensions(
                                service.find('EXTENSIONS'))
                            tmps['extensions'] = extensions
                        self._service_groups[group_id]['services'].append(tmps)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, XMLSyntaxError) as exc:
            msg = module_class_name(self) + ' Customer:%s : Error parsing service groups feed - %s' % (
                self.logger.customer, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc

    def get_group_endpoints(self):
        group_list, groupofendpoints = list(), list()
        group_list = group_list + [value for _,
                                   value in self._service_groups.items()]

        for group in group_list:
            for service in group['services']:
                tmpg = dict()
                tmpg['type'] = 'SERVICEGROUPS'
                tmpg['group'] = group['name']
                tmpg['service'] = service['type']
                if self.notification_flag:
                    tmpg['notifications'] = {
                        'contacts': [], 'enabled': service['notification']}
                if self.uidservendp:
                    tmpg['hostname'] = '{1}_{0}'.format(
                        service['service_id'], service['hostname'])
                else:
                    tmpg['hostname'] = service['hostname']

                tmpg['tags'] = {'scope': service.get('scope', ''),
                                'monitored': '1' if service['monitored'].lower() == 'Y'.lower() or
                                service['monitored'].lower() == 'True'.lower() else '0',
                                'production': '1' if service['production'].lower() == 'Y'.lower() or
                                service['production'].lower() == 'True'.lower() else '0'}

                if self.uidservendp:
                    tmpg['tags'].update({'hostname': service['hostname']})
                tmpg['tags'].update({'info_ID': str(service['service_id'])})

                if self.pass_extensions:
                    for key, value in service['extensions'].items():
                        tmpg['tags'].update({
                            'info_ext_' + key: value
                        })
                if service['endpoint_urls']:
                    tmpg['tags'].update({
                        'info_service_endpoint_URL': service['endpoint_urls']
                    })

                groupofendpoints.append(tmpg)

        return groupofendpoints

    def get_group_groups(self):
        group_list, groupofgroups = list(), list()
        group_list = group_list + [value for _,
                                   value in self._service_groups.items()]

        for group in group_list:
            tmpg = dict()
            tmpg['type'] = 'PROJECT'
            tmpg['group'] = self.custname
            if self.notification_flag:
                tmpg['notifications'] = {
                    'contacts': [], 'enabled': group['notification']}
            tmpg['subgroup'] = group['name']
            tmpg['tags'] = {'monitored': '1' if group['monitored'].lower() == 'Y'.lower() or
                            group['monitored'].lower() == 'True'.lower() else '0', 'scope': group.get('scope', '')}
            groupofgroups.append(tmpg)

        return groupofgroups
