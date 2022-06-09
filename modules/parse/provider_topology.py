from urllib.parse import urlparse
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import filename_date, module_class_name, construct_fqdn

import uuid


SERVICE_NAME_WEBPAGE='eu.eosc.portal.services.url'


def buildmap_id2groupname(resources):
    id2name = dict()
    for resource in resources:
        id2name[resource['group']] = resource['tags']['info_groupname']
    return id2name


def build_urlpath_id(http_endpoint):
    path = urlparse(http_endpoint).path.replace('/', '')
    if path and path != http_endpoint:
        return uuid.uuid3(uuid.NAMESPACE_URL, path)
    else:
        return None


class ParseResources(ParseHelpers):
    def __init__(self, logger, data=None, custname=None):
        super(ParseResources, self).__init__(logger)
        self.data = data
        self.custname = custname
        self._resources = list()
        self._parse_data()

    def _parse_data(self):
        try:
            if type(self.data) == str:
                json_data = self.parse_json(self.data)
            else:
                json_data = self.data
            for resource in json_data['results']:
                self._resources.append({
                    'id': resource['id'],
                    'hardcoded_service': SERVICE_NAME_WEBPAGE,
                    'name': resource['name'],
                    'provider': resource['resourceOrganisation'],
                    'webpage': resource['webpage'],
                    'resource_tag': resource['tags'],
                    'description': resource['description']
                })
            self.data = self._resources

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            msg = module_class_name(self) + ' Customer:%s : Error parsing EOSC Resources feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc


class ParseProviders(ParseHelpers):
    def __init__(self, logger, data, custname):
        super(ParseProviders, self).__init__(logger)
        self.data = data
        self.custname = custname
        self._providers = list()
        self._parse_data()

    def _parse_data(self):
        try:
            if type(self.data) == str:
                json_data = self.parse_json(self.data)
            else:
                json_data = self.data
            for provider in json_data['results']:
                self._providers.append({
                    'id': provider['id'],
                    'website': provider['website'],
                    'name': provider['name'],
                    'abbr': provider['abbreviation'],
                    'provider_tag': provider['tags']
                })
            self.data = self._providers

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            msg = module_class_name(self) + ' Customer:%s : Error parsing EOSC Providers feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc


class ParseExtensions(ParseHelpers):
    def __init__(self, logger, data=None, groupnames=None, uidservendp=True, custname=None):
        super(ParseExtensions, self).__init__(logger)
        self.data = data
        self.custname = custname
        self.uidservendp = uidservendp
        self._extensions = list()
        self.groupnames = groupnames
        self._parse_data()

    def _parse_data(self):
        try:
            if type(self.data) == str:
                json_data = self.parse_json(self.data)
            else:
                json_data = self.data

            for extension in json_data['results']:
                if extension['serviceId'] not in self.groupnames:
                    continue

                for group in extension['monitoringGroups']:
                    gee = dict()
                    gee['type'] = 'SERVICEGROUPS'
                    gee['service'] = group['serviceType']
                    gee['group'] = extension['serviceId']
                    if self.uidservendp:
                        hostname = construct_fqdn(group['endpoint'])
                        urlpath_id = build_urlpath_id(group['endpoint'])
                        if not hostname:
                            hostname = group['endpoint']
                        if urlpath_id:
                            gee['hostname'] = '{}_{}_{}'.format(hostname, extension['id'], urlpath_id)
                        else:
                            gee['hostname'] = '{}_{}'.format(hostname, extension['id'])
                    else:
                        hostname = construct_fqdn(group['endpoint'])
                        if not hostname:
                            hostname = group['endpoint']
                        gee['hostname'] = hostname
                    gee['tags'] = dict(
                        info_URL=group['endpoint'],
                        info_ID='{}_{}'.format(extension['id'], urlpath_id) if urlpath_id else extension['id'],
                        info_monitored_by=extension['monitoredBy'],
                        info_groupname=self.groupnames[extension['serviceId']]
                    )
                    if self.uidservendp:
                        hostname = construct_fqdn(group['endpoint'])
                        if not hostname:
                            hostname = group['endpoint']
                        gee['tags'].update(dict(hostname=hostname))
                    self._extensions.append(gee)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            msg = module_class_name(self) + ' Customer:%s : Error parsing EOSC Resources Extensions feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc

    def get_extensions(self):
        return self._extensions


class ParseTopo(object):
    def __init__(self, logger, providers, resources, uidservendp, custname):
        self.uidservendp = uidservendp
        self.providers = ParseProviders(logger, providers, custname)
        self.resources = ParseResources(logger, resources, custname)
        self.maxDiff = None

    def get_group_groups(self):
        gg = list()
        for provider in self.providers.data:
            resource_from_provider = list(filter(
                lambda resource: resource['provider'] == provider['id'],
                self.resources.data
            ))
            for resource in resource_from_provider:
                gge = dict()
                gge['type'] = 'PROJECT'
                gge['group'] = provider['id']
                gge['subgroup'] = resource['id']
                if provider.get('provider_tag', False):
                    provider_tags = [tag.strip() for tag in provider['provider_tag']]
                    gge['tags'] = dict(provider_tags=', '.join(provider_tags), info_projectname=provider['abbr'])
                else:
                    gge['tags'] = dict(info_projectname=provider['abbr'])
                gg.append(gge)

        return gg

    def get_group_endpoints(self):
        ge = list()
        for resource in self.resources.data:
            gee = dict()
            gee['type'] = 'SERVICEGROUPS'
            gee['service'] = resource['hardcoded_service']
            gee['group'] = resource['id']
            if self.uidservendp:
                gee['hostname'] = '{}_{}'.format(construct_fqdn(resource['webpage']), resource['id'])
            else:
                gee['hostname'] = construct_fqdn(resource['webpage'])
            if resource.get('resource_tag', False):
                resource_tags = [tag.strip() for tag in resource['resource_tag']]
                gee['tags'] = dict(service_tags=', '.join(resource_tags),
                                   info_URL=resource['webpage'],
                                   info_ID=resource['id'],
                                   info_groupname=resource['name'])
            else:
                gee['tags'] = dict(info_URL=resource['webpage'],
                                   info_ID=resource['id'],
                                   info_groupname=resource['name'])
            if self.uidservendp:
                gee['tags'].update(dict(hostname=construct_fqdn(resource['webpage'])))
            ge.append(gee)

        return ge
