from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.utils import module_class_name, remove_non_utf

import json
from json.decoder import JSONDecodeError
from unidecode import unidecode


class ParseAgoraTopo(object):
    def __init__(self, logger, providers, resources, uidservendp):
        self.logger = logger
        self.providers = providers
        self.resources = resources
        self.uidservendp = uidservendp

    def get_group_groups(self):
        try:
            providers = list()
            providers_data = json.loads(self.providers)

            for data in providers_data:
                subgroup = unidecode(data['epp_bai_id'])
                catalog_id = unidecode(data['id'])
                catalog_url = unidecode(f'https://catalogue.ni4os.eu/?_=/providers/{data["id"]}')
                ext_name = unidecode(data['epp_bai_name'])

                providers.append({
                    'group': 'NI4OS Providers',
                    'type': 'PROVIDERS',
                    'subgroup': subgroup,
                    'tags': {
                        'info_ext_catalog_id': catalog_id,
                        'info_ext_catalog_type': 'provider',
                        'info_ext_catalog_url': catalog_url,
                        'info_ext_name': ext_name,
                    }
                })

            return providers

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, JSONDecodeError) as exc:
            msg = module_class_name(self) + ' Customer:%s : Error parsing Agora Providers feed - %s' % (
                self.logger.customer, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc

    def get_group_endpoints(self):
        try:
            resources = list()
            providers_data = json.loads(self.providers)
            resources_data = json.loads(self.resources)

            for data in resources_data:
                if data['erp_bai_providers_public'] != [] and len(data['erp_bai_providers_public']) > 1:
                    group_name = [d['epp_bai_id']for d in data['erp_bai_providers_public']]

                elif data['erp_bai_providers_public'] != []:
                    group_name = data['erp_bai_providers_public'][0]['epp_bai_id']

                data_hostname = unidecode(data["erp_bai_id"].lower().replace(" ", "_"))
                rsc_hostname = remove_non_utf(f'agora.ni4os.eu_{data_hostname}')
                rsc_info_id = remove_non_utf(f'{data_hostname if data["erp_bai_id"] is not None else ""}')
                rsc_catalog_id = f'{unidecode(data["id"])}'
                rsc_catalog_url = f'https://catalogue.ni4os.eu/?_=/resources/{unidecode(data["id"])}'
                rsc_ext_name = f'{unidecode(data["erp_bai_name"])}'

                if type(group_name) == list and group_name != []:
                    for i in range(len(group_name)):
                        resources.append({
                            'group': unidecode(group_name[i]),
                            'type': 'SERVICEGROUPS',
                            'service': 'catalog.service.entry',
                            'hostname': rsc_hostname,
                            'tags': {
                                'hostname': 'agora.ni4os.eu',
                                'info_ID': rsc_info_id,
                                'info_ext_catalog_id': rsc_catalog_id,
                                'info_ext_catalog_type': 'resource',
                                'info_ext_catalog_url': rsc_catalog_url,
                                'info_ext_name': rsc_ext_name
                            }
                        })

                else:
                    resources.append({
                        'group': unidecode(group_name),
                        'type': 'SERVICEGROUPS',
                        'service': 'catalog.service.entry',
                        'hostname': rsc_hostname,
                        'tags': {
                            'hostname': 'agora.ni4os.eu',
                            'info_ID': rsc_info_id,
                            'info_ext_catalog_id': rsc_catalog_id,
                            'info_ext_catalog_type': 'resource',
                            'info_ext_catalog_url': rsc_catalog_url,
                            'info_ext_name': rsc_ext_name
                        }
                    })

            # constructed from provider entry and planted as artificial resource
            for pr_data in providers_data:
                prov_group = unidecode(pr_data['epp_bai_id'])
                host_pr = unidecode(pr_data["epp_bai_id"].lower().replace(" ", "_").replace(",", ""))
                prov_hostname = remove_non_utf(f'agora.ni4os.eu_{host_pr}')
                prov_info_id = remove_non_utf(unidecode(pr_data['epp_bai_id'].lower().replace(" ", "_").replace(",", "")))
                prov_catalog_id = unidecode(pr_data['id'])
                prov_catalog_url = f'https://catalogue.ni4os.eu/?_=/providers/{unidecode(pr_data["id"])}'
                prov_ext_name = unidecode(pr_data['epp_bai_name'])

                resources.append({
                    'group': prov_group,
                    'type': 'SERVICEGROUPS',
                    'service': 'catalog.provider.entry',
                    'hostname': prov_hostname,
                    'tags': {
                        'hostname': 'agora.ni4os.eu',
                        'info_ID': prov_info_id,
                        'info_ext_catalog_id': prov_catalog_id,
                        'info_ext_catalog_type': 'provider',
                        'info_ext_catalog_url': prov_catalog_url,
                        'info_ext_name': prov_ext_name
                    }
                })

            return resources

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, JSONDecodeError) as exc:
            msg = module_class_name(self) + ' Customer:%s : Error parsing Agora Resources feed - %s' % (
                self.logger.customer, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc
