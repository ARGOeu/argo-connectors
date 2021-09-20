import json
from argo_egi_connectors.tools import module_class_name
from argo_egi_connectors.io.http import ConnectorError


class ParseMetricProfiles(object):
    def __init__(self, logger, data, target_profiles, namespace):
        self.logger = logger
        self.data = data
        self.target_profiles = target_profiles
        self.namespace = namespace

    def _parse_json(self, buf):
        return json.loads(buf)

    def get_data(self):
        try:
            fetched_profiles = self._parse_json(self.data)['data']
            target_profiles = list(filter(lambda profile: profile['name'] in self.target_profiles, fetched_profiles))
            profile_list = list()

            if len(target_profiles) == 0:
                self.logger.error('Customer:' + self.logger.customer + ' Job:' + self.logger.job + ': No profiles {0} were found!'.format(', '.join(self.target_profiles)))
                raise SystemExit(1)

            for profile in target_profiles:
                for service in profile['services']:
                    for metric in service['metrics']:
                        if self.namespace:
                            profile_name = '{0}.{1}'.format(self.namespace, profile['name'])
                        else:
                            profile_name = profile['name']
                        profile_list.append({
                            'profile': profile_name,
                            'metric': metric,
                            'service': service['service']
                        })
            return profile_list

        except (KeyError, IndexError, ValueError) as exc:
            self.logger.error(module_class_name(self) + ': Error parsing feed - %s' % (repr(exc).replace('\'', '')))
            raise ConnectorError()

        except Exception as exc:
            if getattr(self.logger, 'job', False):
                self.logger.error('{} Customer:{} Job:{} : Error - {}'.format(module_class_name(self), self.logger.customer, self.logger.job, repr(exc)))
            else:
                self.logger.error('{} Customer:{} : Error - {}'.format(module_class_name(self), self.logger.customer, repr(exc)))
            raise exc

    def _format(self, profile_list):
        profiles = []

        for profile in profile_list:
            tmpp = dict()
            tmpp['metric'] = profile['metric']
            tmpp['profile'] = profile['profile']
            tmpp['service'] = profile['service']
            profiles.append(tmpp)

        return profiles
