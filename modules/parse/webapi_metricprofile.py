import json
from argo_connectors.utils import module_class_name
from argo_connectors.io.http import ConnectorHttpError
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers


class ParseMetricProfiles(ParseHelpers):
    def __init__(self, logger, data, target_profiles):
        self.logger = logger
        self.data = data
        self.target_profiles = target_profiles

    def get_data(self):
        try:
            fetched_profiles = self.parse_json(self.data)['data']
            target_profiles = list(filter(lambda profile: profile['name'] in self.target_profiles, fetched_profiles))
            profile_list = list()

            if len(target_profiles) == 0:
                self.logger.error('Customer:' + self.logger.customer + ' Job:' + self.logger.job + ': No profiles {0} were found!'.format(', '.join(self.target_profiles)))
                raise SystemExit(1)

            for profile in target_profiles:
                for service in profile['services']:
                    for metric in service['metrics']:
                        profile_name = profile['name']
                        profile_list.append({
                            'profile': profile_name,
                            'metric': metric,
                            'service': service['service']
                        })
            return profile_list

        except (KeyError, IndexError, ValueError) as exc:
            self.logger.error(module_class_name(self) + ': Error parsing feed - %s' % (repr(exc).replace('\'', '')))
            raise ConnectorParseError()

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
