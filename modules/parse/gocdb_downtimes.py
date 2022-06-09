import datetime
import xml.dom.minidom

from xml.parsers.expat import ExpatError
from argo_connectors.utils import module_class_name
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers


class ParseDowntimes(ParseHelpers):
    def __init__(self, logger, data, start, end, uid=False):
        self.uid = uid
        self.start = start
        self.end = end
        self.logger = logger
        self.data = data

    def get_data(self):
        filtered_downtimes = list()

        try:
            downtimes = self.parse_xml(self.data).getElementsByTagName('DOWNTIME')

            for downtime in downtimes:
                classification = downtime.getAttributeNode('CLASSIFICATION').nodeValue
                hostname = self.parse_xmltext(downtime.getElementsByTagName('HOSTNAME')[0].childNodes)
                service_type = self.parse_xmltext(downtime.getElementsByTagName('SERVICE_TYPE')[0].childNodes)
                start_str = self.parse_xmltext(downtime.getElementsByTagName('FORMATED_START_DATE')[0].childNodes)
                end_str = self.parse_xmltext(downtime.getElementsByTagName('FORMATED_END_DATE')[0].childNodes)
                severity = self.parse_xmltext(downtime.getElementsByTagName('SEVERITY')[0].childNodes)
                try:
                    service_id = self.parse_xmltext(downtime.getElementsByTagName('PRIMARY_KEY')[0].childNodes)
                except IndexError:
                    service_id = downtime.getAttribute('PRIMARY_KEY')
                start_time = datetime.datetime.strptime(start_str, "%Y-%m-%d %H:%M")
                end_time = datetime.datetime.strptime(end_str, "%Y-%m-%d %H:%M")

                if start_time < self.start:
                    start_time = self.start
                if end_time > self.end:
                    end_time = self.end

                if classification == 'SCHEDULED' and severity == 'OUTAGE':
                    downtime = dict()
                    if self.uid:
                        downtime['hostname'] = '{0}_{1}'.format(hostname, service_id)
                    else:
                        downtime['hostname'] = hostname
                    downtime['service'] = service_type
                    downtime['start_time'] = start_time.strftime('%Y-%m-%dT%H:%M:00Z')
                    downtime['end_time'] = end_time.strftime('%Y-%m-%dT%H:%M:00Z')
                    filtered_downtimes.append(downtime)

            return filtered_downtimes

        except (KeyError, IndexError, AttributeError, TypeError, AssertionError) as exc:
            msg = '{} Customer:{} : Error parsing downtimes feed - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc
