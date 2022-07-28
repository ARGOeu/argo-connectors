import datetime
import xml.dom.minidom

from xml.parsers.expat import ExpatError

from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import construct_fqdn
from argo_connectors.utils import module_class_name


class ParseDowntimes(ParseHelpers):
    def __init__(self, logger, data, current_date, uid=False):
        self.logger = logger
        self.data = self.csv_to_json(data)
        self.start = current_date
        self.end = current_date.replace(hour=23, minute=59, second=59)
        self.uid = uid

    def get_data(self):
        downtimes = list()

        for downtime in self.data:
            entry = dict()

            service_id = downtime['unique_id']
            classification = downtime['Severity']
            if not service_id or classification != 'OUTAGE':
                continue

            hostname = construct_fqdn(downtime['url'])
            service_type = downtime['service_type']
            start_time = datetime.datetime.strptime(downtime['start_time'], "%m/%d/%Y %H:%M")
            end_time = datetime.datetime.strptime(downtime['end_time'], "%m/%d/%Y %H:%M")

            if self.uid:
                entry['hostname'] = '{0}_{1}'.format(hostname, service_id)
            else:
                entry['hostname'] = hostname

            start_date = start_time.replace(hour=0, minute=0, second=0)
            end_date = end_time.replace(hour=0, minute=0, second=0)
            if self.start >= start_date and self.start <= end_date:
                if start_time < self.start:
                    start_time = self.start
                if end_time > self.end:
                    end_time = self.end

                entry['service'] = service_type
                entry['start_time'] = start_time.strftime('%Y-%m-%dT%H:%M:00Z')
                entry['end_time'] = end_time.strftime('%Y-%m-%dT%H:%M:00Z')

                downtimes.append(entry)

        return downtimes
