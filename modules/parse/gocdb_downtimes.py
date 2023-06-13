import datetime
from lxml import etree
from lxml.etree import XMLSyntaxError

from argo_connectors.utils import module_class_name
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers


class ParseDowntimes(ParseHelpers):
    def __init__(self, logger, data, start, end, uid=False):

        self.logger = logger
        self.data = data
        self.start = start
        self.end = end
        self.uid = uid

    def get_data(self):
        filtered_downtimes = list()

        try:
            doc = self.parse_xml(self.data)
            xml_bytes = doc.encode("utf-8")
            root = etree.fromstring(xml_bytes)

            for downtimes in root:
                classification = downtimes.attrib['CLASSIFICATION']
     
                for downtime in downtimes.xpath('.//HOSTNAME'):
                    hostname = downtime.text

                for downtime in downtimes.xpath('.//SERVICE_TYPE'):
                    service_type = downtime.text

                for downtime in downtimes.xpath('.//FORMATED_START_DATE'):
                    start_str = downtime.text

                for downtime in downtimes.xpath('.//FORMATED_END_DATE'):
                    end_str = downtime.text

                for downtime in downtimes.xpath('.//SEVERITY'):
                    severity = downtime.text

                try:
                    for downtime in downtimes.xpath('.//PRIMARY_KEY'):
                        service_id = downtime.text
                except ImportError:
                    service_id = downtime.getAttribute('PRIMARY_KEY')

                start_time = datetime.datetime.strptime(
                    start_str, "%Y-%m-%d %H:%M")
                end_time = datetime.datetime.strptime(
                    end_str, "%Y-%m-%d %H:%M")

                if start_time < self.start:
                    start_time = self.start
                if end_time > self.end:
                    end_time = self.end

                if classification == 'SCHEDULED' and severity == 'OUTAGE':
                    downtime = dict()
                    if self.uid:
                        downtime['hostname'] = '{0}_{1}'.format(
                            hostname, service_id)
                    else:
                        downtime['hostname'] = hostname
                    downtime['service'] = service_type
                    downtime['start_time'] = start_time.strftime(
                        '%Y-%m-%dT%H:%M:00Z')
                    downtime['end_time'] = end_time.strftime(
                        '%Y-%m-%dT%H:%M:00Z')
                    filtered_downtimes.append(downtime)

            return filtered_downtimes

        except (KeyError, IndexError, AttributeError, TypeError, AssertionError, XMLSyntaxError) as exc:
            msg = '{} Customer:{} : Error parsing downtimes feed - {}'.format(
                module_class_name(self), self.logger.customer, repr(exc))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc
