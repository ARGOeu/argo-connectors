import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_contacts import ParseSiteContacts
from argo_egi_connectors.io.http import ConnectorError


logger = Logger('test_contactfeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


class ParseSitesContactTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-site_contacts.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        parse_sites_contacts = ParseSiteContacts(logger, self.content, CUSTOMER_NAME)
        self.contacts = parse_sites_contacts.get_contacts()

    def test_lenContacts(self):
        self.assertEqual(len(self.contacts), 9)

    def test_malformedContacts(self):
        self.assertRaises(ConnectorError, ParseSiteContacts, logger, 'wrong mocked data', CUSTOMER_NAME)

    def test_formatContacts(self):
        import ipdb; ipdb.set_trace()

if __name__ == '__main__':
    unittest.main()
