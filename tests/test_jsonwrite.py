import unittest
import json
from unittest.mock import mock_open, patch, Mock
import os

from argo_connectors.io.jsonwrite import JsonWriter

mock_json = [
    {
        "type": "NGI",
        "group": "iris.ac.uk",
        "subgroup": "dirac-durham",
        "tags": {
            "certification": "Certified",
            "scope": "iris.ac.uk",
            "infrastructure": "Production"
        },
        "notifications": {
            "contacts": [
                "a.g.basden@durham.ac.uk"
            ],
            "enabled": "true"
        }
    }
]

mock_filename = "mock_file.json"

class TestJsonWriter(unittest.TestCase):

    def tearDown(self):
        if os.path.exists(mock_filename + '.gz'):
            os.remove(mock_filename + '.gz')   

        if os.path.exists(mock_filename):
            os.remove(mock_filename)   


    def test_write_compressed_json(self):
        with patch('gzip.open', mock_open()) as m:
            data_writer = JsonWriter(mock_json, 'mock_file.json', 'True')
            data_writer.write_json()    


        m.assert_called_once_with('mock_file.json.gz', 'wb')
        handle = m()
        handle.write.assert_called_once_with(
            b'[\n    {\n        "type": "NGI",\n        "group": "iris.ac.uk",\n        "subgroup": "dirac-durham",\n        "tags": {\n            "certification": "Certified",\n            "scope": "iris.ac.uk",\n            "infrastructure": "Production"\n        },\n        "notifications": {\n            "contacts": [\n                "a.g.basden@durham.ac.uk"\n            ],\n            "enabled": "true"\n        }\n    }\n]')

        writer = JsonWriter(mock_json, 'mock_file.json', 'True')
        success, error = writer.write_json()

        self.assertTrue(success)
        self.assertIsNone(error)


    def test_write_json(self):
        with patch('builtins.open', mock_open()) as m:
            data_writer = JsonWriter(mock_json, 'mock_file.json', 'False')
            data_writer.write_json()

        m.assert_called_once_with('mock_file.json', 'w')
        handle = m()
        handle.write.assert_called_once_with(
            '[\n    {\n        "type": "NGI",\n        "group": "iris.ac.uk",\n        "subgroup": "dirac-durham",\n        "tags": {\n            "certification": "Certified",\n            "scope": "iris.ac.uk",\n            "infrastructure": "Production"\n        },\n        "notifications": {\n            "contacts": [\n                "a.g.basden@durham.ac.uk"\n            ],\n            "enabled": "true"\n        }\n    }\n]')


    def test_fail_jsonwrite(self):
        mock_jsondumps = Mock(name='json.dumps')
        mock_jsondumps.side_effect = json.JSONDecodeError(
            "Mocked error", '', 0)

        with patch('json.dumps', mock_jsondumps):
            writer = JsonWriter('mock_key: mock_value', mock_filename, 'False')
            result, error = writer.write_json()

            self.assertEqual(result, False)
            self.assertEqual(str(error), "Mocked error: line 1 column 1 (char 0)")


if __name__ == '__main__':
    unittest.main()
