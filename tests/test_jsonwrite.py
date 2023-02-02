import unittest
from unittest.mock import patch, mock_open, Mock
import json

from argo_connectors.io.jsonwrite import JsonWriter

mock_json = [
    {'id': 5414470, 'name': 'mike'},
    {'id': 5414472, 'name': 'tom'},
    {'id': 5414232, 'name': 'pete'},
]

mock_filename = "mock_file.json"


class JsonWriteTest(unittest.TestCase):

    def test_save_data_to_file(self):
        with patch('builtins.open', mock_open()) as m:
            data_writer = JsonWriter(mock_json, 'mock_file.json')
            data_writer.write_json()

        m.assert_called_once_with('mock_file.json', 'w')
        handle = m()
        handle.write.assert_called_once_with(
            '[\n    {\n        "id": 5414470,\n        "name": "mike"\n    },\n    {\n        "id": 5414472,\n        "name": "tom"\n    },\n    {\n        "id": 5414232,\n        "name": "pete"\n    }\n]')

    def test_fail_jsonwrite(self):
        mock_jsondumps = Mock(name='json.dumps')
        mock_jsondumps.side_effect = json.JSONDecodeError(
            "Mocked error", '', 0)

        with patch('json.dumps', mock_jsondumps):
            writer = JsonWriter('mock_key: mock_value', mock_filename)
            result, error = writer.write_json()

            self.assertEqual(result, False)
            self.assertEqual(
                str(error), "Mocked error: line 1 column 1 (char 0)")


if __name__ == '__main__':
    unittest.main()
