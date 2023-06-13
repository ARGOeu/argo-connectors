import json
import gzip


class JsonWriter(object):
    def __init__(self, data, filename, compress_json):
        self.data = data
        self.filename = filename
        self.compress_json = compress_json

    def write_json(self):
        try:
            if self.compress_json == str(True):
                json_data = json.dumps(self.data, indent=4)

                with gzip.open(self.filename + '.gz', 'wb') as f:
                    f.write(json_data.encode())

                return True, None
            
            else:
                json_data = json.dumps(self.data, indent=4)

                with open(self.filename, 'w') as f:
                    f.write(json_data)

                return True, None

        except Exception as e:
            return False, e
