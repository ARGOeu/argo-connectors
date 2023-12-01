import json
import gzip


from argo_connectors.singleton_config import ConfigClass


class JsonWriter(object):
    def __init__(self, data, filename):#, compress_json):
        self.data = data
        self.filename = filename
        #self.compress_json = compress_json

        self.config = ConfigClass()
        self.args = self.config.parse_args()
        self.cglob = self.config.get_cglob(self.args)
        self.globopts = self.config.get_globopts(self.cglob)
        self.compress_json = eval(self.globopts['GeneralCompressJson'.lower()])

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
