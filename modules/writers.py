import avro.schema
from argo_egi_connectors.config import VOConf, EGIConf
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from exceptions import IOError

class AvroWriter:
    """ AvroWriter """
    def __init__(self, schema, outfile, listdata):
        self.schema = schema
        self.listdata = listdata
        self.outfile = outfile

    def write(self):
        try:
            schema = avro.schema.parse(open(self.schema).read())
            avrofile = open(self.outfile, 'w+')
            datawrite = DataFileWriter(avrofile, DatumWriter(), schema)

            for elem in self.listdata:
                datawrite.append(elem)

            datawrite.close()
            avrofile.close()

        except (avro.schema.SchemaParseException, avro.io.AvroTypeException):
            print self.__class__, " couldn't parse %s" % self.schema
        except IOError as e:
            print self.__class__, e
