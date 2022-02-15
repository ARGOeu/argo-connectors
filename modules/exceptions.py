class ConnectorParseError(Exception):
    def __init__(self, msg=None):
        self.msg = msg


class ConnectorHttpError(Exception):
    def __init__(self, msg=None):
        self.msg = msg
