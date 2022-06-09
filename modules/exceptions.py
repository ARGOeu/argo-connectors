class ConnectorError(Exception):
    def __init__(self, msg=None):
        self.msg = msg

class ConnectorParseError(ConnectorError):
    def __init__(self, msg=None):
        self.msg = msg


class ConnectorHttpError(ConnectorError):
    def __init__(self, msg=None):
        self.msg = msg
