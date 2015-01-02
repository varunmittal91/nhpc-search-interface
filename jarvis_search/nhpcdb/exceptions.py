class NhpcDBException(Exception):
    def __str__(self):
        return self._msg

class NhpcDBInvalidProperty(NhpcDBException):
    def __init__(self, props, data_type):
        self._msg = "'%s' properties invalid, should be %s" % (" ".join(props), data_type)

class NhpcDBInvalidAttribute(NhpcDBException):
    def __init__(self, classname, attrname):
        self._msg = "'%s' attribute not implemented in '%s'" % (attrname, classname)

class NhpcDBFieldNotImplemented(NhpcDBException):
    def __init__(self, classname):
        self._msg = "'%s' field not implemented" % classname

class NhpcDBFieldRequired(NhpcDBException):
    def __init__(self, classname, attrname):
        self._msg = "'%s' field required for '%s'" % (classname, attrname)

class NhpcDBInvalidValue(NhpcDBException):
    def __init__(self, classname, req_classname):
        self._msg = "'%s' should be '%s'" % (classname, req_classname)
