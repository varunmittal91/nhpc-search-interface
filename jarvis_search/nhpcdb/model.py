from uuid import uuid4
import zlib
import json
try:
    import cPickle as pickle
except ImportError:
    import pickle
import calendar
from collections import OrderedDict
from datetime import datetime, timedelta

#from cassandra import InvalidRequest, AlreadyExists

from .exceptions import NhpcDBInvalidAttribute, NhpcDBFieldNotImplemented, NhpcDBFieldRequired, NhpcDBInvalidValue, NhpcDBInvalidProperty 
from .query import NhpcDBQueryObject
from .conn_common import db_opts
try:
    from .conn import cassandra_conn
except ImportError:
    from .conn_webi import cassandra_conn

cassandra_types = {
    'integer': 'int',
    'string': 'varchar',
    'blob': 'blob',
    'datetime': 'timestamp'
}

class DBProperty(object):
    required = False
    default  = None
    validator = None
    repeated = None

    def __init__(self, required=False, default=None, validator=None, repeated=None, indexed = False, **kwargs):
        self._indexed = indexed
        if default:
            self._value = default
        self._required = required
    def setValue(self, value):
        raise NhpcDBFieldNotImplemented(self.__class__.__name__), self._attr
    def getValue(self, attr, is_new, value=None):
        self._attr = attr
        if value:
            return {'attr': attr, 'value': value, '_t': self._type, 'indexed': self._indexed}
        else:
            return {'attr': attr, 'value': 'null', '_t': self._type, 'indexed': self._indexed}
    def getDefault(self, is_new=True):
        try:
            return self._value
        except AttributeError:
            if is_new and self._required:
                raise NhpcDBFieldRequired(self.__class__.__name__, self._attr)
            return
    def readValue(self, value):
        try:
            return value
        except AttributeError:
            return
    def _operation(self, value, symbol):
        return "%s %s %s" % (self._attr, symbol, value)
    def __eq__(self, value):
        return self._operation(value, "=") 
    def __lte__(self, value):
        return self._operation(value, "<=")
    def __lt__(self, value):
        return self._operation(value, "<")
    def __gte__(self, value):
        return self._operation(value, ">=")
    def __gt__(self, value):
        return self._operation(value, ">")

class IntegerProperty(DBProperty):
    _type = 'integer'

    def __init__(self, default=None, required=False, indexed=False):
        super(self.__class__, self).__init__(default=default, required=required, indexed=indexed)
    def setValue(self, value):
        return int(value)

class DateTimeProperty(DBProperty):
    _type = 'datetime'

    def __init__(self, default=None, required=False, auto_now=False, auto_now_add=False, indexed=False):
        if default:
            self._value = self.__validate(default)
        try:
            assert isinstance(auto_now, bool)
            assert isinstance(auto_now_add, bool)
        except AssertionError:
            raise NhpcDBInvalidProperty(["auto_now", "auto_now_add"], "bool")
        super(self.__class__, self).__init__(required=required, indexed=indexed)
        self._auto_now = auto_now
        self._auto_now_add = auto_now_add            
    def setValue(self, value):
        return self.__validate(value)
    def getDefault(self, is_new=True):
        if (is_new and self._auto_now_add) or self._auto_now:
            return self.__validate(datetime.now())
        return super(DateTimeProperty, self).getDefault()
    def getValue(self, attr, is_new, value=None):
        if self._auto_now or (self._auto_now_add and is_new):
            value = self.__validate(datetime.now())
        return super(DateTimeProperty, self).getValue(attr, is_new, value)
    def __validate(self, value):
        if not isinstance(value, datetime):
            raise NhpcDBInvalidValue(self.__class__.__name__, datetime.__name__)
        value = (value - datetime.utcfromtimestamp(0))
        value = long(value.total_seconds() * 1000.0)
        return value
    def _operation(self, value, symbol):
        if not isinstance(value, datetime):
            raise NhpcDBInvalidValue(self.__class__.__name__, datetime.__name__)
        value = calendar.timegm(value.utctimetuple())
        return "%s %s %s" % (self._attr, symbol, value)
    def readValue(self, value):
        if isinstance(value, datetime):
            return value
        return datetime.fromtimestamp(long(value)/1e3)

DateProperty = DateTimeProperty
TimeProperty = DateTimeProperty

class StringProperty(DBProperty):
    _type = 'string'

    def __init__(self, required=False, default=None, indexed=False):
        if default:
            self._value = "'%s'" % default
        super(StringProperty, self).__init__(required=required, indexed=indexed)
    def setValue(self, value):
        return "'%s'" % value.replace("'", "''")
    def getValue(self, attr, is_new, value=None):
        return super(self.__class__, self).getValue(attr, is_new, value)
    def _operation(self, value, symbol):
        return "%s %s '%s'" % (self._attr, symbol, value)
    def readValue(self, value):
        return value.replace("''", "'")

class BlobProperty(DBProperty):
    _type = 'blob'

    def __init__(self, compressed=False, required=False, default=None):
        self._compressed=compressed
        if default != None:
            self._value = self.__validate(default)
        super(BlobProperty, self).__init__(required=required)
    def getValue(self, attr, is_new, value=None):
        if value:
            return {'attr': attr, 'value': "textAsBlob('%s')" % value.encode("hex"), '_t': self._type, 'indexed': self._indexed}
        else: 
            return {'attr': attr, 'value': 'null', '_t': self._type, 'indexed': self._indexed}
    def setValue(self, value):
        return self.__validate(value)
    def __validate(self, value):
        if self._compressed:
            value = zlib.compress(value)
        return value
    def readValue(self, value):
        return self._decompress(value)
    def _decompress(self, value):
        try:
            value = value.decode('hex')
        except TypeError:
            pass
        if self._compressed:
            return zlib.decompress(value)
        return value

TextProperty = BlobProperty
class JsonProperty(BlobProperty):
    def __init__(self, **kwargs):
        default = kwargs.get('default', None)
        required = kwargs.get('required', None)
        if default:
            json.loads(default)
        super(JsonProperty, self).__init__(**kwargs)
    def setValue(self, value):
        json.loads(value)
        return super(JsonProperty, self).setValue(value)
class PickleProperty(BlobProperty):
    def __init__(self, **kwargs):
        default = kwargs.get('default', None)
        required = kwargs.get('required', None)
        if default != None:
            kwargs['default'] = pickle.dumps(default)
        super(PickleProperty, self).__init__(**kwargs)
    def setValue(self, value):
        return super(PickleProperty, self).setValue(pickle.dumps(value))
    def readValue(self, value):
        try:
            value = self._decompress(value)
            return pickle.loads(value)
        except AttributeError:
            return None

class BaseClass(type):
    def __init__(self, name, bases, attr, **kwargs):
        self._columns = OrderedDict()
        try:
            del attr['key']
        except KeyError:
            pass
        attr['key'] = self._key
        for field_name, field_type in attr.items():
            if isinstance(field_type, DBProperty):
                self._columns[field_name] = field_type
                field_type.getValue(field_name, False)
        super(BaseClass, self).__init__(name, bases, attr)
        self.name = super(BaseClass, self).__name__.lower()

class Models(object):
    __metaclass__ = BaseClass
    _key = StringProperty()

    def __init__(self, **kwargs):
        self._attributes = {}
        columns = self._columns
        attributes = self._attributes
        try:
            self.__key = kwargs['key']
            self._new_instance = False
        except KeyError:
            self._new_instance = True
            self.__key = str(uuid4())
            for key in set(columns.keys()) - set(['keys']):
                column = columns[key]
                try:
                    attributes[key] = column.setValue(kwargs[key])
                except KeyError:
                    attributes[key] = column.getDefault()
            try:
                raise NhpcDBInvalidAttribute(self.__class__.__name__, [key for key in set(kwargs.keys()) - set(attributes.keys())][0])
            except IndexError:
                pass
        attributes['key'] = self._key.setValue(self.__key)
    def getFields(self):
        return self._attributes.keys()
    def __getattribute__(self, name):
        _columns = super(Models, self).__getattribute__('_columns')
        try:
            column = _columns[name]
            _attributes = super(Models, self).__getattribute__('_attributes')
            value = _attributes.get(name)
            if not value:
                return
            return column.readValue(value)
        except (KeyError, AttributeError):
            return super(Models, self).__getattribute__(name)
    def loadRow(self, row, projection):
        if not projection:
            projection = self._columns.keys()
        for key in projection:
            value = row.get(key, None)
            self._attributes[key] = value
    def key(self):
        return self._key
    @classmethod
    def query(*args):
        return NhpcDBQueryObject(args[0], args[1:])
    def put(self, db_opts=None):
        values = []
        values_append = values.append
        attributes = self._attributes
        for attr,column in self._columns.items():
            value = column.getValue(attr, self._new_instance, attributes[attr])
            values_append(str(value['value']))
        db_opt = {'table_name': self.__class__.__name__, 
	          'table_schema': [[key, cassandra_types[column._type], str(column._indexed)] for key,column in self._columns.items()], 
                  'rows': values}
        if not db_opts:
            return cassandra_conn.put(**db_opt)
        db_opts.addOp(db_opt)

def put_multi(models):
    db_opt = db_opts()
    [model.put(db_opt) for model in models]
    return cassandra_conn.put_multi(db_opt)
