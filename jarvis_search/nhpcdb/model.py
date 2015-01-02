from uuid import uuid4
import zlib
import json
import pickle
import calendar
from collections import OrderedDict
from datetime import datetime

from cassandra import InvalidRequest, AlreadyExists

from .exceptions import NhpcDBInvalidAttribute, NhpcDBFieldNotImplemented, NhpcDBFieldRequired, NhpcDBInvalidValue, NhpcDBInvalidProperty 
from .query import NhpcDBQueryObject
from .conn import cassandra_conn

cassandra_types = {
    'integer': 'int',
    'string': 'varchar',
    'blob': 'blob',
    'datetime': 'timestamp'
}

class DBProperty(object):
    def __init__(self, required=False, default=None, validator=None, repeated=None, **kwargs):
        try:
            del self._value
        except AttributeError:
            pass

        if default:
            self._value = default
        self._required = required
    def reInit(self):
        try:
            del self._value
        except AttributeError:
            pass
    def setValue(self, value):
        raise NhpcDBFieldNotImplemented(self.__class__.__name__)
    def getValue(self, attr, is_new):
        self._attr = attr
        try:
            return {'attr': attr, 'value': self._value, '_t': self._type}
        except AttributeError:
            if self._required:
                if is_new:
                    raise NhpcDBFieldRequired(self.__class__.__name__, attr)
            return {'attr': attr, 'value': 'null', '_t': self._type}
    def loadValue(self, value):
        self._value = value.replace("''", "'")
    def readValue(self):
        try:
            return self._value
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

    def __init__(self, default=None, required=False):
        super(self.__class__, self).__init__(default=default, required=required)
    def setValue(self, value):
        self._value = int(value)

class DateTimeProperty(DBProperty):
    _type = 'datetime'

    def __init__(self, default=None, required=False, auto_now=False, auto_now_add=False):
        if default:
            self._value = default
            self.__validate()
        try:
            assert isinstance(auto_now, bool)
            assert isinstance(auto_now_add, bool)
        except AssertionError:
            raise NhpcDBInvalidProperty(["auto_now", "auto_now_add"], "bool")
        super(self.__class__, self).__init__(required=required)
        self._auto_now = auto_now
        self._auto_now_add = auto_now_add            
    def setValue(self, value):
        self._value = value
        self.__validate()
    def getValue(self, attr, is_new):
        if self._auto_now or (self._auto_now_add and is_new):
            self._value = datetime.now()
            self._validate()
        return super(self.__class__, self).getValue(attr, is_new)
    def __validate(self):
        if not isinstance(self._value, datetime):
            raise NhpcDBInvalidValue(self.__class__.__name__, datetime.__name__)
        self._value = calendar.timegm(self._value.utctimetuple())
    def _operation(self, value, symbol):
        if not isinstance(value, datetime):
            raise NhpcDBInvalidValue(self.__class__.__name__, datetime.__name__)
        value = calendar.timegm(value.utctimetuple())
        return "%s %s %s" % (self._attr, symbol, value)
    def loadValue(self, value):
        self.setValue(value)
    def readValue(self):
        try:
            return datetime.fromtimestamp(self._value)
        except AttributeError:
            return

DateProperty = DateTimeProperty
TimeProperty = DateTimeProperty

class StringProperty(DBProperty):
    _type = 'string'

    def __init__(self, required=False, default=None):
        if default:
            self._value = "'%s'" % default
        super(StringProperty, self).__init__(required=required)
    def setValue(self, value):
        self._value = "'%s'" % value.replace("'", "''")
    def getValue(self, attr, is_new):
        return super(self.__class__, self).getValue(attr, is_new)
    def _operation(self, value, symbol):
        return "%s %s '%s'" % (self._attr, symbol, value)
    def readValue(self):
        try:
            return self._value.replace("''", "'")
        except AttributeError:
            return

class BlobProperty(DBProperty):
    _type = 'blob'

    def __init__(self, compressed=False, required=False, default=None):
        self._compressed=compressed
        if default:
            self._value = default
            self.__validate()
        super(BlobProperty, self).__init__(required=required)
    def getValue(self, attr, is_new):
        try:
            return {'attr': attr, 'value': "textAsBlob('%s')" % self._value.encode("hex"), '_t': self._type}
        except AttributeError: 
            if self._required:
                if is_new:
                    raise NhpcDBFieldRequired(self.__class__.__name__, attr)
            return {'attr': attr, 'value': 'null', '_t': self._type}
    def setValue(self, value):
        self._value = value
        self.__validate()
    def __validate(self):
        if self._compressed:
            self._value = zlib.compress(self._value)
    def loadValue(self, value):
        self._value = value.decode('hex')
    def readValue(self):
        return self._decompress()
    def _decompress(self):
        if self._compressed:
            return zlib.decompress(self._value)
        return self._value

TextProperty = BlobProperty
class JsonProperty(BlobProperty):
    def __init__(self, **kwargs):
        default = kwargs.get('default', None)
        required = kwargs.get('required', None)
        if default:
            json.loads(default)
        super(JsonProperty, self).__init__(kwargs)
    def setValue(self, value):
        json.loads(value)
        self._value = value
        super(JsonProperty, self).setValue(value)
class PickleProperty(BlobProperty):
    def __init__(self, **kwargs):
        default = kwargs.get('default', None)
        required = kwargs.get('required', None)
        if default:
            kwargs['default'] = pickle.dumps(value)
        super(PickleProperty, self).__init__(kwargs)
    def setValue(self, value):
        value = pickle.dumps(value)
        super(PickleProperty, self).setValue(value)
    def readValue(self):
        try:
            value = self._decompress()
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
        super(BaseClass, self).__init__(name, bases, attr)
        self.name = super(BaseClass, self).__name__.lower()

class Models(object):
    __metaclass__ = BaseClass
    _key = StringProperty()

    def __init__(self, **kwargs):
        try:
            self.__key = kwargs['key']
            self._new_instance = False
        except KeyError:
            self._new_instance = True
            self.__key = str(uuid4())
        self._key.setValue(self.__key)
            
        for attr,value in kwargs.items():
            try:
                column = self._columns[attr]
                column.setValue(value)
            except KeyError:
                raise NhpcDBInvalidAttribute(self.__class__.__name__, attr)
        self._attrs = []
        attrs_append = self._attrs.append
        for attr,column in self._columns.items():
            value = column.getValue(attr, self._new_instance)
            if value:
                attrs_append(value)
    def __getattribute__(self, name):
        _columns = super(Models, self).__getattribute__('_columns')
        try:
            return _columns[name].readValue()
        except KeyError:
            return super(Models, self).__getattribute__(name)
    def loadRow(self, row, projection):
        if not projection:
            projection = [self._columns.keys()]
        for key in projection:
            value = getattr(row, key, None)
            if value:
                self._columns[key].loadValue(value)
    def reInit(self):
        for column in self._columns.values():
            column.reInit()
    def key(self):
        return self._key
    @classmethod
    def query(*args):
        return NhpcDBQueryObject(args[0](), args[1:])
    def put(self):
        values = [str(attr['value']) for attr in self._attrs]
        command = "insert into %s (%s) values (%s)"% (self.__class__.__name__, ", ".join(self._columns.keys()), ", ".join(values))
        try:
            cassandra_conn.session.execute(command)
        except InvalidRequest as e:
            desc_query = "select * from system.schema_columns where keyspace_name='nhpcdb' and columnfamily_name = '%s'" % (self.__class__.__name__.lower())
            rows = cassandra_conn.session.execute(desc_query)
            if len(rows) > 0:
                add_fields = []
                change_fields = []
                add_fields = set(self._columns.keys()) - set([row.column_name for row in rows])
                features = ["alter table %s ADD %s %s " % (self.__class__.__name__, key, cassandra_types[column._type]) for key, column in self._columns.items() if key in add_fields]
                for feature in features:
                    cassandra_conn.session.execute(feature)
            else:
                features = ["%s %s" % (key, cassandra_types[column._type]) for key,column in self._columns.items()]
                query = "create table %s (%s, PRIMARY KEY (key))" % (self.__class__.__name__, ", ".join(features))
                cassandra_conn.session.execute(query)
            cassandra_conn.session.execute(command)
