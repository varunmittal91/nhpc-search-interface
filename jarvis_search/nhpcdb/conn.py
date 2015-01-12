from django.conf import settings

from cassandra.cluster import Cluster
from cassandra import InvalidRequest
from cassandra.query import dict_factory

class CassandraClient:
    def __init__(self, keyspace):
        SERVERS = getattr(settings, 'CS_HOSTS', [])
        cluster = Cluster(SERVERS)
        try:
            self.session = cluster.connect(keyspace)
        except InvalidRequest as e:
            session = cluster.connect()
            session.execute("CREATE KEYSPACE %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};" % keyspace)
            session.set_keyspace(keyspace)
            self.session = session
        self.session.row_factory = dict_factory
    def put(self, table_name, table_schema, rows):
        keys = []
        keys_append = keys.append
        types = []
        types_append = types.append
        indexed = []
        indexed_append = indexed.append
        [(keys_append(_key), types_append(_type), indexed_append(_indexed)) for _key, _type, _indexed in table_schema]

        command = "insert into %s (%s) values (%s)"% (table_name, ", ".join(keys), ", ".join(rows))
        try:
            self.session.execute(command)
        except InvalidRequest:
            desc_query = "select * from system.schema_columns where keyspace_name='nhpcdb' and columnfamily_name = '%s'" % (table_name)
            rows = self.session.execute(desc_query)
            if len(rows) > 0:
                add_fields = []
                change_fields = []
                add_fields = set(keys) - set([row.column_name for row in rows])
                features = ["alter table %s ADD %s %s " % (table_name, key, column_type) for key,column_type in zip(keys, types) if key in add_fields]
                for feature in features:
                    self.session.execute(feature)
            else:
                features = ["%s %s" % (key, column_type) for key,column_type in zip(keys, types)]
                query = "create table %s (%s, PRIMARY KEY (key))" % (table_name, ", ".join(features))
                self.session.execute(query)
                for key, index in zip(keys, indexed):
                    if index == 'True':
                        index_command = "create index %s_%s on %s (%s)" % (table_name, key, table_name, key)
                        self.session.execute(index_command)
            self.session.execute(command)
    def query(self, t_name, columns, conditions, limit):
        if columns:
            columns.append("key")
            columns = "%s, key" % (",".join(columsn))
        else:
            columns = "*"
        if conditions:
            cond = "where %s" % " and ".join([condition for condition in conditions])
        try:
            query = "select %s from  %s %s limit %d" % (columns, t_name, cond, limit)
        except TypeError:
            query = "select %s from %s %s" % (columns, t_name, cond)
        try:
            rows = cassandra_conn.session.execute(query)
        except InvalidRequest:
            rows = []
        return rows
cassandra_conn = CassandraClient('nhpcdb')
def Query(*argv):
    return cassandra_conn.query(*argv)
