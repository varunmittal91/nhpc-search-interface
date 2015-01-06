from django.conf import settings

from cassandra.cluster import Cluster
from cassandra import InvalidRequest

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
    def put(self, table, fields):
        pass
    def query(self, query):
        try:
           return self.session.execute(query)
        except InvalidRequest as e:
           return []

cassandra_conn = CassandraClient('nhpcdb')
def Query(*argv):
    return cassandra_conn.query(*argv)
