import requests
import json
from zlib import compress as zlib_compress, decompress as zlib_decompress
from requests.auth import HTTPBasicAuth

from django.conf import settings

try:
    from jarvis_frontend.utilities import isDevelopmentServer
    is_appengine_env = True
except:
    is_appengine_env = False

class CassandraClientWeb:
    def __init__(self, keyspace):
        SERVERS = getattr(settings, 'CS_HOSTS', [])
        if is_appengine_env:
            if isDevelopmentServer():
               	__servers = []
                for server in SERVERS:
                    if 'production' in server and server['production']:
                        continue
                    __servers.append(server)
                SERVERS = __servers
        for server in SERVERS:
            if 'use_ssl' and server['use_ssl'] == True:
                url = "https://%s:%s/" % (server['host'], server['port'])
            else:
                url = "http://%s:%s/" % (server['host'], server['port'])
            server['url'] = url
        self.SERVERS = SERVERS
    def put(self, table_name, table_schema, rows, db_opts=None):
        if db_opts:
            db_opts.addOp({'schema': table_schema, 'rows': rows, 't_name': table_name})
            return
        server = self.SERVERS[0]
        credentials = server['http_auth'].split(':')
        r = requests.post("%s/nhpcdb/put/" % server['url'], auth=HTTPBasicAuth(credentials[0], credentials[1]), 
                data=zlib_compress(json.dumps({'schema': table_schema, 'rows': rows, 't_name': table_name})))
    def put_multi(self, db_opts):
        server = self.SERVERS[0]
        credentials = server['http_auth'].split(':')
        for opts in db_opts.get_opts():
            r = requests.post("%s/nhpcdb/put/" % server['url'], auth=HTTPBasicAuth(credentials[0], credentials[1]),
                    data=zlib_compress(json.dumps(opts)), timeout=60)
    def query(self, t_name, columns, conditions, limit):
        server = self.SERVERS[0]
        credentials = server['http_auth'].split(':')
        r = requests.post("%s/nhpcdb/query/" % server['url'], auth=HTTPBasicAuth(credentials[0], credentials[1]),
                data=zlib_compress(json.dumps({'t_name': t_name, 'columns': columns, 'cond': conditions, 'limit': limit})), timeout=60)
        rows = json.loads(zlib_decompress(r.content))
        return rows
cassandra_conn = CassandraClientWeb('nhpcdb')
