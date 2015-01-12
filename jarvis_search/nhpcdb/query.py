from copy import deepcopy

try:
    from .conn import cassandra_conn
except ImportError:
    from .conn_webi import cassandra_conn

class NhpcDBQueryObject:
    def __init__(self, trgt_class, conditions):
        cond = ""
        if conditions:
            cond = "where %s" % " and ".join([condition for condition in conditions])
        self.__query = "from %s %s" % (trgt_class.name, cond)
        self.__model_t = trgt_class
        self.__t_name  = trgt_class.name
        self.__cond    = conditions
    def fetch(limit=10):
        pass
    def fetch_async(self, limit=1000, projection=[]):
        results = []

        rows = cassandra_conn.query(self.__t_name, projection, self.__cond, limit)
        for row in rows:
            new_model = deepcopy(self.__model_t)(key=row['key'])
            new_model.loadRow(row, projection)
            results.append(new_model)
        return results
