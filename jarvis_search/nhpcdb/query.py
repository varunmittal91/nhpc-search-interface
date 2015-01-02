from copy import deepcopy

from .conn import cassandra_conn

class NhpcDBQueryObject:
    def __init__(self, trgt_class, conditions):
        cond = ""
        if conditions:
            cond = "where %s" % " and ".join([condition for condition in conditions])
        self.__query = "from %s %s" % (trgt_class.name, cond)
        self.__model_t = trgt_class
    def fetch(limit=10):
        pass
    def fetch_async(self, limit=10, projection=[]):
        results = []

        if projection:
            projection.append("key")
            columns = "%s, key" % (",".join(projection))
        else:
            columns = "*"
        query = "select %s %s limit %d" % (columns, self.__query, limit)
        rows = cassandra_conn.query(query)
        for row in rows:
            new_model = deepcopy(self.__model_t)
            new_model.__init__(key=row.key)
            new_model.reInit()
            new_model.loadRow(row, projection)
            results.append(new_model)
        return results
