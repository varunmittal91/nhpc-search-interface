import json
from zlib import decompress as zlib_decompress, compress as zlib_compress

from django.http import HttpResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_exempt

from cassandra import InvalidRequest

from .conn import cassandra_conn

@csrf_exempt
def put(request):
    results = []
    try:
        data = eval(zlib_decompress(request.body))
        if type(data) == list:
            for row in data:
                results.append(cassandra_conn.put(**row))
        else:
            table  = data['t_name'].lower()
            schema = data['schema']
            rows   = data['rows']
            results.append(cassandra_conn.put(table_name=table, table_schema=schema, rows=rows))
    except KeyError:
        return HttpResponseBadRequest("Invalid request")
    return HttpResponse()

@csrf_exempt
def query(request):
    try:
        data = eval(zlib_decompress(request.body))
        t_name = data['t_name']
        columns = data['columns']
        conditions = data['cond']
        limit = data['limit']
    except KeyError:
        return HttpResponseBadRequest("Invalid request")
    rows = cassandra_conn.query(t_name=t_name, columns=columns, limit=limit, conditions=conditions)
    return HttpResponse(zlib_compress(json.dumps(rows)))
