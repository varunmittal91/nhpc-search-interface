#
#       (C) 2013 Varun Mittal <varunmittal91@gmail.com>
#       JARVIS program is distributed under the terms of the GNU General Public License v3
#
#       This file is part of JARVIS.
#
#       JARVIS is free software: you can redistribute it and/or modify
#       it under the terms of the GNU General Public License as published by
#       the Free Software Foundation version 3 of the License.
#
#       JARVIS is distributed in the hope that it will be useful,
#       but WITHOUT ANY WARRANTY; without even the implied warranty of
#       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#       GNU General Public License for more details.
#
#       You should have received a copy of the GNU General Public License
#       along with JARVIS.  If not, see <http://www.gnu.org/licenses/>.
#

import sys
import base64
import simplejson as json
import requests
from requests.auth import HTTPBasicAuth
from copy import deepcopy
is_appengine_environment = False

try:
    from google.appengine.runtime.apiproxy_errors import DeadlineExceededError
    from google.appengine.api import urlfetch
    is_appengine_environment = True
except ImportError:
    pass

from elasticsearch import helpers
from elasticsearch.exceptions import TransportError

from .exceptions import IndexException
from .conn import ElasticSearchClient
from .scored_document import EsSearchDocument, EsFieldBase
es_client_conn = ElasticSearchClient()

class EsIndex:
    transaction_header_size = 100
    max_transaction_size = 10000000 - transaction_header_size

    def __init__(self, name):
        self.__name = name.lower()
    def get(self, search_doc_id, doc_type):
        document = es_client_conn.es.get(doc_type=doc_type, id=search_doc_id, index=self.__name, ignore=[400, 404])
        if 'found' not in document or not document['found']:
            return None
        document = document['_source']
        fields = []
        for name, value in zip(document.keys(), document.values()):
            fields.append(EsFieldBase(name=name, value=value))
        doc = EsSearchDocument(rank=document['_rank'], doc_type=doc_type, fields=fields, id=search_doc_id)
        return doc
    def update(self, doc, update_fields, doc_type):
        actions = [{
            "_op_type": 'update',
            "_index": self.__name,
            "_type": doc_type,
            "_id": doc.doc_id,
            "doc": {},
        }]
        for field in update_fields:
            actions[0]['doc'][field.name] = field.value
        results = helpers.bulk(client=es_client_conn.es, actions=actions,)
        del actions
        return results[0]
    def put(self, documents):
        __documents = []
        results = []
        if type(documents) == list:
            for document in documents:
                __documents.append(document.getDoc(self.__name))
        else:
            __documents.append(documents.getDoc(self.__name))
        actions = EsActions()
        for document in __documents:
            action = {
                "_index": document['index'],
                "_type": document['doc_type'],
                "_id": document['id'],
                "_source": document['body'],
            }
            actions.addAction(action)
        actions.push()
        results = actions.push()
        del actions
        return results
    def delete(self, search_doc_ids, doc_type):
        actions = []
        if type(search_doc_ids) != list:
            search_doc_ids = [search_doc_ids]
        actions = EsActions()
        for doc_id in search_doc_ids:
            actions.addAction({
                "_op_type": 'delete',
                "_index": self.__name,
                "_type": doc_type,
                "_id": doc_id,
            })
        actions.push()
        results = actions.getResults()
        del actions
        return results
    def __put__(self, actions):
        results = [action['_id'] for action in actions]
        if is_appengine_environment:
            retry_count = 0
            while retry_count < 5:
                try:
                    __results = helpers.bulk(client=es_client_conn.es, actions=actions,)
                    break
                except DeadlineExceededError:
                    retry_count += 1
            if retry_count == 5:
                raise DeadlineExceededError
        else:
            __results = helpers.bulk(client=es_client_conn.es, actions=actions,)
        for result in __results[1]:
            if result['index']['status'] != 200:
                raise IndexException(result['index']['error'])
            results.append(result['index']['_id'])
        return results
    def query(self, query_object):
        config = query_object.getSearchObject()

        _source = config.get('_source', "")
        if len(_source) > 0 and not _source.endswith('_rank'):
            _source = "%s,_rank" % _source
        else:
            _source = "_rank"
        config['_source'] = _source

        config['index'] = self.__name
        match_only = False
        try:         
            del config['match_only']
            match_only = True
        except KeyError:
            if config['reverse']:
                config['sort']  = "_rank:asc"        
            else:
                config['sort']  = "_rank:desc"
        reverse = config['reverse']
        del config['reverse']
        try:
            if is_appengine_environment:
                retry_count = 0
                while retry_count < 2:
                    try:
                        response = es_client_conn.es.search(**config)
                        break
                    except DeadlineExceededError:
                        retry_count += 1
                if retry_count == 2:
                    raise DeadlineExceededError
            else:
                response = es_client_conn.es.search(**config)
        except TransportError:
            return EsResultObject()
        return EsResultObject(response, match_only=match_only, reverse=reverse)
    def query_filtered(self, query_object):
        server = es_client_conn.SERVERS[0]
        credentials = server['http_auth'].split(':')
        payload = {'q': query_object.getQueryString(), 'm': query_object.getOffset(), 's': query_object.getLimit()}
        r = requests.get("%s/search/" % server['url'], auth=HTTPBasicAuth(credentials[0], credentials[1]), params=payload, timeout=60)
        if r.status_code == 200:
            return r.content
        return

class EsActions:
    transaction_header_size = 100
    max_transaction_size = 10000000 - transaction_header_size

    def __init__(self):
        self.__actions = []
        self.__results = []
        self.__action_size = 0
    def __del__(self):
        del self.__actions
    def addAction(self, action):
        self.__actions.append(action)
        self.__action_size += sys.getsizeof(str(action))
        results = None
        if len(self.__actions) > 200 or self.__action_size > self.max_transaction_size:
            results = self.push()
            del self.__actions
            self.__actions = []
            self.__action_size = 0
        return results
    def push(self):
        if is_appengine_environment:
            retry_count = 0
            while retry_count < 5:
                try:
                    __results = helpers.bulk(client=es_client_conn.es, actions=self.__actions,)
                    break
                except DeadlineExceededError:
                    retry_count += 1
            if retry_count == 5:
                raise DeadlineExceededError
        else:
            __results = helpers.bulk(client=es_client_conn.es, actions=self.__actions,)
        for action in self.__actions:
            self.__results.append(action['_id'])
    def getResults(self):
        return self.__results

class EsQueryObject:
    def __init__(self, query_string, doc_type, returned_fields=[], limit=25, default_operator="AND", offset=0, reverse=False, match_only=False):
        self.__config = {}
        if returned_fields:
            self.__config['_source'] = ",".join(returned_fields)
        self.__config['q'] = query_string
        self.__config['size'] = limit
        self.__config['doc_type'] = doc_type
        self.__config['default_operator'] = default_operator
        self.__config['from_'] = offset
        self.__config['reverse'] = reverse
        if match_only:
            self.__config['match_only'] = True
    def __del__(self):
        del self.__config
    def getSearchObject(self):
        # temporary fix for index query, deleting parameters to make compatible to elasticsearch api
        return deepcopy(self.__config)
    def getQueryString(self):
        return self.__config['q']
    def getLimit(self):
        return self.__config['size']
    def getOffset(self):
        return self.__config['from_']
	
class EsResultObject:
    def __init__(self, response=None, match_only=False, reverse=False):
        self.documents = []
        self.total_count = 0

        if not response:
            return
        self.total_count = response['hits']['total']
        for result in response['hits']['hits']:
            doc = result['_source']
            fields = []
            for name, value in zip(doc.keys(), doc.values()):
                fields.append(EsFieldBase(name=name, value=value))
            document = EsSearchDocument(rank=doc['_rank'], doc_type=result['_type'], fields=fields, id=result['_id'])
            self.documents.append(document)
        if match_only:
            self.documents = sorted(self.documents, key=lambda document: document['_rank'], reverse=not reverse)
