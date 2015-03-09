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

import re
from uuid import uuid4
from datetime import datetime

from .exceptions import IncompleteParameters

stop_words = []
lists = ['en.list']
for list in lists:
    file = open("jarvis_search/stop_word_list/%s" % list, "r")
    words = file.read().split('\n')
    words = [re.sub('[^a-zA-Z0-9\']', '', word.lower()) for word in words]
    stop_words.extend(words)

class EsSearchDocument:
    def __init__(self, **kwargs):
        self.__config = {}
        self.__config['body'] = kwargs.get('fields')
        self.__config['id'] = kwargs.get('id', str(uuid4()))
        self.__config['doc_type'] = kwargs.get('doc_type')
        self.rank = kwargs.get('rank', datetime.now())
        if not all(self.__config.values()):
            raise IncompleteParameters(self.__config)
        body = {}
        for field in kwargs.get('fields'):
            body[field.name] = field.value
        body['_rank'] = self.rank
        self.__config['body'] = body
        self.doc_id = self.__config['id']
    def getDoc(self, index_name):
        self.__config['index'] = index_name
        return self.__config
    def __getitem__(self, name):
        return self.__config['body'].get(name)
    def __repr__(self):
        return str(self.__config)

class EsFieldBase:
    def __init__(self, name, value, language=None):
        self.name = name
        self.value = value
    def __repr__(self):
        return "%s:%s" % (self.name, self.value)

def tokenize_string(phrase):
    a = []
    for word in phrase.split():
        if len(word) > 12 or word in stop_words:
            continue
        for j in xrange(3, len(word)):
            for i in xrange(0, len(word)-j+1):
                a.append(word[i:i+j])
    a.extend(phrase.split())
    return a

class EsStringField(EsFieldBase):
    def __init__(self, **kwargs):
        value = kwargs.get('value', "")
        value = tokenize_string(value)
        kwargs['value'] = " ".join(value)
        EsFieldBase.__init__(self, **kwargs)

class EsTextField(EsFieldBase): pass

class EsArrayField(EsFieldBase):
    def __init__(self, **kwargs):
        assert(type(kwargs.get('value', [])), list)
