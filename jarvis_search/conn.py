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

from django.conf import settings

import elasticsearch
from elasticsearch import Elasticsearch, RequestsHttpConnection

from jarvis_frontend.utilities import isDevelopmentServer

class ElasticSearchClient:
    def __init__(self):
        SERVERS = getattr(settings, 'ES_HOSTS', [])
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
        self.es = Elasticsearch(SERVERS)
