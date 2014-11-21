#
#       (C) 2014 Varun Mittal <varunmittal@nlpcore.com>
#       NLPCORE-ENGINE program is distributed under the terms of the GNU General Public License v3
#
#       This file is part of NLPCORE-ENGINE.
#
#       NLPCORE-ENGINE is free software: you can redistribute it and/or modify
#       it under the terms of the GNU General Public License as published by
#       the Free Software Foundation version 3 of the License.
#
#       NLPCORE-ENGINE is distributed in the hope that it will be useful,
#       but WITHOUT ANY WARRANTY; without even the implied warranty of
#       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#       GNU General Public License for more details.
#
#       You should have received a copy of the GNU General Public License
#       along with NLPCORE-ENGINE.  If not, see <http://www.gnu.org/licenses/>.
#

class IncompleteParameters(Exception):
    def __init__(self, kwargs):
        self.__string = "following parameter(s) required:" + ", ".join([key for key in kwargs if not kwargs[key]])
    def __str__(self):
        return self.__string

class IndexException(Exception):
    def __init__(self, error_mssg):
        self.__string = error_mssg
    def __str__(self):
        return self.__string
