#-------------------------------------------------------------------------------
# Name:        sqlite3keyvaluestore
# Purpose:     provides a dictionary-like wrapper for sqlite3
#
# Author:      Simon
#
# Created:     20/06/2014
# Copyright:   (c) Simon 2014
# Licence:     GNU General Public License v3
#-------------------------------------------------------------------------------

"""provides a dictionary-like wrapper for sqlite3 allowing it to be used as a key value store
For Python 2.7

Differences from a normal python dictionary:
- a file and path must be provided on creation
- it's not possible to create a kvstore literally as you would a dict

The following dictionary methods are not implemented:
- fromkeys(seq[, value])
- viewitems()
- viewkeys()
- viewvalues()"""


import sqlite3
import json
import UserDict

try:
    from cPickle import Pickler, Unpickler
except ImportError:
    from pickle import Pickler, Unpickler

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


class PySQLite3KVStore(UserDict.DictMixin):

    def __init__(self, filename_and_path, table_name='ApplicationConfiguration', serialisation_type='json'):
        """filenameandpath - the name and path to the file
        table_name - the name of the table to create (or use) in the database (this allows the file to be used for other purposes with control over potential name clashes
        serialisationtype - either 'json' or 'pickle', defaults to 'json' - the method used to serialise the value in the database"""
        self.con = sqlite3.connect(filename_and_path)
        self.cur = self.con.cursor()
        self.file_name_and_path = filename_and_path
        self.table_name = table_name
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name='%s';" % self.table_name
        self.cur.execute(query)
        if not self.cur.fetchall():
            query = """CREATE TABLE `%s` (
                                	`k`	TEXT,
                                	`v`	TEXT,
                                	PRIMARY KEY(k)
                                );""" % self.table_name
            self.cur.execute(query)
            self.con.commit()
        if serialisation_type == 'json':
            self._tostring = self._jsontostring
            self._fromstring = self._jsonfromstring
        elif serialisation_type == 'pickle':
            self._tostring = self._pickletostring
            self._fromstring = self._picklefromstring

    def _pickletostring(self,value):
        f = StringIO()
        p = Pickler(f, 0)
        p.dump(value)
        return f.getvalue()

    def _picklefromstring(self,value):
        f = StringIO(value)
        return Unpickler(f).load()

    def _jsontostring(self,value):
        return json.dumps(value)

    def _jsonfromstring(self,value):
        return json.loads(value)

    def keys(self):
        query = "SELECT k FROM %s" % self.table_name
        self.cur.execute(query)
        return [item[0] for item in self.cur.fetchall()]

    def values(self):
        query = "SELECT v FROM %s" % self.table_name
        self.cur.execute(query)
        return [self._fromstring(item[0]) for item in self.cur.fetchall()]

    def items(self):
        query = "SELECT k, v FROM %s" % self.table_name
        self.cur.execute(query)
        return [(item[0],self._fromstring(item[1])) for item in self.cur.fetchall()]

    def __len__(self):
        query = "SELECT Count(*) FROM %s" % self.table_name
        self.cur.execute(query)
        return self.cur.fetchone()[0]

    def __contains__(self, key):
        query = "SELECT k FROM %s WHERE k = ?" % self.table_name
        self.cur.execute(query, (key,))
        return bool(self.cur.fetchone())

    def __getitem__(self, key):
        query = "SELECT v FROM %s WHERE k = ?" % self.table_name
        self.cur.execute(query,(key,))
        result = self.cur.fetchone()
        if result:
            return self._fromstring(result[0])
        else:
            raise KeyError

    def __setitem__(self, key, value):
        query = "INSERT OR REPLACE INTO %s (k, v) VALUES (?, ?);" % self.table_name
        self.cur.execute(query,(key, self._tostring(value)))
        self.con.commit()

    def __delitem__(self, key):
        if not self.has_key(key):
            raise KeyError
        query = "DELETE FROM %s WHERE k = ?" % self.table_name
        self.cur.execute(query,(key,))
        self.con.commit()

    def close(self):
        self.con.close()

    def __del__(self):
        self.con.close()

    def clear(self):
        query = "DELETE FROM %s" % self.table_name
        self.cur.execute(query)

    def copy(self):
        return dict(self.items())

    def iter(self):
        return self.iterkeys()

    def __iter__(self):
        return self.iterkeys()

    def iteritems(self):
        def result_function(result):
            return (result[0], self._fromstring(result[1]))
        return DatabaseIterator(self.file_name_and_path, self.table_name, "SELECT k, v FROM %s LIMIT 1 OFFSET " % self.table_name, result_function)

    def iterkeys(self):
        def result_function(result):
            return result[0]
        return DatabaseIterator(self.file_name_and_path, self.table_name, "SELECT k FROM %s LIMIT 1 OFFSET " % self.table_name, result_function)

    def itervalues(self):
        def result_function(result):
            return self._fromstring(result[0])
        return DatabaseIterator(self.file_name_and_path, self.table_name, "SELECT v FROM %s LIMIT 1 OFFSET " % self.table_name, result_function)


class DatabaseIterator:
    def __init__(self, filenameandpath, table_name, query, resultfunction):
        """query should be in the form "SELECT <your code here> LIMIT 1 OFFSET "
        """
        con = sqlite3.connect(filenameandpath)
        self.cur = con.cursor()
        self.query = query
        self.offset = 0
        self.resultfunction = resultfunction

    def __iter__(self):
        return self

    def next(self):
        query = self.query + str(self.offset) + ';'
        self.cur.execute(query)
        result = self.cur.fetchone()
        if result is None:
            raise StopIteration
        else:
            self.offset += 1
            return self.resultfunction(result)

