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

import pysqlite3kvstore
import os

def test1(database):
    """Testing: keys, values, clear, __setitem__, __getitem__"""
    database.clear()
    assert len(database.keys()) == 0
    assert len(database.values()) == 0
    assert len(database.items()) == 0
    database['hello'] = 'world'
    assert database['hello'] == 'world'
    assert len(database.keys()) == 1
    assert len(database.values()) == 1
    assert len(database.items()) == 1
    assert database.keys() == ['hello']
    assert database.values() == ['world']
    assert database.items() == [('hello','world')]
    assert len(database) == 1
    database.clear()
    assert len(database) == 0

def test2(database):
    """Testing: copy, setdefault, iter, iterkeys, iteritems, itervalues, pop, popitem"""
    database.clear()
    database['hello'] = 'world'
    assert database.copy() == {'hello':'world'}
    assert database.setdefault('hello') == 'world'
    assert database.setdefault('goodbye') == None
    database['goodbye'] = 'cruel world'
    iterator = database.iter()
    assert iterator.next() == 'hello'
    assert iterator.next() == 'goodbye'
    iterator = database.iteritems()
    assert iterator.next() == ('hello','world')
    assert iterator.next() == ('goodbye','cruel world')
    iterator = database.itervalues()
    assert iterator.next() == 'world'
    assert iterator.next() == 'cruel world'
    assert database.pop('hello') == 'world'
    assert database.pop('non-existent key','default response') == 'default response'
    assert database.popitem() == ('goodbye','cruel world')

def test3(database):
    """Testing: __repr__, __cmp__"""
    database.clear()
    database['hello'] = 'world'
    database['goodbye'] = 'cruel world'
    assert str(database) == "{u'hello': u'world', u'goodbye': u'cruel world'}"
    assert database == {'hello':'world','goodbye':'cruel world'}
    database.clear()

def test4(database):
    """Testing get"""
    database.clear()
    database['hello'] = 'world'
    database['goodbye'] = 'cruel world'
    assert database.get('hello') == 'world'
    database.clear()

def test5(database):
    """Testing: update"""
    database.clear()
    database.update([('hello','world'),('goodbye','cruel world')])
    assert database == {'hello':'world','goodbye':'cruel world'}
    database.clear()



def main():
    file_name = 'testconfigdatabase.sqlite'

    kvdatabase = pysqlite3kvstore.PySQLite3KVStore(file_name, table_name='ApplicationConfiguration', serialisation_type='json')

    test1(kvdatabase)
    test2(kvdatabase)
    test3(kvdatabase)
    test4(kvdatabase)
    test5(kvdatabase)

    kvdatabase.close()

    os.remove(file_name)


if __name__ == '__main__':
    main()
