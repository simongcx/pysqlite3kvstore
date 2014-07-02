pysqlite3kvstore
================

Persistent dictionary-like key value store for Python based on SQLite


Aims
----

 - Dictionary-like interface (as close as possible)
 - Concurrency (provided by SQLite3)
 - Low memory footprint (values are only loaded into memory when required)
 - Data interchange (provided by storing values as JSON to preserve type)


Basic usage
-----------

    import pysqlite3kvstore
    file_name = 'testconfigdatabase.sqlite'
    kvdatabase = pysqlite3kvstore.PySQLite3KVStore(file_name, table_name='ApplicationConfiguration', serialisation_type='json')
    kvdatabase['timeout'] = 10
    print kvdatabase['timeout']
    >>> 10


Dictionary attributes that are not implemented
----------------------------------------------

- fromkeys(seq[, value])
- viewitems()
- viewkeys()
- viewvalues()


Notes
-----

Reviewed on StackExchange CodeReview here: http://codereview.stackexchange.com/questions/54960/persistent-key-value-store-for-python-using-sqlite3


