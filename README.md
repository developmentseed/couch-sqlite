# About

The couch-sqlite library allows easy moving of data from CouchDB into SQLite. The library provides two modes of operation; one-off and continuous.

## Usage

Using it is quite simple. It provides one function, which accepts configuration options and returns a connection object. This connection has a `run` method which actually does the syncronization, and a `map` method which allow you to transform records after they've been read from CouchDB and before they're written to SQLite. Couch-SQLite automatically keeps track of the last record it's moved over, and only ports since that changes over when called.

The function takes the following parameters:

* sqlite: path to the SQLite database. Does not need to actually exist -- couch-sqlite autocreates it.
* table: SQLite table name.
* schema: table schema, used when autocreating the table.
* couchUri: The URI of the couch database. If couch is running on localhost, using the default port (5884) and the database name is `sqlite_data` this would be `http://localhost:5984/sqlite_data`.

```javascript
var couchSqlite = require('couch-sqlite');

couchSqlite({
    sqlite: options.config.files + '/data.sqlite',
    table: 'data',
    schema: 'NAME VARCHAR, ISO3 VARCHAR',
    couchUri: 'http://localhost:5984/sqlite_data',
}).map(function(doc) {
    if (doc._id.indexOf('Data') !== 0) {
        return false;
    }

    return values = {
      NAME: doc.name,
      ISO3: doc.ISO3
    };
}).run();
```
