# About

The couch-sqlite library allows easy moving of data from CouchDB into SQLite. The library provides two modes of operation; one-off and continuous.

## Usage

Using it is quite simple. It provides one function, which accepts configuration options and returns a connection object. This connection has a `run` method which actually does the syncronization, and a `map` method which allow you to transform records after they've been read from CouchDB and before they're written to SQLite. Couch-SQLite automatically keeps track of the last record it's moved over, and only ports since that changes over when called.

The function takes the following parameters:

* sqlite: path to the SQLite database. Does not need to actually exist -- couch-sqlite autocreates it.
* table: SQLite table name.
* schema: table schema, used when autocreating the table.
* keys: when determining whether to insert an update a row, these keys are checked.
* couchHost: the host of the couch database
* couchPort: the port on which couch is running
* couchDb: the database name

```javascript
var couchSqlite = require('couch-sqlite');

couchSqlite({
    sqlite: options.config.files + '/data.sqlite',
    table: 'data',
    schema: 'NAME VARCHAR, ISO3 VARCHAR',
    keys: ['NAME'],
    couchHost: 'localhost',
    couchPort: 1234,
    couchDb: 'data_for_sqlite'
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
