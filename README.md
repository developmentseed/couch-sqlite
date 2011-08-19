The couch-sqlite library allows easy moving of data from CouchDB into SQLite.

Using it is quite simple. It provides one function, which moves data over from Couch to SQLite. Couch-SQLite automatically keeps track of the last record it's moved over, and only ports since that changes over when called.

The function takes the following parameters:

* sqlite: path to the SQLite database. Does not need to actually exist -- couch-sqlite autocreates it.
* table: SQLite table name.
* schema: table schema, used when autocreating the table.
* keys: when determining whether to insert an update a row, these keys are checked.
* map: a callback that maps data from couch to SQLite. It receives one parameter, which is the document straight out of couch. It must return an object whose keys correspond to the table columns defined in the schema. If you don't want a given row to be inserted into the SQLite database, simply return false.
* couchHost: the host of the couch database
* couchPort: the port on which couch is running
* couchDb: the database name

```javascript
var couch_sqlite = require('couch-sqlite');

couch_sqlite({
    sqlite: options.config.files + '/data.sqlite',
    table: 'data',
    schema: 'NAME VARCHAR, ISO3 VARCHAR',
    keys: ['NAME'],
    map: function(doc) {
        if (doc._id.indexOf('Data') !== 0) {
            return false;
        }

        return values = {
          NAME: doc.name,
          ISO3: doc.ISO3
        };

        return values;
    },
    couchHost: 'localhost',
    couchPort: 1234,
    couchDb: 'data_for_sqlite'
});
```