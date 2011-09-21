var events = require('events'),
    util = require('util'),
    _ = require('underscore')._,
    request = require('request'),
    sqlite3 = require('sqlite3');
    pool = require('generic-pool').Pool;

/**
 * Opens a connection to sqlite, optionally installs the database and its
 * tables.
 */
var openSqlite = function(conn, table, schema, callback) {
    var actions = [],
        db;

    actions.push(function(next) {
        conn.pool.acquire(function(err, sqliteDb) {
            if (err) return next(err);
            db = sqliteDb; // closure.
            next();
        })
    });

    // Only attempt to create our tables if we've been handed a schema.
    if (schema) {
        actions.push(function(next, err, sqliteDb) {
            if (err) return next(err);
            db.exec('CREATE TABLE IF NOT EXISTS last_seq (id INTEGER)', next) ;
        });

        actions.push(function(next, err) {
            if (err) return next(err);

            // Add a `_id` column for internal tracking.
            schema += ', _id VARCHAR';
            db.exec('CREATE TABLE IF NOT EXISTS '+ table +'('+ schema +')', next);
        });
    }

    _(actions).reduceRight(_.wrap, function(err) {
        callback(err, db);
    })();
};

// Fetch that last sequence id from sqlite.
var getLastId = function(db, callback) {
    db.all('SELECT * FROM last_seq', function(err, data) {
        if (err) return callback(err);

        var last = 0;
        if (data.length) {
            last = data.pop().id;
        }
        callback(err, last);
    });
}

// Grab a fresh connection and set the Last id in SQLite.
var setLastId = function(conn, id, callback) {
    var actions = [],
        db;

    actions.push(function(next, err) {
        if (err) return callback(err);
        openSqlite(conn, null, null, next);
    });

    actions.push(function(next, err, sqlite) {
        if (err) return callback(err);
        db = sqlite;
        db.exec('BEGIN TRANSACTION', next);
    });

    actions.push(function(next, err) {
        if (err) return callback(err);
        db.run('DELETE FROM last_seq', next);
    });

    actions.push(function(next, err) {
        if (err) return callback(err);
        db.run('INSERT INTO last_seq VALUES (?)', [ id ], next);
    });

    actions.push(function(next, err) {
        if (err) return next(err);
        db.exec('COMMIT', next);
    });

    _(actions).reduceRight(_.wrap, function(err) {
        conn.pool.release(db);
        callback(err);
    })();
};


// Accept updates from couch, write them to SQLite.
var update = function(conn, record, callback) {

    // Allow data to be transformed.
    if (conn._map) {
        record.doc = conn._map(record.doc);
    }

    // If the result of the `map` is a falsy value we do nothing.
    if (!record.doc) return callback();

    var actions = [],
        db;

    // Grab a fresh connection to SQLite so that we can execute
    // each update as a transaction.
    actions.push(function(next) {
        openSqlite(conn, null, null, next);
    });

    actions.push(function(next, err, sqlite) {
        if (err) return next(err);
        db = sqlite;
        db.exec('BEGIN TRANSACTION', next);
    });

    actions.push(function(next, err) {
        if (err) return next(err);
        db.run('DELETE FROM ' + conn.options.table + ' WHERE _id = ?', [ record.id ], next);
    });

    // handle deleted records...
    //if (!record.deleted) {
    if (true) {
       actions.push(function(next, err) {
            if (err) return next(err);

            record.doc._id = record.id;
            var stmt = 'INSERT INTO ' + conn.options.table + ' (' + _.map(record.doc, function(v,k) { return "'" + k + "'" }).join(',') + ') VALUES (',
                values = [],
                args = [];

            _(record.doc).each(function(value) {
                args.push(value);
                values.push('?');
            });
            stmt += values.join(',') + ')';

            db.run(stmt, args, next);
       });
    }

    actions.push(function(next, err) {
        if (err) return next(err);
        db.exec('COMMIT', next);
    });

    _(actions).reduceRight(_.wrap, function(err) {
        conn.pool.release(db);
        callback(err);
    })();
};

/**
 * Object which managed the connection between Couch and Sqlite.
 * Takes a single `options` argument, an object which can have the
 * following keys;
 *
 * - `sqlite` (required) Path to a sqlite file, or a location to write one.
 * - `schema` (required) a statement which can be used to create the schema.
 * - `couchHost` (required) the host of the couch database
 * - `couchPort` (required) the port on which couch is running
 * - `couchDb` (required) the database name
 * - `table` Name to use for data table in the sqlite database, defaults to `data`
 * - `map` (deprecated)
 *
 * The Connector object which is passed to the callback is an eventEmitter
 * which emits the following events:
 *
 * - error
 * - done (not implemented)
 */
var Connector = function(options, callback) {
    // TODO enforce defaults, copy things over.
    this.options = options;

    if (typeof(options.map) == 'function') {
        this._map = options.map;
    }

    // Setup the (single) connection pool.
    this.pool = pool({
        create   : function(callback) {
            var db;
            db = new sqlite3.Database(options.sqlite, function(err) {
                callback(err, db);
            });
        },
        destroy  : function(client) { client.close(); },
        max      : 1,
        idleTimeoutMillis : 100,
        log : false
    });

    return this;
};

util.inherits(Connector, events.EventEmitter);

Connector.prototype.run = function(persistent) {
    var that = this,
        lastId = 0,
        actions = [];

    // Assumble the basic changes uri.
    var uri = 'http://' + this.options.couchHost;
        uri += ':' + this.options.couchPort + '/';
        uri += this.options.couchDb + '/_changes?include_docs=true';

    actions.push(function(next) {
        openSqlite(that, that.options.table, that.options.schema, next);
    });

    // Fetch that last updated ID.
    actions.push(function(next, err, db) {
        if (err) return next(err);

        getLastId(db, next);
        that.pool.release(db);
    });

    if (persistent) {
        // Handle continuous connections
        actions.push(function(next, err, id) {
            if (err) return next(err);

            uri += '&since=' + id;
            uri += '&feed=continuous';
            uri += '&heartbeat=30000'; // TODO make configurable.

            var handleData = function(chunk) {
                var body = chunk.toString('utf8');
                // "heartbeat" chunks will contain a single newline.
                if (body.length > 1) {
                    body = JSON.parse(body);
                    update(that, body, function() {
                        if (err) return that.emit('error',err);

                        if (body.seq > lastId) {
                          lastId = body.seq; // closure
                          setLastId(that, lastId, function(err){
                              if (err) return that.emit('error',err);
                          });
                        }
                    });
                }
            };

            // Fetch the CouchDB _changes URI.
            request.get({uri: uri, onResponse: true}, function(err, response) {
                response.on('data', handleData);
            });
        });
    } else {
        // Fetch the CouchDB _changes URI.
        actions.push(function(next, err, id) {
            if (err) return next(err);

            lastId = id; // closure.

            uri += '&since=' + id;
            request.get({uri: uri }, next);
        });

        // Update SQLite.
        actions.push(function(next, err, response, body) {
            if (err) return callback(err);

            var resp = JSON.parse(body);
            if (!resp.results || resp.results.length == 0) {
              return next(null, false);
            }

            // Update the lastId closure.
            lastId = resp.last_seq;

            // Only call next once we're through all the records.
            next = _.after(resp.results.length, next);

            // Capture all errors we generate in a closure.
            var errors = [];
            var done = function(err) {
                if (err) errors.push(err);
                next(errors.length || null, true);
            };

            _(resp.results).each(function(record) {
                update(that, record, done);
            });
        });

        // Set the Last id in SQLite.
        actions.push(function(next, err, updateSeq) {
            if (err) return next(err);
            if (updateSeq) {
                setLastId(that, lastId, next);
            } else {
                next();
            }
        });
    }

    _(actions).reduceRight(_.wrap, function(err) {
        if (err) that.emit('error', err);
    })();
};

// Interface for setting up a map function for processing records before they
// are written to SQLite
Connector.prototype.map = function(map) {
    this._map = map;
    return this;
}

module.exports = function(options) {
   return new Connector(options);
}
