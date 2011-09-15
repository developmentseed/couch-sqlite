var events = require('events'),
    util = require('util'),
    _ = require('underscore')._,
    request = require('request'),
    sqlite3 = require('sqlite3');

/**
 * Opens a connection to sqlite, optionally installs the database and its
 * tables.
 * TODO make that "optionally" a reality.
 * TODO un-nestify
 */
var openSqlite = function(sqliteDb, schema, callback) {
    var db;
    db = new sqlite3.Database(sqliteDb, function(err) {
        if (err) callback(err);

        db.run('CREATE TABLE IF NOT EXISTS last_seq (id INTEGER)', function(err) {
            if (err) return callback(err);

            var schema = (schema + ', ' || '') + '_id VARCHAR';
            db.run('CREATE TABLE IF NOT EXISTS data (' + schema + ')', function (err) {
                return callback(err, db);
            });
        });
    });
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

var setLastId = function(db, id, callback) {
    // TODO run this as a transaction, or the like.
    db.run('DELETE FROM last_seq', function(err) {
        if (err) return callback(err);

        db.run('INSERT INTO last_seq VALUES (?)', [ id ], function(err) {
            callback(err);
        });
    });
};

// Accept updates from couch, write them to SQLite.
var update = function(db, table, record, callback) {
    //console.log(record);
    //return callback('die');

    var actions = [];

    actions.push(function(next) {
        db.exec('BEGIN TRANSACTION', next);
    });

    actions.push(function(next, err) {
        if (err) return next(err);
        db.run('DELETE FROM ' + table + ' WHERE _id = ?', [ record.id ], next);
    });

    // handle deleted records...
    //if (!record.deleted) {
    if (true) {
       actions.push(function(next, err) {
            if (err) return next(err);

            record.doc._id = record.id;
            var stmt = 'INSERT INTO ' + table + ' (' + _.map(record.doc, function(v,k) { return "'" + k + "'" }).join(',') + ') VALUES (',
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

    _(actions).reduceRight(_.wrap, callback)();
};



// For the long run...
var run = function() {

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
 * - `keys` ...
 * - `table` Name to use for data table in the sqlite database, defaults to `data`
 * - `map` (deprecated)
 *
 * The Connector object which is passed to the callback is an eventEmitter
 * which emits the following events:
 *
 * - map
 * - error
 * - done
 */
var Connector = function(options, callback) {
    // TODO enforce defaults, copy things over.
    this.options = options;

    return this;
};

util.inherits(Connector, events.EventEmitter);

Connector.prototype.run = function(persistent) {
    var that = this,
        actions = [];

    // Assumble the basic changes uri.
    var uri = 'http://' + this.options.couchHost;
        uri += ':' + this.options.couchPort + '/';
        uri += this.options.couchDb + '/_changes?include_docs=true';

    actions.push(function(next) {
        openSqlite(that.options.sqlite, that.options.schema, next);
    });

    // Fetch that last updated ID.
    actions.push(function(next, err, db) {
        getLastId(db, next);
    });

    // Fetch the CouchDB _changes URI.
    actions.push(function(next, err, id) {
        if (err) return next(err);

        uri += '&since=' + id;

        request.get({uri: uri }, next);
    });

    // Update SQLite.
    actions.push(function(next, err, response, body) {
        if (err) return callback(err);

        var resp = JSON.parse(body);
        // TODO resp.last_seq...

        // Only call next once we're through all the records.
        next = _.after(resp.results.length, next);

        // Capture all errors we generate in a closure.
        var errors = [];
        var done = function(err) {
            if (err) errors.push(err);
            next(errors.length || null);
        };

        _(resp.results).each(function(record) {

            // Allow data to be transformed.
            // TODO remove 'options.map' way...
            if (that.options.map) {
                record.doc = that.options.map(record.doc);
            }
            that.emit('map', record.doc);

            // If the result of the `map` is a falsy value we do nothing.
            if (!record.doc) return next();

            // Grab a fresh connection to SQLite so that we can execute
            // each update as a transaction.
            openSqlite(that.options.sqlite, null, function(err, db) {
                if (err) return next(err);

                update(db, that.options.table, record, done);
            });
        });
    });

    // Set the Last id in SQLite (TODO).
    actions.push(function(next, err) {
        if (err) return next(err);
        next();
    })

    _(actions).reduceRight(_.wrap, function(err) {
        if (err) that.emit('error', err);
    })();
};

module.exports = function(options) {
   return new Connector(options);
}
