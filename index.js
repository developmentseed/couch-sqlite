var events = require('events'),
    util = require('util'),
    _ = require('underscore')._,
    request = require('request'),
    sqlite3 = require('sqlite3');

// Installs the database and it's tables
// TODO the name here is a bit of a misnomer.
var install = function(sqliteDb, schema, callback) {
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
var lastId = function(db, callback) {
    db.all('SELECT * FROM last_seq', function(err, data) {
        if (err) return callback(err);

        var last = 0;
        if (data.length) {
            last = data.pop().id;
        }
        callback(err, last);
    });
}

// Accept updates from couch.
var update = function(records, last, callback) {
    console.log(records);
    return;
    _(records).each(function(record) {
        var data = options.map(record.doc),
            next = group();

        if (!record.deleted && !data) {
            next();
        }
        else {
            db.run('DELETE FROM ' + options.table + ' WHERE _id = ?', [ record.id ], function(err) {
                if (err) throw err;
                if (record.deleted) {
                    next();
                }
                else {
                    data._id = record.id;
                    var stmt = 'INSERT INTO ' + options.table + ' (' + _.map(data, function(v,k) { return "'" + k + "'" }).join(',') + ') VALUES (',
                        values = [],
                        args = [];
                    _(data).each(function(value) {
                        args.push(value);
                        values.push('?');
                    });
                    stmt += values.join(',') + ')';
                    db.run(stmt, args, function(err) {
                        if (err) throw err;
                        next();
                    });
                }
            });
        }
    });

    // can't by sync..
    db.run('DELETE FROM last_seq', function(err) {
        if (err) throw err;
        db.run('INSERT INTO last_seq VALUES (?)', [ last ], function(err) {
            if (err) throw err;
            self();
        });
    });
};

// Just run this once
var oneOff = function(uri, callback) {

    request.get({uri: uri }, function(err, response, body) {
        if (err) return callback(err);

        var body = JSON.parse(body);
        records = body.results;
        last = body.last_seq;
        update(records, last, callback);
    });
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
    this.options = options;

    // If a callback isn't specified, assume that we just want the deprecated
    // old style behavior.
    if (callback == undefined) {
        callback = function(err, conn) {
            if (err) return console.warn(err);
            conn.run();
        }
    }

    var that = this;
    install(options.sqlite, options.schema, function(err, db) {
        if (err) return callback(err);
        
        that.db = db;
        callback(null, that);
    });
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
        lastId(that.db, next);
    });

    actions.push(function(next, err, id) {
        if (err) return next(err);

        /* TESTING */ id = id - 1; /* END TESTING */
        uri += '&since=' + id;

        if (persistent) {
            run();
        } else {
            oneOff(uri, next);
        }
    });

    _(actions).reduceRight(_.wrap, function(err) {
        if (err) that.emit('error', err);
    })();
};

module.exports = function(options, callback) {
   new Connector(options, callback);
}
