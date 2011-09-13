var events = require('events'),
    util = require('util'),
    _ = require('underscore')._,
    request = require('request'),
    sqlite3 = require('sqlite3');

// Installs the database and it's tables
var install = function(sqliteDb, schema, callback) {
    var db;
    db = new sqlite3.Database(sqliteDb, function(err) {
        if (err) callback(err);

        db.run('CREATE TABLE IF NOT EXISTS last_seq (id INTEGER)', function(err) {
            if (err) return callback(err);

            var schema = (schema + ', ' || '') + '_id VARCHAR';
            db.run('CREATE TABLE IF NOT EXISTS data (' + schema + ')', function (err) {
                return callback(err);
            });
        });
    });
};

// Accept updates from couch.
var update = function() {

        db.all('SELECT * FROM last_seq', function(err, data) {

        _(records).each(function(record) {
            var data = options.map(record.doc), next = group();
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
        })
};

// Just run this once
var oneOff = function(lastId, callback) {

    var uri = 'http://' + options.couchHost + ':' + options.couchPort + '/';
        uri += options.couchDb + '/_changes?since=' + lastId + '&include_docs=true',

    request.get({uri: uri }, function(err, response, body) {
        if (err) return callback(err);

        var body = JSON.parse(body);
        records = body.results;
        last = body.last_seq;
        callback();
    });


// For the long run...
var run = function() {

}

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
 */
var Connector = function(options) {

    var db, last, records;

    Step(function() {
        install(options.sqlite, options.schema, this);
    }, function() {
        var self = this;

}

util.inherits(Connector, events.EventEmitter);

Connector.prototype.run = function(persistent) {
    if (persistent) {
        run();
    } else {
        oneOff();
    }
    return this;
}

module.exports = function(options, callback) {
   new Connector(options, callback);
}
