var Step = require('step'),
    _ = require('underscore')._,
    request = require('request'),
    sqlite3 = require('sqlite3');

module.exports = function(options) {
    var db, last, records;
    Step(function() {
        var self = this;
        // Install sqlite db.
        db = new sqlite3.Database(options.sqlite, function(err) {
            if (err) throw err;
            db.run('CREATE TABLE IF NOT EXISTS last_seq (id INTEGER)', function(err) {
                if (err) throw err;
                self();
            });
        });
    }, function() {
        var self = this;
        options.schema += ', _id INTEGER';
        db.run('CREATE TABLE IF NOT EXISTS ' + options.table + ' (' + options.schema + ')', function (err) {
            if (err) throw err;
            self();
        });
    }, function() {
        var self = this;
        db.all('SELECT * FROM last_seq', function(err, data) {
            if (err) throw err;
            var last_id = ((data && data[0] && data[0].id) || 0);

            request.get({
                uri: 'http://' +
                    options.couchHost + ':' +
                    options.couchPort + '/' +
                    options.couchDb + '/_changes?since=' + last_id + '&include_docs=true',
            }, function(err, response, body) {
                var body = JSON.parse(body);
                records = body.results;
                last = body.last_seq;
                self();
            });
        });
    }, function() {
        var group = this.group();
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
    }, function() {
        var self = this;
        db.run('DELETE FROM last_seq', function(err) {
            if (err) throw err;
            db.run('INSERT INTO last_seq VALUES (?)', [ last ], function(err) {
                if (err) throw err;
                self();
            });
        })
    });
};
