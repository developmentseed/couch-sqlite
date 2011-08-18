var Step = require('step'),
    _ = require('underscore')._,
    request = require('request'),
    sqlite3 = require('sqlite3');

module.exports = function(options) {
    var db, last, records;
    Step(function() {
        var self = this;
        // Install sqlite db.
        db = new sqlite3.Database(options.sqlite, function() {
            db.run('CREATE TABLE IF NOT EXISTS last_seq (id INTEGER)', function() {
                self();
            });
        });
    }, function() {
        var self = this;
        db.run('CREATE TABLE IF NOT EXISTS ' + options.table + ' (' + options.schema + ')', function (err) {
            if (err) throw err;
            self();
        });
    }, function() {
        var self = this;
        db.all('SELECT * FROM last_seq', function(err, data) {
            if (err) throw err;
            var last_id = ((data && data[0] && data.id) || 0);
    
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
            if (!data) {
                next();
            }
            else {
                var stmt = 'INSERT INTO ' + options.table + ' (' + _.map(data, function(v,k) { return "'" + k + "'" }).join(',') + ') VALUES (',
                    values = [],
                    args = [];
                _(data).each(function(value) {
                    args.push(value);
                    values.push('?');
                });
                stmt += values.join(',') + ')';
                var del = 'DELETE FROM ' + options.table + ' WHERE ', deleteArgs = [], deleteValues = [];
                _(options.keys).each(function(key) {
                   deleteArgs.push(data[key]);
                   deleteValues.push(key + ' = ?');
                });
                del += deleteValues.join(' AND ');

                db.run(del, deleteArgs, function(err) {
                    if (err) throw err;
                    db.run(stmt, args, function(err) {
                        if (err) throw err;
                        next();
                    });
                });
            }
        });
    }, function() {
        var self = this;
        console.log('here', last);
        db.run('DELETE FROM last_seq', function(err) {
            if (err) throw err;
            db.run('INSERT INTO last_seq VALUES (?)', [ last ], function(err) {
                if (err) throw err;
                self();
            });
        })
    });
};
