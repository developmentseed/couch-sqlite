var fs = require('fs'),
    _ = require('underscore')._,
    couchSqlite = require('couch-sqlite'),
    sqlite3 = require('sqlite3'),
    daybed = require('./lib/daybed');

var prefix = function(len) {
    var str= '', chars = 'abcdefghijklmnopqrstuvwxyz';
    for (var i = 0; i < len; i++) {
        str += chars[parseInt(Math.random() * 25)];
    }
    return 'test' + str + '_';
}

// Generate a uniq prefix for the database.
var dbName = '/tmp/' + prefix(6) + 'data.sqlite',
    port = 5985;

// Closure variable used to capture state.
var results = {
    count: 0,
    name: 0,
    data: 0,
    lastSeqVal: null,
    lastSeqLen: 0,
    dataLen: 0,
    data_id: 0,
    dataName: 0,
    dataData: 0
};

daybed(port, function() {
    couchSqlite({
        sqlite: dbName,
        table: 'data',
        schema: 'name VARCHAR, data VARCHAR',
        couchUri: 'http://localhost:'+ port +'/database_name',
    }).map(function(doc) {

        results.count++;
        if (doc.name) results.name++;
        if (doc.data) results.data++;

        return values = {
          name: doc.name,
          data: doc.data
        };
    }).on('done', function(lastId) {
        var actions = [],
            db;

        actions.push(function(next) {
            db = new sqlite3.Database(dbName, next);
        });

        actions.push(function(next, err) {
            db.all('SELECT * FROM last_seq', next);
        });

        actions.push(function(next, err, data) {
            results.lastSeqLen = data.length;
            results.lastSeqVal = data.pop().id;
            db.all('SELECT * FROM data', next);
        });

        actions.push(function(next, err, data) {
            results.dataLen = data.length;
            _.each(data, function(i) {
                if (i._id) results.data_id++;
                if (i.name) results.dataName++;
                if (i.data) results.dataData++;
            });
            // Cleanup the temporary database.
            db.close();
            fs.unlink(dbName, next);
        });

        _(actions).reduceRight(_.wrap, function(err) {
            exports.testNormal = function(beforeExit, assert) {
                assert.equal(2, results.name, 'Missing document name.');
                assert.equal(2, results.data, 'Missing document data.'); 
                assert.equal(2, results.count, 'Missing expected record.');
                assert.equal(1, results.lastSeqLen, 'Extra last_seq records found.');
                assert.equal(2, results.lastSeqVal, 'last_seq not written correctly to SQLite.');
                assert.equal(2, results.dataLen, 'Missing SQLite record.');
                assert.equal(2, results.data_id, 'Missing _id in SQLIte.');
                assert.equal(2, results.dataName, 'Missing name in SQLIte.');
                assert.equal(2, results.dataData, 'Missing data in SQLIte.');
                assert.equal(2, lastId, 'last_seq not reported correctly.');
            };
        })();

    }).run();
});
