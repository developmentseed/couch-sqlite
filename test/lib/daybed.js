var http = require('http'),
    parseUrl= require('url').parse;

/**
 * Prop up a fake CouchDB server which can be used to (psycho)analyize what our
 * library tells it.
 */
module.exports = function(port, callback) {
    var server = new http.Server();

    server.on('request', function(req, res) {
        req.on('end', function() {
            var url = parseUrl(req.url);

            //assert.equal('GET', req.method, 'Wrong HTTP verb used to query daybed');
            //assert.equal('/database_name/_changes', url.pathname, 'Bad changes API path.');
            //assert.match(url.query, /.*include_docs=true.*/, 'Bad querystring.');
            //assert.match(url.query, /.*since=0.*/, 'Bad querystring.');

            if (url.query.match(/.*feed=continuous.*/)) {
                // todo
            } else {
                var resBody = JSON.stringify({
                    results: [
                        {id: 'foo', doc: {name: 'Foo', data: 42}},
                        {id: 'bar', doc: {name: 'Bar', data: 7}}
                    ],
                    last_seq: 2
                });
                res.end(resBody);
            }
            server.close();
        });
    });
    server.listen(port, callback);
};
