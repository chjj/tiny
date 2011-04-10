// a small http environment to examine the db
// hope to eventually turn this into a small blogging engine as an example
// also to see how it may perform in the real world, i.e. the data store of a blog

var tiny = require('../tiny');
var ins = require('util').inspect;
var json = JSON.stringify;
var http = require('http');
var url = require('url');
var qs = require('querystring');

var db = tiny('small.tiny');

var html = function(s) { return s.replace(/</g, '&lt;').replace(/>/g, '&gt;'); };

function onGet(req, res) {
  if (req.path[0] === 'del') {
    db.remove(req.path[1], function() {
      res.redirect('/');
    });
  } else {
    db.find().desc('timestamp')(function(err, results) {
      results.forEach(function(doc, i) {
        results[i] = '<p>\n\t<a href="/del/'+escape(doc._key)+'">x</a>\n\t'+html(json(doc))+'\n</p>';
      });
      res.write([
        '<style>',
        '  textarea, input { display: block; }',
        '  textarea { width: 500px; height: 300px; }',
        '</style>',
        (results || []).join('\n'),
        '<form method="POST" action="/">',
        '  ID: <input type="text" name="id">',
        '  Title: <input type="text" name="title">',
        '  Text: <textarea name="content"></textarea>',
        '  <input type="submit">',
        '</form>',
        '<pre><b>CACHE ('+db._cacheSize+'b):</b>\n'+html(ins(db._cache))+'</pre>',
        '<pre><b>INDEX:</b>\n'+html(ins(db._index))+'</pre>'
      ].join('\n'));
      db.fetch(['timestamp'], function(doc, total) {
        if (doc.timestamp > (Date.now() - 5*60*1000)) {
          return ['id', 'title'];
        }
      }, function(err, results) {
        res.end([
          'Results of query:<br>',
          html(ins(results))
        ].join('\n'));
      });
    });
  }
}

function onPost(req, res) {
  var data = req.body;
  data.timestamp = Date.now();
  db.set(req.body.id, req.body, function() {
    res.redirect('/');
  });
}

http.createServer(function(req, res) {
  res.statusCode = 200;
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  req.path = url.parse(req.url).pathname;
  req.path = req.path.replace(/^\/|\/$/g, '').split('/');
  req.path = req.path.map(function(v) { return unescape(v).replace(/\+/g, ' '); });
  if (!req.path[0]) req.path = [];
  res.redirect = function(loc) {
    res.statusCode = 303;
    res.removeHeader('Content-Type');
    res.setHeader('Location', 'http://'+req.headers.host+loc);
    res.end();
  };
  if (req.method === 'POST') {
    req.body = '';
    req.setEncoding('utf8');
    req.on('data', function(chunk) { 
      req.body += chunk;
    }).on('end', function() {
      req.body = qs.parse(req.body);
      onPost(req, res);
    });
  } else {
    onGet(req, res);
  }
}).listen(80, '127.0.0.2');