var assert = require('assert');
var fs = require('fs');

var Tiny = require('../tiny');

// mock data
var data = require('./data');

// a step-like function
var next = function() {
  var cur = 0, func = arguments;
  (function next() { 
    if (!func[cur]) return;
    var args = Array.prototype.slice.call(arguments);
    if (func[cur+1]) { 
      args.unshift(next);
    }
    func[cur++].apply(null, args);
  })();
};

try { // delete the old test db
  fs.unlinkSync(__dirname + '/test.tiny');
} catch(e) {}

var db;
next(
function(next) {
  db = Tiny(__dirname + '/test.tiny', next);
}, 
function(next) {
  db.set('one', data[0], function() {
    db.set('two', data[1], function() {
      db.set('three', data[2], function() {
        db.set('four', data[3], next);
      });
    });
  });
}, 
function(next) {
  db.get('one', function(err, data) {
    console.log(data);
    next();
  });
}, 
function(next) {
  db.query({two: 'hi'}, function(err, results) {
    console.log('RESULTS:', results);
    next();
  });
},
function(next) {
  db.find({num: { $gte: 72 }, two: { $eq: 'hi' }})
  .desc('num').limit(2)(function(err, results) {
    console.log('RESULTS:', results);
    next();
  });
}, 
function(next) {
  db.find({$or: [ 
    { num: { $gt: 82 } }, 
    { num: { $eq: 82 } } 
  ]}).desc('num').limit(3)(function(err, results) {
    console.log('RESULTS:', results);
    next();
  });
}, 
function(next) {
  db.close(function() {
    db = null;
    next();
  });
},
function(next) {
  db = Tiny(__dirname+'/test.tiny', next);
},
function(next) {
  db.fetch({desc: 'num', limit: 3}, function(doc, total) { 
    if (doc.num > 82 || doc.num === 82) {
      console.log('found', doc._key); 
      return true;
    }
  }, function(err, results) {
    //console.log(db._cache);
    console.log('RESULTS:', results);
    next();
  });
},
function(next) {
  var str = '', i = 10000;
  while (i--) str += 'a';
  db.set('other', {
    prop1: 'hello',
    prop2: 'world',
    big: str
  }, function() {
    //console.log(db._cache);
    next(str);
  });
},
function(str) {
  db.get('other', function(err, data) {
    assert.ok(data.big === str, 'selective caching failed');
    if (data.big === str) console.log('done');
  });
}
);