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
  // db.query is the non-chainable version of db.find
  db.query({two: 'hi'}, function(err, results) {
    console.log('RESULTS:', results);
    next();
  }, {select: ['four']});
}, 
/*function(next) {
  db.find({two: 'hi'})
  .select('four', 'three')
  (function(err, results) {
    console.log('RESULTS:', results);
    next();
  });
}, 
function(next) {
  db.find({two: { $eq: 'hi' }})
  .select('four', 'three')
  (function(err, results) {
    console.log('RESULTS:', results);
    next();
  });
}, */
function(next) {
  db.find({num: { $gte: 72 }, two: { $eq: 'hi' }})
  .desc('num').limit(2)(function(err, results) {
    console.log('RESULTS:', results);
    next();
  });
}, 
function(next) {
  db.find({$or: [ { num: { $gt: 82 } }, { num: { $eq: 82 } }  ] })
  .desc('num').limit(3)(function(err, results) {
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
  db.fetch(function(doc, total) { // ['num'], 
    if (total === 3) return 'break';
    if (doc.num > 82 || doc.num === 82) {
      console.log('found', doc._key); 
      return true;
    }
  }, function(err, results) {
    console.log(db._cache);
    console.log('RESULTS:', Tiny.sort.desc(results, 'num'));
    next();
  });
},
function(next) {
  var a = '', i = 5000;
  while (i--) a += 'a';
  db.set('other', {
    prop1: 'hello',
    prop2: 'world',
    big: a
  }, function() {
    //console.log(db._cache);
    next(a);
  });
},
function(a) {
  db.get('other', function(err, data) {
    assert.ok(data.big === a, 'selective caching failed');
    if (data.big === a) console.log('done');
  });
}
);