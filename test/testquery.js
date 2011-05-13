var data = {
  one: {
    num: 100,
    str: 'hello',
  },
  two: {
    num: 50,
    str: 'hello',
  },
  three: {
    num: 150,
    str: 'world',
  },
  four: {
    num: 150,
    str: 'world',
  }
};

var assert = require('assert');
var fs = require('fs');

var Tiny = require('../');

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
  }, function(next) {
    db.set('one', data.one, function() {
      db.set('two', data.two, function() {
        db.set('three', data.three, function() {
          db.set('four', data.four, next);
        });
      });
    });
  }, 
  function() {
    db.find({$or: [ 
      { num: { $eq: 150 } }, 
      { num: { $eq: 100 } } 
    ]})(function(err, results) {
      console.log('RESULTS:', results);
    });
  }
);