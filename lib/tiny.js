/**
 * node-tiny 
 *  (c) Copyright 2011, Christopher Jeffrey (MIT Licensed) (//github.com/chjj)
 *  An embedded/in-process document/object store for node.
 *  Largely inspired by nStore.
 *  No schemas, just store your objects.
 *  - 
 *  node-tiny uses indexes for stored and flatenned JSON objects.
 *  This allows for querying which should go easy on the memory.
 *  There is Mongo-style querying included, as well as a 
 *  sort of map-reduce-like interface, similar to Couch's "views".
 *  This is a work in progess, not recommended for any 
 *  heavy-duty production use.
 */

var fs = require('fs'),
    path = require('path');

// debug messages?
var _DEBUG = false; 

// the amount of bytes at which properties 
// are no longer cached, 128b by default
var CACHE_LIMIT = 128; 

// a token to recognize deleted properties
// maybe use null or vertical tab? 
// JSON might have trouble with other things
var DELETED = '\r'; 

var debug = _DEBUG
  ? function() {
    var args = Array.prototype.slice.call(arguments);
    args.unshift('Tiny:');
    return console.log.apply(console, args);
  } 
  : function() {};

// store all anonymous databases in `./.data`
var DB_DIR = path.normalize(__dirname + '/../.data');
if (!path.existsSync(DB_DIR)) {
  fs.mkdirSync(DB_DIR, 0666);
} 

// the tiny constructor, takes a `name`
// which is really a filename path
var Tiny = module.exports = function(name, func) { 
  if (!(this instanceof Tiny)) {
    return new Tiny(name, func);
  }
  this.name = name.charAt(0) !== '/' 
    ? DB_DIR + '/' + name
    : name;
  this._load(func);
};

// this will cache database objects the same 
// object will be returned every time
Tiny.open = (function() { 
  var cache = {};
  return function(name, func) {
    name = name.charAt(0) !== '/' 
      ? DB_DIR + '/' + name
      : name;
    if (cache[name]) {
      func.call(cache[name], null, cache[name]);
    } else {
      cache[name] = new Tiny(name, func);
    }
    return cache[name];
  };
})();

// change the property cache limit
Tiny.limit = function(n) {
  CACHE_LIMIT = n;
  return this;
};

// constructor for lookups
// holds the byte offsets of 
// large properties
var Lookup = function(index) {
  this[0] = index[0];
  this[1] = index[1];
};

// parse and load small properties into memory
Tiny.prototype._load = function(func) {
  var self = this;
  
  self._cache = {};
  self._queue = [];
  self._busy = false;
  
  // size of the chunks
  var data = new Buffer(4 * 1024); 
  
  var key = '',
      state = 'KEY',
      lines = {}, 
      start = 0, 
      pos = 0;
  
  // each "line" array will contain 2 indexes: 
  // the pointer position, and the length
  
  fs.open(self.name, 'a+', 0666, function(err, fd) {
    if (err) return func && func.call(self, err);
    (function read(done) {
      fs.read(fd, data, 0, data.length, pos, function(err, bytes) {
        if (err || !bytes) return done(err);
        
        for (var i = 0, k = 0; i < bytes; i++) {
          switch (state) {
            case 'KEY':
              if (data[i] === 9) { // tab
                state = 'DATA';
                start = pos + 1;
                key += data.slice(k, i).toString('ascii');
              }
              break;
            case 'DATA':
              if (data[i] === 10) { // line feed
                state = 'KEY';
                lines[key] = [ start, pos - start ];
                key = '';
                k = i + 1;
              }
              break;
          }
          pos++;
        }
        
        if (state === 'KEY') {
          key += data.slice(k).toString('ascii');
        }
        
        read(done);
      });
    })(function(err) {
      if (err) return self._error(func, err);
      debug('Done parsing.');
      self._fd = fd;
      self._total = pos;
      debug('Loading small properties into memory.');
      para(lines, function(loop, index, realKey) {
        var s = realKey.split('.'), docKey = s[0], propKey = s[1];
        if (!self._cache[docKey]) self._cache[docKey] = {};
        if (index[1] < CACHE_LIMIT) {
          self._lookup(index, function(err, data) {
            self._cache[docKey][propKey] = data;
            loop();
          });
        } else {
          // create a lookup for large properties
          self._cache[docKey][propKey] = new Lookup(index);
          loop();
        }
      }, function() {
        debug('DB open.');
        if (func) func.call(self, err, self);
      });
    });
  });
};

// curry on an error
Tiny.prototype._error = function(func, err, ret) {
  err = err || 'Not found.';
  debug('Error:', err + '');
  if (func) func.call(this, new Error(err), ret);
};

// commit the changes to the fd
Tiny.prototype.commit = function(func) {
  var self = this, data, queue;
  
  if (self._busy || !self._queue.length || !self._fd) return;
  
  debug('committing - ' + self._queue.length + ' items in queue.');
  
  self._busy = true;
  queue = self._queue;
  self._queue = [];
  data = [];
  
  queue.forEach(function(item) {
    var total = 0, propKey, prop, 
        realKey, propSize, realKeySize;
    for (propKey in item.doc) {
      realKey = item.docKey + '.' + propKey;
      prop = realKey + '\t' + item.doc[propKey] + '\n';
      propSize = Buffer.byteLength(prop);
      realKeySize = Buffer.byteLength(realKey + '\t');
      
      if (propSize > CACHE_LIMIT) {
        self._cache[item.docKey][propKey] = new Lookup([ 
          self._total + total + (realKeySize - 1), 
          propSize - realKeySize 
        ]);
      }
      
      total += propSize;
      data.push(prop);
    }
  });
  
  data = new Buffer(data.join(''));
  
  fs.write(self._fd, data, 0, data.length, self._total, function(err, bytes) {
    self._total += bytes;
    queue.forEach(function(item) {
      item.func.call(self, err);
    });
    queue = null;
    data = null;
    self._busy = false;
    // check the queue again
    self.commit();
    if (func) func.call(self, err);
  });
};

// grab an text excerpt from the FD 
Tiny.prototype._read = function(pos, length, func) {
  var data = new Buffer(length);
  fs.read(this._fd, data, 0, length, pos, function(err, bytes) {
    func(err, data.toString('utf-8'));
  });
};

// lookup a property, ignore deleted properties, deep by default
Tiny.prototype._lookup = function(lookup, func) {
  var self = this;
  self._read(lookup[0], lookup[1], function(err, data) {
    if (err) { 
      return self._error(func, err);
    }
    try {
      data = JSON.parse(data);
    } catch(err) {
      return self._error(func, err); 
    }
    // make sure to ignore if its deleted
    if (data === DELETED) {
      return self._error(func); 
    }
    if (func) func.call(self, null, data);
  });
};

// set/insert/save/update a document/object
Tiny.prototype._set = function(docKey, _doc, func, flag) {
  var self = this, doc = {}; // the new stringified doc
  var cache = self._cache;
  
  if (!_doc || typeof _doc !== 'object') {
    return this._error(func, 'Bad object.'); 
  }
  
  debug('setting doc:', docKey);
  
  // if there are any properties in the cache
  // that were excluded on the input object
  // need to explicitly mark them as deleted
  // on the stringified object
  if (flag !== 'update' && cache[docKey]) {
    for (var k in cache[docKey]) { 
      // implictly set _key to DELETED for 'delete'
      // this assumes ._key is in the cached object
      if (!Object.prototype.hasOwnProperty.call(_doc, k)) {
        doc[k] = stringify(DELETED);
      }
    }
  }
  
  // this works in such a way that it will not
  // alter the original object at all.
  // we *could* just have the cache be a reference
  // to the original object, but if the 
  // user is unaware of this, it might produce
  // unexpected results.
  switch (flag) {
    case 'set':
    case 'update':
      if (flag === 'set' || !cache[docKey]) {
        cache[docKey] = {_key:docKey};
      }
      
      // shallow copy
      var k = Object.keys(_doc);
      for (var i = 0, l = k.length; i < l; i++) {
        cache[docKey][k[i]] = _doc[k[i]];
        doc[k[i]] = stringify(_doc[k[i]]);
      }
      
      // make sure it has a key
      doc._key = stringify(docKey);
      break;
    case 'delete':
      delete cache[docKey];
      break;
  }
  
  self._queue.push({
    docKey: docKey, 
    doc: doc, 
    func: func
  });
  
  if (func) self.commit();
};

// ========== PUBLIC API ========== //

// set a document
Tiny.prototype.set = function(docKey, doc, func) {
  return this._set(docKey, doc, func, 'set');
};

// updates the document, do not overwrite missing properties.
Tiny.prototype.update = function(docKey, doc, func) {
  if (!this._cache[docKey]) {
    return this._error(func, 'No such key.'); 
  }
  return this._set(docKey, doc, func, 'update');
};

// remove a document
Tiny.prototype.remove = function(docKey, func) {
  if (!this._cache[docKey]) {
    return this._error(func, 'No such key.'); 
  }
  return this._set(docKey, {}, func, 'delete'); 
};

// simple function to get a stored object
// .get() can either be deep or shallow
// if it is shallow, only the properties
// under the CACHE_LIMIT will be retrieved
Tiny.prototype.get = function(docKey, func, shallow) {
  var self = this, doc = {}, 
      cache = self._cache[docKey];
  if (!cache) {
    return self._error(func);
  }
  // iterate over the properties and build the object
  // if there is a Lookup object, perform the lookup
  para(cache, function(loop, prop, propKey) {
    if (!shallow && cache[propKey] instanceof Lookup) {
      self._lookup(cache[propKey], function(err, data) {
        doc[propKey] = data;
        loop();
      });
    } else {
      doc[propKey] = cache[propKey];
      loop();
    }
  }, function() {
    if (func) func.call(self, null, doc);
  });
};

// return an array containing *every* document
// it is shallow by default, meaning it will
// only lookup and include properties smaller
// than the cache limit (<1kb)
Tiny.prototype.all = function(func, deep) { 
  this.fetch({shallow: !deep}, function() {
    return true;
  }, func);
};

// iterate through *every* document
// this is basically a forEach on the
// results from .all(), shallow by default
Tiny.prototype.each = function(func, deep) { 
  var self = this;
  self.all(function(err, docs) {
    docs.forEach(function(doc) {
      func.call(self, doc, doc._key);
    });
  }, deep);
};

// close the FD
Tiny.prototype.close = function(func) {
  var self = this;
  self._busy = true;
  fs.close(self._fd, function() {
    delete self._fd;
    self._busy = false;
    if (func) func.call(self);
  });
};

// simple getters for db size/length for ease of use
Tiny.prototype.__defineGetter__('length', function() {
  return Object.keys(this._cache).length;
});

Tiny.prototype.__defineGetter__('size', function() {
  return this._total || 0;
});

// kill the db and start anew
Tiny.prototype.kill = function(func) {
  var self = this;
  self.close(function() {
    fs.unlink(self.name, function() {
      self._load();
      if (func) func.call(self);
    });
  });
};

// clean up the append-only mess
// this may be very memory intensive 
Tiny.prototype.compact = function(func) {
  var self = this;
  self.all(function(err, docs) { // make sure everything is loaded
    self.close(function() {
      fs.open(self.name, 'w', function(err, fd) {
        self._fd = fd;
        self._total = 0;
        (function set(i, done) {
          if (!docs[i]) return done();
          self.set(docs[i]._key, docs[i], function() {
            set(++i, done);
          });
        })(0, function() {
          fs.close(fd, function(err) {
            process.nextTick(function() {
              self._load(func);
            });
          });
        });
      });
    });
  }, true); 
};

// dump the entire database to a single JSON file
// the "pretty" parameter will pretty print the JSON
// $ node
// > require('node-tiny')('db.tiny').dump(true);
Tiny.prototype.dump = function(pretty, func) {
  var self = this, data = {};
  if (typeof pretty === 'function') { 
    func = pretty; 
    pretty = undefined; 
  }
  if (!self._total) { // a hack in case the db hasnt loaded
    return setTimeout(function() { self.dump(pretty, func); }, 500);
  }
  self.all(function(err, docs) {
    if (err) { 
      console.log('Tiny: Dump failed.');
      return self._error(func, 'Dump failed.');
    }
    docs.forEach(function(doc) {
      data[doc._key] = doc;
      delete doc._key;
    });
    data = JSON.stringify(data, null, pretty ? 2 : 0);
    fs.writeFile(self.name + '.json', data, function(err) {
      if (func) func.call(self, err);
      if (!err) console.log('Tiny: Dump was successful.');
    });
    data = null; // kill references to make
    docs = null; // sure the GC cleans up
  }, true);
};

// --- hypothetical features --- //
// note that these functions are 
// completely inert at the moment

// "dont cache these properties"
// might not be especially helpful as a lookup
// object might use more memory than the data itself
Tiny.prototype.ignore = function() {
  if (!this._ignore) this._ignore = [];
  this._ignore = this._ignore.concat(_slice.call(arguments));
};

// "only cache these properties"
Tiny.prototype.index = function() {
  if (!this._index) this._index = [];
  this._index = this._index.concat(_slice.call(arguments));
};

// ========== QUERYING ========== //
Tiny.prototype.fetch = function(opt, filter, done) {
  var self = this, results = [], keys;
  if (!done) {
    done = filter;
    filter = opt;
    opt = {};
  }
  if (opt.count) results = 0;
  if (opt.desc || opt.asc) {
    keys = (function() {
      var order = opt.asc ? 'asc' : 'desc',
          prop = opt.asc || opt.desc,
          keys = sort[order](self._cache, prop);
      return keys.map(function(v) {
        return v._key;
      });
    })();
  } else {
    keys = Object.keys(self._cache);
  }
  if (opt.skip) {
    keys = keys.slice(opt.skip);
  }
  if (opt.limit) {
    keys = keys.slice(0, opt.limit);
  }
  para(keys, function(loop, docKey) {
    if (filter.call(self, self._cache[docKey], docKey) === true) {
      if (opt.count) {
        results++;
        return loop();
      }
      self.get(docKey, function(err, doc) {
        if (!err) results.push(doc);
        loop();
      }, opt.shallow);
    } else { 
      loop();
    }
  }, function() {
    if (!results || !results.length) {
      return done.call(self, new Error('No Records'), results);
    }
    if (opt.one || opt.single) results = results[0];
    if (done) done.call(self, null, results);
  });
};

// mongo-style querying
Tiny.prototype.query = (function() {
  // test a statement against a document
  var testStatement = (function() {
    // operator logic
    var ops = {
      $lt: function(a, b) {
        return (a < b);
      },
      $lte: function(a, b) {
        return (a <= b);
      },
      $gt: function(a, b) {
        return (a > b);
      },
      $gte: function(a, b) {
        return (a >= b);
      },
      $eq: function(a, b) {
        return (a == b);
      },
      $ne: function(a, b) {
        return (a != b);
      },
      $regex: function(a, b) {
        return b.test(a);
      },
      $in: function(a, b) { // contains any of..
        for (var k in a) {
          var va = a[k];
          for (var k in b) {
            var vb = b[k];
            if (va == vb) {
              return true;
            }
          }
        }
      },
      $nin: function(a, b) { // does not contain any of
        return !ops['$in'](a, b);
      },
      $all: function(a, b) { // contains all...
        var found = 0, need = Object.keys(b).length;
        for (var k in a) {
          var va = a[k];
          for (var k in b) {
            var vb = b[k];
            if (va == vb) {
              found++;
              break;
            }
          }
        }
        return !!(found === need);
      },
      $exists: function(a, b) {
        return b ? (a !== undefined) : (a === undefined);
      },
      $size: function(a, b) { // why? because i can
        return (Buffer.byteLength(a) === b);
      }
    };
    
    // test an object/statement to see
    // if it matches a document's properties.
    // this is the back bone of the query really.
    // some crazy recursion needs to be going
    // on because of the $or operator.
    return function testStatement(state, doc) {
      var fail = false;
      if (Array.isArray(state)) {
        var fails = 0, i = state.length;
        while (i--) {
          if (!testStatement(state[i], doc)) fails++;
        }
        return !!(fails < state.length);
      }
      for (var propKey in state) {
        var targetProp = state[propKey];
        var prop = doc[propKey];
        if (propKey === '$or') {
          if (!testStatement(targetProp, doc)) {
            fail = true; 
          }
        } else if (targetProp && typeof targetProp === 'object') {
          var propOperations = targetProp;
          for (var operator in propOperations) {
            targetProp = propOperations[operator];
            if (operator === '$or') {
              if (!testStatement(targetProp, doc)) fail = true; 
            } else if (ops[operator] && !ops[operator](prop, targetProp)) {
              fail = true;
            }
          }
        } else {
          if (prop != targetProp) fail = true; 
        }
      }
      return !fail; 
    };
  })();
  
  // the actual .query() function
  return function query(where, func, opt) {
    where = where || {};
    opt = opt || {};
    this.fetch(opt, function(doc) { 
      if (testStatement(where, doc)) {
        return true;
      }
    }, func);
  };
})();

// the same thing as query except with a chainable interface
Tiny.prototype.find = function() {
  var self = this, args = _slice.call(arguments);
  if (!args[0]) args[0] = null;
  if (typeof args[1] === 'function') {
    return self.query.apply(self, args);
  }
  var opt = {};
  var chain = function(func) {
    return self.query.apply(self, args.concat(func, opt));
  };
  // doesnt work at the moment
  // may be removed entirely
  chain.select = function() {
    console.error('Tiny:', 
      'Warning: `.select()` does not work currently.',
      'It may be removed in future versions.'
    );
    opt.select = Array.isArray(arguments[0]) 
      ? arguments[0] 
      : _slice.call(arguments)
    ;
    return chain;
  };
  chain.count = function() {
    opt.count = true;
    return chain;
  };
  chain.desc = function(prop) {
    opt.desc = prop;
    return chain;
  };
  chain.asc = function(prop) {
    opt.asc = prop;
    return chain;
  };
  chain.limit = function(limit) {
    opt.limit = limit;
    return chain;
  };
  chain.skip = function(skip) {
    opt.skip = skip;
    return chain;
  };
  // if shallow is true, only properties less than 1kb are available to the doc object
  // when no properties are explicitly selected
  chain.shallow = function() {
    opt.shallow = true;
    return chain;
  };
  chain.one = function() {
    opt.one = true;
    opt.limit = 1;
    return chain;
  };
  return chain;
};

// ========== HELPERS ========== //
var _slice = [].slice;

// sorting functions for queries
var sort = function(obj, key) {
  return toArray(obj).filter(function(v) { 
    if (v) v = v[key];
    return !!(v !== undefined);
  }).sort(function(a, b) {
    a = a[key]; b = b[key];
    if (!/^[\d.]+$/.test(a)) {
      a = (a + '').toLowerCase().charCodeAt(0);
      b = (b + '').toLowerCase().charCodeAt(0);
    }
    return a > b ? 1 : (a < b ? -1 : 0);
  });
};
sort.asc = function() {
  return sort.apply(this, arguments);
};
sort.desc = function() {
  return sort.apply(this, arguments).reverse();
};

// a parallel foreach
var para = function(obj, looper, func) {
  var c = 0, i = 0, l, k, key;
  if (typeof obj.length !== 'number') {
    k = Object.keys(obj);
  }
  l = (k || obj).length;
  if (!l) return func && func();
  var next = function() {
    if (++c === l) func && func();
  };
  for (; i < l; i++) {
    key = k ? k[i] : i;
    looper(next, obj[key], key, c);
  }
};

// safe stringify, JSON throws 
// if you pass in non-scalars
var stringify = function(val) {
  var t = typeof val;
  if (t === 'undefined' || t === 'function' || val !== val) {
    val = null;
  }
  return JSON.stringify(val);
};

// convert an object to an array
// this is needed for the sort function
var toArray = function(obj) {
  if (typeof obj.length === 'number') {
    return _slice.call(obj);
  }
  var a = [], k = Object.keys(obj),
      i = 0, l = k.length;
  for (; i < l; i++) {
    a.push(obj[k[i]]);
  }
  return a;
};