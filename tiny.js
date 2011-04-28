/**
 * node-tiny : (c) Copyright 2011, Christopher Jeffrey (http://epsilon-not.net/)
 *  (see LICENSE for more info)
 *  An embedded/in-process document/object store for node.
 *  Largely inspired by nStore.
 *  No schemas, just store your objects.
 *  - 
 *  node-tiny uses indexes for stored and flatenned JSON objects.
 *  This allows for querying which should go easy on the memory.
 *  There is Mongo-style querying included, as well as a 
 *  sort of map-reduce-like interface, similar to Couch's "views".
 *  This is a work in progess, not recommended for any 
 *  serious production use.
 */

var fs = require('fs');

// debug messages?
var _DEBUG = true; 

// the amount of bytes at which properties 
// are no longer cached, 1kb by default
var CACHE_LIMIT = 1024; 

// a token to recognize deleted properties
// maybe use null or vertical tab? 
// JSON might have trouble with other things
var DELETED = '\r'; 

var debug;
if (_DEBUG) {
  debug = function() {
    var args = _slice.call(arguments);
    args.unshift('Tiny:');
    return console.log.apply(console, args);
  };
} else {
  debug = function() {};
}

var Tiny = module.exports = function(name, func) { 
  if (!(this instanceof Tiny)) {
    return new Tiny(name, func);
  }
  this.name = name;
  if (name.charAt(0) !== '/') {
    this.name = __dirname + '/' + name;
  }
  this._load(func);
};

var Lookup = function(index) {
  this[0] = index[0];
  this[1] = index[1];
};

Tiny.open = Tiny;

Tiny.limit = function(n) {
  CACHE_LIMIT = n;
  return this;
};

Tiny.prototype._load = function(func) {
  var self = this;
  
  self._cache = {};
  self._queue = [];
  self._busy = false;
  
  var data = new Buffer(4 * 1024); // size of the chunks
  
  var 
    key,
    state = 'key',
    lines = {}, // each line array will contain 2 indexes: the pointer position, and the length
    start = 0, // the start position of a new line/key
    total = 0;
  
  fs.open(self.name, 'a+', 0666, function(err, fd) {
    if (err) return func && func.call(self, err);
    (function read(pos, done) {
      fs.read(fd, data, 0, data.length, pos, function(err, bytes) {
        if (err || !bytes) return done(err);
        
        for (var i = 0; i < bytes; i++) {
          if (data[i] === 9) { // tab
            state = 'data';
            // the key may have been cut off by the transition
            // from one chunk to another, we need to concatenate
            if (start < pos) {
              key += data.slice(0, i).toString('utf-8');
            } else {
              key = data.slice(start - pos, i).toString('utf-8');
            }
            // starting pos of the JSON data
            lines[key] = [ pos + i ]; 
          } else if (data[i] === 10) { // line feed
            state = 'key';
            // set the starting position of the 
            // new line including the key
            start = pos + i + 1;
            // set the length of the previous line
            lines[key][1] = start - lines[key][0]; 
          }
        }
        
        if (state === 'key') {
          key = data.slice(start - pos, i).toString('utf-8');
        }
        
        total += bytes;
        
        read(pos + bytes, done);
      });
    })(0, function(err) {
      debug('Done parsing.');
      self._fd = fd;
      self._total = total;
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
          self._cache[docKey][propKey] = new Lookup(index);
          loop();
        }
      }, function done() {
        process.nextTick(function() {
          debug('DB open.');
          if (func) func.call(self, err, self);
        });
      });
    });
  });
};

// curry on an error
Tiny.prototype._err = function(func, err, ret) {
  err = err || 'Not found.';
  debug(err + '');
  return func && func.call(this, new Error(err), ret);
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
          // seems to be 1 byte too long in tests 
          self._total + total + realKeySize - 1, 
          propSize - realKeySize 
        ]);
      }
      
      total += propSize;
      data.push(prop);
    }
  });
  
  data = new Buffer(data.join(''));
  
  fs.write(self._fd, data, 0, data.length, self._total, function(err, written) {
    self._total += written;
    queue.forEach(function(item) {
      item.func.call(self, err);
    });
    queue = null;
    data = null;
    process.nextTick(function() {
      self._busy = false;
      if (func) func.call(self, err);
      self.commit();
    });
  });
};

// set/insert/save/update a document/object
Tiny.prototype._set = function(docKey, _doc, func, flag) {
  var self = this, doc = {}; // the new stringified doc
  var cache = self._cache;
  
  debug('setting doc :', docKey);
  
  // if there are any properties in the cache
  // that were excluded on the input object
  // need to explicitly mark them as deleted
  // on the stringified object
  if (flag !== 'update' && cache[docKey]) {
    for (var k in cache[docKey]) { 
      // implictly set _key to DELETED for 'delete'
      // this assumes ._key is in the cached object
      if (!(k in _doc)) doc[k] = stringify(DELETED);
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
      for (var k in _doc) {
        cache[docKey][k] = _doc[k];
        doc[k] = stringify(_doc[k]);
      }
      
      // make sure it has a key
      doc._key = stringify(docKey);
      break;
    case 'delete':
      delete cache[docKey];
      break;
  }
  
  // the "cache with a reference" code
  /*_doc._key = docKey; // alter to ensure _key
  
  switch (flag) {
    case 'set':
      cache[docKey] = _doc;
      for (var k in _doc) {
        doc[k] = stringify(_doc[k]);
      }
      break;
    case 'update':
      if (!cache[docKey]) {
        cache[docKey] = {};
      }
      for (var k in _doc) {
        cache[docKey][k] = _doc[k];
        doc[k] = stringify(_doc[k]);
      }
      break;
    case 'delete':
      delete cache[docKey];
      break;
  }*/
  
  self._queue.push({
    docKey: docKey, 
    doc: doc, 
    func: func
  });
  
  if (func) self.commit();
};

Tiny.prototype.set = function(docKey, doc, func) {
  return this._set(docKey, doc, func, 'set');
};

// updates the document, do not overwrite missing properties.
Tiny.prototype.update = function(docKey, doc, func) {
  if (!this._cache[docKey]) return this._err(func, 'No such key.'); 
  return this._set(docKey, doc, func, 'update');
};

// remove a document...
Tiny.prototype.remove = function(docKey, func) {
  if (!this._cache[docKey]) return this._err(func, 'No such key.'); 
  return this._set(docKey, {}, func, 'delete'); 
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
      return self._err(func, err);
    }
    try {
      data = JSON.parse(data);
    } catch(err) {
      return self._err(func, err); 
    }
    // make sure to ignore if its deleted
    if (data === DELETED) {
      return self._err(func); 
    }
    if (func) func.call(self, null, data);
  });
};

// simple function to get a stored object
// .get() can either be deep or shallow
// if it is shallow, only the properties
// under the CACHE_LIMIT will be retrieved
Tiny.prototype.get = function(docKey, func, shallow) {
  var self = this, cache = self._cache[docKey];
  if (!cache) return self._err(func);
  var doc = {};
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
  var self = this;
  self.fetch(function() {
    return true;
  }, function(err, docs) {
    if (func) func.call(self, err, docs); 
  }, !deep);
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

// clean up the append-only mess
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
// > require('tiny')('mydb.tiny').dump(true);
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
      return self._err(func, 'Dump failed.');
    }
    docs.forEach(function(doc) {
      data[doc._key] = doc;
      delete doc._key;
    });
    data = JSON.stringify(data, null, pretty ? 2 : 0);
    fs.writeFile(self.name + '.json', data, func);
    console.log('Tiny: Dump was successful.');
    data = null; // kill references to make
    docs = null; // sure the GC cleans up
  }, true);
};

// hypothetical features...

// "dont cache these properties"
// might not be especially helpful as a lookup
// object might use more memory than the data itself
// Tiny.prototype.ignore = function() {
//   if (!db._ignore) db._ignore = [];
//   db._ignore = db._ignore.concat(_slice.call(arguments));
// };

// "only cache these properties"
// Tiny.prototype.index = function() {
//   if (!db._index) db._index = [];
//   db._index = db._index.concat(_slice.call(arguments));
// };

// ======================= QUERYING ======================= //
Tiny.prototype.fetch = function(map, done, shallow) {
  var self = this, docs = [];
  para(self._cache, function(loop, cache, docKey, cur) {
    if (map.call(self, cache, cur) === true) {
      self.get(docKey, function(err, doc) {
        docs.push(doc);
        loop();
      }, shallow);
    } else { 
      loop();
    }
  }, function() {
    if (!docs.length) {
      return done.call(self, new Error('No Records'), docs);
    }
    if (done) done.call(self, null, docs);
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
  
  // sorting functions for queries
  var sort = function(obj) {
    var keys = _slice.call(arguments, 1); 
    return toArray(obj).filter(function(v) { 
      keys.forEach(function(k) { if (v) v = v[k]; });
      return !!(v !== undefined);
    }).sort(function(a, b) {
      keys.forEach(function(k) { a = a[k]; b = b[k]; }); 
      if (!/^[\d.]+$/.test(a)) {
        a = (a+'').toLowerCase().charCodeAt(0);
        b = (b+'').toLowerCase().charCodeAt(0);
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
  
  // expose these as class methods
  Tiny.sort = sort;
  
  // the actual .query() function
  return function query(where, func, options) {
    var self = this;
    where = where || {};
    options = options || {};
    var skip = options.skip || 0, limit = options.limit; 
    self.fetch(function(doc, total) { 
      if (total < skip) return 'continue';
      if (limit && (total-skip) > limit) return 'break';
      if (testStatement(where, doc)) {
        return true;
      }
    }, function(err, results) {
      if (options.desc) {
        results = sort.desc.apply(null, [results].concat(options.desc));
      } else if (options.asc) {
        results = sort.asc.apply(null, [results].concat(options.asc));
      }
      if (options.one) results = results[0];
      if (options.count) results = results.length;
      if (func) func.call(self, null, results);
    }, options.shallow);
  };
})();

// the same thing as query except with a chainable interface
Tiny.prototype.find = function() {
  var self = this, args = _slice.call(arguments);
  if (!args[0]) args[0] = null;
  if (typeof args[1] === 'function') {
    return self.query.apply(self, args);
  }
  var options = {};
  var chain = function(func) {
    return self.query.apply(self, args.concat(func, options));
  };
  chain.select = function() {
    options.select = Array.isArray(arguments[0]) 
      ? arguments[0] 
      : _slice.call(arguments)
    ;
    return chain;
  };
  chain.count = function() {
    options.count = true;
    return chain;
  };
  chain.desc = function() {
    options.desc = _slice.call(arguments);
    return chain;
  };
  chain.asc = function() {
    options.asc = _slice.call(arguments);
    return chain;
  };
  chain.limit = function(limit) {
    options.limit = limit;
    return chain;
  };
  chain.skip = function(skip) {
    options.skip = skip;
    return chain;
  };
  // if shallow is true, only properties less than 1kb are available to the doc object
  // when no properties are explicitly selected
  chain.shallow = function() {
    options.shallow = true;
    return chain;
  };
  chain.one = function() {
    options.one = true;
    options.limit = 1;
    return chain;
  };
  return chain;
};

// ========== helper functions ========== //
var _slice = [].slice;

var para = function(obj, looper, func) {
  var cur = 0, k = Object.keys(obj), 
      i = 0, l = k.length;
  if (!l) return func && func();
  var next = function() {
    if (++cur === l) func && func();
  };
  for (; i < l; i++) {
    looper(next, obj[k[i]], k[i], cur);
  }
};

// safe stringify
var stringify = function(val) {
  var t = typeof val;
  if (t === 'undefined' || t === 'function' || val !== val) {
    val = null;
  }
  return JSON.stringify(val);
};

var toArray = function(obj) {
  if (typeof obj.length === 'number') {
    return _slice.call(obj);
  }
  var a = [];
  for (var k in obj) a.push(obj[k]);
  return a;
};