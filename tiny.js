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

// precaching will cache all small properties (<1kb) into 
// the memory when the database loads. this will improve 
// performance and minimize reads, but potentially use 
// more memory enable this if you have a database that isnt 
// moderately large
var ENABLE_PRECACHING = false;

// a token to recognize deleted properties
// maybe use null or vertical tab? 
// JSON might have trouble with other things
var DELETED = '\r'; 

// Before getting to the code, it helps to define some 
// terms here just to keep things straight
//  docKey: the key for a doc
//  propKey: the key of a document's property
//  realKey: docKey + '.' + propKey :: this is the key by which the 
//    property is actually stored in the database/index
//  prop: the actual data of a property i.e. props[realKey];
//  doc: a collection of props with propKeys as their keys
//  index: a flattened collection of props with realKeys as their keys

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

Tiny.limit = function(n) {
  CACHE_LIMIT = n;
  return this;
};

Tiny.precache = function() {
  ENABLE_PRECACHING = true;
  return this;
};

// a different index structure may be better:
// this._index['test.prop1'] = {
//   pos: 100,
//   size: 10,
//   doc: 'test',
//   prop: 'prop1'
// };

Tiny.prototype._load = function(func) {
  var self = this;
  
  self._index = {};
  self._cache = {};
  self._cacheSize = 0; // this may be useful for limiting total cache size in the future
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
            state = 'line';
            // the key may have been cut off by the transition
            // from one chunk to another, we need to concatenate
            if (start < pos) {
              key += data.slice(0, i).toString('utf-8');
            } else {
              key = data.slice(start - pos, i).toString('utf-8');
            }
            lines[key] = [ pos + i ]; // starting pos of the JSON data
          } else if (data[i] === 10) { // line feed
            state = 'key';
            // set the starting position of the new line
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
      process.nextTick(function() {
        self._fd = fd;
        self._total = total;
        self._index = lines;
        // temporary fix to remove 
        // deleted docs from the index
        /*for (var realKey in self._index) {
          if (self._index[realKey]._key === DELETED) {
            delete self._index[realKey];
          }
        }*/
        if (ENABLE_PRECACHING) {
          var lookups = [];
          for (var realKey in self._index) {
            if (self._index[realKey][1] < CACHE_LIMIT) {
              lookups.push(realKey);
            }
          }
          self._lookupMany(lookups, done);
        } else {
          done();
        }
        function done() {
          debug('DB open.');
          if (func) func.call(self, err, self);
        }
      });
    });
  });
};

// curry on an error
Tiny.prototype._err = function(func, err, ret) {
  err = err || 'Not found.';
  debug(err);
  return func && func.call(this, new Error(err), ret);
};

// commit the changes to the fd
// this and .set() both need cleaning up
Tiny.prototype.commit = function(func) {
  var self = this;
  var data, queue;
  
  if (self._busy || !self._queue.length || !self._fd) return;
  
  debug('committing - ' + self._queue.length + ' items in queue.');
  
  self._busy = true;
  queue = self._queue;
  self._queue = [];
  data = [];
  
  queue.forEach(function(item) {
    var total = 0, propKey, prop, realKey, propSize, realKeySize;
    for (propKey in item.doc) {
      realKey = item.docKey + '.' + propKey;
      prop = realKey + '\t' + item.doc[propKey] + '\n';
      propSize = Buffer.byteLength(prop);
      realKeySize = Buffer.byteLength(realKey + '\t');
      
      /*if (item.doc.deleted) {
        delete self._cache[realKey];
        delete self._index[realKey];
      } else {*/
        self._index[realKey] = [ 
          self._total + total + realKeySize - 1, // seems to be 1 byte too long in tests 
          propSize - realKeySize 
        ];
      //}
      
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
// we could check to see if the properties are 
// different from the cached ones to optimize things
Tiny.prototype.set = function(docKey, _doc, func, update) {
  var self = this;
  
  var doc = {}; // the new stringified doc
  
  debug('setting doc:', docKey);
  
  if (!_doc._key) _doc._key = docKey;
  
  // right here we need to find all the current properties not
  // included in the input object and set all those properties 
  // to the deleted token - if we werent storing flattened objects, 
  // this would be a lot easier
  if (!update) {
    for (var s in self._index) {
      s = s.split('.');
      if (s[0] === docKey && !(s[1] in _doc)) {
        doc[s[1]] = DELETED;
      }
    }
  }
  
  for (var propKey in _doc) {
    doc[propKey] = stringify(_doc[propKey]);
    var realKey = docKey + '.' + propKey;
    var size = doc[propKey].length; // use .length, doesnt need to be exact
    if (size < CACHE_LIMIT) {
      if (self._cache[realKey]) {
        self._cacheSize -= self._index[realKey][1];
      }
      self._cache[realKey] = _doc[propKey];
      self._cacheSize += size;
    } else if (self._cache[realKey]) { // the prop used to be below the cache limit, but that has changed
      delete self._cache[realKey];
      self._cacheSize -= self._index[realKey][1];
    }
  }
  
  self._queue.push({
    docKey: docKey, 
    doc: doc, 
    func: func, 
    // temporary fix, should only need to set key to deleted if this works
    //deleted: !!(_doc._key === DELETED)
  });
  if (func) self.commit();
};

// extends/updates the document, do not overwrite missing properties.
// this is necessary because specific properties can be selected in 
// a query, and a resulting object may only contain a few properties.
Tiny.prototype.update = function(docKey, doc, func) {
  return Tiny.prototype.set.call(this, docKey, doc, func, true);
};

// grab an text excerpt from the FD 
Tiny.prototype._read = function(pos, length, func) {
  var data = new Buffer(length);
  fs.read(this._fd, data, 0, length, pos, function(err, bytes) {
    func(err, data.toString('utf-8'));
  });
};

// lookup a property, either from cache or by reading a chunk from the FD
// ignore deleted properties, deep by default
Tiny.prototype._lookup = function(realKey, func, shallow) {
  var self = this;
  var index = self._index[realKey];
  if (!index) { 
    return self._err(func, 'Property not indexed.'); 
  }
  //if (self._cache[realKey.split('.')[0] + '._key'] === DELETED) {
  //if (self._cache[index.doc + '._key'] === DELETED) {
  //  return self._err(func);
  //}
  if (self._cache[realKey]) {
    if (self._cache[realKey] === DELETED) {
      return self._err(func); 
    }
    if (func) func.call(self, null, self._cache[realKey]);
  } else {
    // check to see if the lookup is shallow
    // if it is, dont return properties exceeding
    // the cache limit
    if (shallow && index[1] > CACHE_LIMIT) {
      return self._err(func); 
    }
    self._read(index[0], index[1], function(err, data) {
      if (err) { // maybe throw?
        return self._err(func, err);
      }
      try {
        data = JSON.parse(data);
      } catch(err) {
        return self._err(func, err); 
      }
      if (index[1] < CACHE_LIMIT) {
        self._cache[realKey] = data;
        self._cacheSize += index[1];
      }
      // make sure to ignore if its deleted
      if (data === DELETED) {
        return self._err(func); 
      }
      if (func) func.call(self, null, data);
    });
  }
};

// an async parallel loop to lookup multiple properties
// it returns them as a structured object (stuctureProps)
// [ 'doc1.title', 'doc2.title' ]
// returns { doc1: { title: ... }, doc2: { title: ... } }
Tiny.prototype._lookupMany = function(realKeys, func, shallow) { 
  var self = this, props = {}, i = 0;
  if (!realKeys || !realKeys.length) {
    return func.call(self, props);
  }
  realKeys.forEach(function(realKey) {
    self._lookup(realKey, function(err, data) {
      if (!err) props[realKey] = data;
      if (++i === realKeys.length && func) {
        func.call(self, structureProps(props));
      }
    }, shallow);
  });
};

// simple function to get a stored object
// .get() can either be deep or shallow
// if it is shallow, only the properties
// under the CACHE_LIMIT will be retrieved
Tiny.prototype.get = function(docKey, func, shallow) {
  var self = this;
  self._lookupMany(self._getRealKeysByDocKey(docKey), function(docs) {
    if (func) func.call(self, null, docs[docKey]);
  }, shallow);
};

// remove a document...
Tiny.prototype.remove = function(docKey, func) {
  var doc, realKey;
  for (realKey in this._index) {
    var s = realKey.split('.');
    if (s[0] === docKey) {
      if (!doc) doc = {};
      doc[s[1]] = DELETED;
    }
  }
  if (!doc) return this._err(func, 'No such key.'); 
  doc._key = DELETED;
  this.set(docKey, doc, func); 
  //this.set(docKey, { _key: DELETED }, func);
};

// return an array containing *every* document
// it is shallow by default, meaning it will
// only lookup and include properties smaller
// than the cache limit (<1kb)
Tiny.prototype.all = function(func, deep) { 
  var self = this, i = 0, msg, docs = {}, done = {};
  var realKeys = Object.keys(self._index);
  realKeys.forEach(function(realKey) {
    var docKey = realKey.split('.')[0];
    if (done[docKey]) return;
    done[docKey] = true;
    self.get(docKey, function(err, doc) {
      docs[docKey] = doc;
      if (!msg) msg = err;
      if (++i === realKeys.length) {
        func.call(self, msg, toArray(docs)); 
      }
    }, !deep);
  });
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

// clear the cache (properties smaller than 1kb)
Tiny.prototype.forget = function() {
  this._cache = {};
  this._cacheSize = 0;
};

// turn a collection of docs into
// a flat collection of properties
// turn: { 'doc1': { 'prop1': 'hi' } }
// into: { 'doc1.prop1': 'hi' }
var flattenDocs = function(docs) {
  var props = {};
  for (var docKey in docs) {
    var doc = docs[docKey];
    for (var propKey in doc) {
      props[docKey + '.' + propKey] = doc[propKey];
    }
  }
  return props;
};

// take a collection of flattened properties
// and structure them into documents
// turn: { 'doc1.prop1': 'hi' }
// into: { 'doc1': { 'prop1': 'hi' } }
var structureProps = function(props) {
  var docs = {};
  for (var realKey in props) {
    var s = realKey.split('.');
    var docKey = s[0], propKey = s[1];
    if (!docs[docKey]) docs[docKey] = { _key: docKey }; // maybe move this down to fetch 
    docs[docKey][propKey] = props[realKey];
  }
  return docs;
};

// take a docKey e.g. "article_1"
// and return an array of all the document's realKeys
// e.g: [ 'article_1.title', 'article_1.timestamp' ]
Tiny.prototype._getRealKeysByDocKey = function(docKey) {
  var self = this, realKeys = [];
  for (var realKey in self._index) {
    if (realKey.split('.')[0] === docKey) {
      realKeys.push(realKey);
    }
  }
  return realKeys;
};

// take an array of propKeys e.g. [ 'title', 'timestamp' ]
// and find all the realKeys that contain these propKeys
// for any document e.g: 
// [ 'doc1.title', 'doc2.title', 'doc1.timestamp', ... ]
Tiny.prototype._getRealKeysByPropKeys = function(propKeys) {
  var self = this, realKeys = [], realKey;
  if (!Array.isArray(propKeys)) {
    propKeys = propKeys ? [ propKeys ] : [];
  }
  for (realKey in self._index) {
    if (!propKeys.length || propKeys.indexOf(realKey.split('.')[1]) !== -1) {
    //if (!propKeys.length || propKeys.indexOf(self._index[realKey].prop) !== -1) {
      realKeys.push(realKey);
    }
  }
  return realKeys;
};

// ======================= QUERYING ======================= //

// how it works:
// since properties are stored individually in the db file
// it is possible to look them up selectively.
// in analyzing a query, we need to start by determining which 
// keys are relevant to the query's comparisons. we then lookup
// these properties and let the callback perform the comparisons
// and return the desired documents/property names. the returned
// properties are then read from the FD, structured and compiled
// and passed into the final callback.


// closely examine the map function to determine 
// what property names are relevant to the query
// for example:
//  function(doc) {
//    if (doc.prop1 && doc.prop2) {
//      return true;
//    }
//  }
// will return [ 'prop1', 'prop2' ]
var examineMapFunction = function(map) {
  var rel = {};
  map = map.toString();
  // get the name of the "doc" parameter
  map.replace(/^\s*function[^(]*\(\s*([^,)]+)/i, function(__, param) {
    // we now put use the name of the doc param in a regex that looks 
    // for the names of properties that are being compared
    
    // the dollar sign is really the only thing we need to escape here
    param = param.trim().replace(/\$/g, '\\$'); 
    
    // normalize property names
    map = map.replace(new RegExp(param + '\\s*\\.\\s*([$_\\w]+)', 'g'), param + '["$1"]');
    
    map.replace(new RegExp(param + '\\s*\\[\\s*("|\')([\\s\\S]+?)\\1\\s*\\]', 'g'), function(__, __, name) {
      rel[name] = true;
    });
  });
  rel = Object.keys(rel);
  return rel.length && rel;
};

// a map-reduce-like function, this is used internally, but
// it is also more efficient than the mongo querying
Tiny.prototype.fetch = function(rel, map, done, shallow) {
  var self = this, lookups;
  if (!Array.isArray(rel)) { shallow = done; done = map; map = rel; rel = undefined; }
  if (!rel) rel = examineMapFunction(map);
  lookups = self._getRealKeysByPropKeys(rel);
  self._lookupMany(lookups, function(docs) {
    var lookups = [], i = 0;
    for (var docKey in docs) {
      var ret = map.call(self, docs[docKey], i++);
      if (ret === 'break' || ret === false) break;
      if (ret === 'continue' || ret == null) continue;
      if (ret === true) {
        lookups = lookups.concat(self._getRealKeysByDocKey(docKey));
      } else {
        lookups = lookups.concat(ret.map(function(propKey) { 
          return docKey + '.' + propKey; 
        }));
      }
    }
    self._lookupMany(lookups, function(docs) {
      docs = toArray(docs);
      if (!docs.length) {
        return done.call(self, new Error('No Records'), docs);
      }
      done.call(self, null, docs);
    }, shallow);
  }, shallow);
};

// mongo-style querying
Tiny.prototype.query = (function() {
  // deeply traverse the an entire object 
  // gather up all non-operator, non-array/numeric keys
  var getRelevantKeys = function(statement) {
    var rel = {};
    (function getKeys(obj) {
      for (var key in obj) {
        if (key.charAt(0) !== '$' && !Array.isArray(obj)) {
          rel[key] = true;
        }
        if (obj[key] && typeof obj[key] === 'object') {
          getKeys(obj[key]);
        }
      }
    })(statement);
    return Object.keys(rel);
  };
  
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
        //for (var i = state.length; i--;) {
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
    self.fetch(getRelevantKeys(where), function(doc, total) {
      if (total < skip) return 'continue';
      if (limit && (total-skip) > limit) return 'break';
      if (testStatement(where, doc)) {
        return options.select || true;
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
    options.select = ['_key']; // have it select a property we know exists
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

// safe stringify
var stringify = function(val) {
  var type = typeof val;
  if (type === 'undefined' || type === 'function' || val !== val) {
    val = null;
  }
  return JSON.stringify(val);
};

var toArray = function(obj) {
  var a = [];
  if (typeof obj.length === 'number') {
    return _slice.call(obj);
  }
  for (var k in obj) a.push(obj[k]);
  return a;
};