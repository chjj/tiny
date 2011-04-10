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
 *  This is a work in progess, not recommend it for any 
 *  serious production use.
 */

var fs = require('fs');

var CACHE_LIMIT = 1024; // the amount of bytes at which properties are no longer cached

// precaching will cache all small properties (<1kb) into the memory when the database loads
// this will improve performance, but potentially use more memory
// enable this if you have a database that isnt moderately large
var ENABLE_PRECACHING = false;

// a token to recognize deleted properties
var DELETED = '\r'; // maybe use null or vertical tab?

/* 
  Before getting to the code, it helps to define some 
    terms here just to keep things straight
  docKey - the key for a doc
  propKey - the key of a document's property
  realKey - docKey + '.' + propKey :: this is the key by which the 
    property is actually stored in the database/index
  prop - the actual data of a property i.e. props[realKey];
  doc - a collection of props with propKeys as their keys
  index - a flattened collection of props with realKeys as their keys
*/

var Tiny = function(name, func) { 
  if (!(this instanceof Tiny)) 
    return new Tiny(name, func);
  this.name = name;
  if (!/^\.?\//.test(name)) 
    this.name = __dirname+'/'+name;
  this._load(func);
}, $tiny = Tiny.prototype;
module.exports = Tiny;

Tiny.limit = function(n) {
  CACHE_LIMIT = n;
  return this;
};

Tiny.precache = function() {
  ENABLE_PRECACHING = true;
  return this;
};

$tiny._load = function(func) {
  var self = this;
  
  self._index = {};
  self._cache = {};
  self._cacheSize = 0; // this may be useful for limiting total cache size in the future
  self._queue = [];
  self._busy = false;
  
  var data = new Buffer(4 * 1024); // size of the chunks
  
  var 
    key,
    lines = {}, // each line array will contain 2 indexes: the pointer position, and the length
    start = 0, // the start position of a new line/key
    total = 0;
  
  fs.open(self.name, 'a+', 0666, function(err, fd) {
    if (err) return func && func.call(self, err);
    (function read(pos, done) {
      fs.read(fd, data, 0, data.length, pos, function(err, bytes) {
        if (err || !bytes) return done(err);
        
        if (bytes < data.length) data.length = bytes;
        
        for (var i = 0, l = data.length; i < l; i++) {
          if (data[i] === 9) { // tab
            key = data.slice(start - pos, i).toString('utf-8').trim();
            lines[key] = [ pos + i ]; // starting pos of the JSON data
          } else if (data[i] === 10) { // line feed
            start = pos + i;
            lines[key][1] = start - lines[key][0]; // get the length
          }
        }
        
        total += bytes;
        
        read(pos + bytes, done);
      });
    })(0, function(err) {
      process.nextTick(function() {
        self._fd = fd;
        self._total = total;
        self._index = lines;
        if (ENABLE_PRECACHING) {
          var lookups = [];
          for (var realKey in self._index) {
            if (self._index[realKey][1] < CACHE_LIMIT) 
              lookups.push(realKey);
          }
          self._lookupMany(lookups, done);
        } else {
          done();
        }
        function done() {
          if (func) func.call(self, err, self);
        }
      });
    });
  });
};

// commit the changes to the fd
$tiny.commit = function(func) {
  var self = this;
  var data, queue = self._queue;
  
  if (self._busy || !queue.length || !self._fd) return;
  
  self._busy = true;
  self._queue = [];
  data = [];
  
  queue.forEach(function(q) {
    var docKey = q[0], doc = q[1], total = 0;
    Object.keys(doc).forEach(function(propKey) {
      var 
        prop = doc[propKey], 
        realKey = docKey+'.'+propKey, 
        // maybe do the realKey concatenation with the string here, easier index arithmetic
        propSize = Buffer.byteLength(prop),
        realKeySize = Buffer.byteLength(realKey+'\t');
      
      self._index[realKey] = [ 
        self._total + total + realKeySize - 1, // seems to be 1 byte too long in tests 
        propSize - realKeySize 
      ];
      
      total += propSize;
      data.push(prop);
    });
  });
  
  data = new Buffer(data.join(''));
  
  fs.write(self._fd, data, 0, data.length, self._total, function(err, written) {
    self._total += written;
    queue.forEach(function(q) {
      if (q[2]) q[2].call(self, err);
    });
    process.nextTick(function() {
      self._busy = false;
      if (func) func.call(self, err);
      self.commit();
    });
  });
};

// set/insert/save/update a document/object
// notes to self:
// check to see if the properties are different from the cached ones?
// only update/include the properties that have changed?
//   -- this might affect the !extend conditional
// maybe just put _key on here and nowhere else
$tiny.set = function(docKey, doc, func, extend) {
  var self = this;
  
  if (!doc._key) doc._key = docKey;
  
  // right here we need to find all the current properties not
  // included in the input object and set all those properties 
  // to the deleted token - if we werent storing flattened objects, 
  // this would be a lot easier
  if (!extend) {
    for (var realKey in self._index) {
      var s = realKey.split('.');
      if (s[0] === docKey) {
        if (!(s[1] in doc)) doc[s[1]] = DELETED;
      }
    }
  }
  
  Object.keys(doc).forEach(function(propKey) {
    var prop = JSON.stringify(doc[propKey]);
    var realKey = docKey+'.'+propKey;
    var size = Buffer.byteLength(prop); // are .byteLength's expensive? probably, maybe just go by length
    if (size < CACHE_LIMIT) {
      if (self._cache[realKey]) {
        self._cacheSize -= self._index[realKey][1];
      }
      self._cache[realKey] = doc[propKey];
      self._cacheSize += size;
    } else if (self._cache[realKey]) { // the prop used to be below the cache limit, but that has changed
      delete self._cache[realKey];
      self._cacheSize -= self._index[realKey][1];
    }
    doc[propKey] = realKey+'\t'+prop+'\n';
  });
  
  self._queue.push([docKey, doc, func]);
  if (func) self.commit();
};

// extends/updates the document, do not overwrite missing properties 
// this is necessary because specific properties
// maybe be selected in a query, and a resulting object may only contain a few properties
// --- maybe make this the default .set() ???
$tiny.update = function(docKey, doc, func) {
  return $tiny.set.call(this, docKey, doc, func, true);
};

// curry on an error
$tiny._err = function(func, err) {
  return func && func.call(this, err || (new Error('Not Found.')));
};

// lookup a property, either from cache or by reading a chunk from the FD
// ignore deleted properties
// this may need some cleaning up
$tiny._lookup = function(realKey, func) {
  var self = this;
  var index = self._index[realKey];
  if (!index) return self._err(func); 
  if (self._cache[realKey]) {
    if (self._cache[realKey] === DELETED) return self._err(func); 
    if (func) func.call(self, null, self._cache[realKey]);
  } else {
    // maybe move the shallow/deep logic here
    // simpler, more elegant, but requires more function calls from a query perspective
    //if (shallow && index[1] > CACHE_LIMIT) return self._err(func); 
    readChunk(self._fd, index[0], index[1], function(err, data) {
      if (err) return self._err(func, err);
      try {
        data = JSON.parse(data);
      } catch(err) {
        return self._err(func, err); 
      }
      if (index[1] < CACHE_LIMIT) {
        self._cache[realKey] = data;
        self._cacheSize += index[1];
      }
      if (data === DELETED) return self._err(func); 
      if (func) func.call(self, null, data);
    });
  }
};

// make this automatically run structureProps?
// maybe not because if theres only one doc its hard to abstract - see: .get()
$tiny._lookupMany = function(realKeys, func) { 
  var self = this, props = {}, i = 0;
  if (!realKeys || !realKeys.length) 
    return func.call(self, props);
  realKeys.forEach(function(realKey) {
    self._lookup(realKey, function(err, data) {
      if (!err) props[realKey] = data;
      if (++i === realKeys.length && func) {
        func.call(self, structureProps(props));
      }
    });
  });
};

// simple function to get a stored object
// .get() can either be deep or shallow
// if it is shallow, only the properties
// under the CACHE_LIMIT will be retrieved
$tiny.get = function(docKey, func, shallow) {
  var self = this;
  self._lookupMany(self.getRealKeysByDocKey(docKey, shallow), function(docs) {
    if (func) func.call(self, null, docs[docKey]);
  });
};

// this function is more complicated than it needs to be conceptually
// it needs to check the index to see what properties the target object
// for deletion currently has, add them to a newly created object, and
// set every property to the deleted token, .set() needs to do something similar
// so that all properties are updated when a document is overwritten
$tiny.remove = function(docKey, func) {
  var del, realKey;
  for (realKey in this._index) {
    var s = realKey.split('.');
    if (s[0] === docKey) {
      if (!del) del = {};
      del[s[1]] = DELETED;
    }
  }
  if (!del) return func.call(this, new Error('No such key.'));
  del._key = DELETED;
  this.set(docKey, del, func); 
};

$tiny.all = function(func, deep) { // .all() is shallow by default
  var self = this, i = 0, msg, docs = {}, done = {};
  var realKeys = Object.keys(self._index);
  realKeys.forEach(function(realKey) {
    var docKey = realKey.split('.')[1];
    if (done[docKey]) return;
    done[docKey] = true;
    self.get(docKey, function(err, doc) {
      docs[docKey] = doc;
      if (!msg) msg = err;
      if (++i === realKeys.length) 
        func.call(self, msg, toArray(docs)); 
    }, !deep);
  });
};

$tiny.each = function(func, deep) { // .each() is shallow by default
  var self = this;
  self.all(function(err, docs) {
    docs.forEach(function(doc) {
      func.call(self, doc, doc._key);
    });
  }, deep);
};

$tiny.close = function(func) {
  var self = this;
  self._busy = true;
  fs.close(self._fd, function() {
    delete self._fd;
    self._busy = false;
    if (func) func.call(self);
  });
};

// clean up the append-only mess -- havent tested this yet
$tiny.compact = function(func) {
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
  }, true); // make sure its deep
};

$tiny.clearCache = function() {
  this._cache = {};
  this._cacheSize = 0;
};

// Grab an text excerpt from an fd by its byte index and its length.
var readChunk = function(fd, pos, length, func) {
  var data = new Buffer(length);
  fs.read(fd, data, 0, length, pos, function(err, bytes) {
    func(err, data.toString('utf-8'));
  });
};

// ====== QUERYING =========================================================================================================

// i know i use this somewhere
/*var flattenDocs = function(docs) {
  var props = {};
  for (var docKey in docs) {
    var doc = docs[docKey];
    for (var propKey in doc) {
      props[docKey+'.'+propKey] = doc[propKey];
    }
  }
  return props;
};*/

var structureProps = function(props) {
  var docs = {};
  for (var realKey in props) {
    var s = realKey.split('.');
    var docKey = s[0], propKey = s[1];
    if (!docs[docKey]) docs[docKey] = { _key: docKey };
    docs[docKey][propKey] = props[realKey];
  }
  return docs;
};

// overly descriptive function names below, 
// and they still dont accurately convey what the functions do =(
$tiny.getRealKeysByDocKey = function(docKey, shallow) {
  var self = this, realKeys = [];
  for (var realKey in self._index) {
    if (shallow && self._index[realKey][1] < CACHE_LIMIT) continue;
    if (realKey.split('.')[0] === docKey) {
      realKeys.push(realKey);
    }
  }
  return realKeys;
};

// maybe move all the shallow/deep logic to the lowest level --- ._lookup() ??
// it may entail several more function calls however, which is unfortunate,
// because in the end it would make things simpler
$tiny.getRealKeysByPropKeys = function(propKeys, shallow) {
  var self = this, realKeys = [];
  if (propKeys && !Array.isArray(propKeys)) 
    propKeys = [ propKeys ];
  for (var realKey in self._index) {
    var index = self._index[realKey];
    var noProps = !!(!propKeys || !propKeys.length);
    var hasProp = !!(propKeys && (propKeys.indexOf(realKey.split('.')[1]) !== -1));
    if (hasProp || (noProps && (!shallow || (shallow && index[1] < CACHE_LIMIT)))) {
      realKeys.push(realKey);
    }
  }
  return realKeys;
};

// closely examine the map function to determine 
// what property names are relevant to the query 
var examineMapFunction = function(map) {
  var rel = {};
  map = map.toString();
  // get the name of the "doc" parameter
  map.replace(/^\s*function[^(]*\(\s*([^,)]+)/i, function(__, param) {
    // put the name of the doc param into a regex that looks 
    // for the names of properties that are being compared
    
    // the dollar sign is really the only thing we need to escape here
    param = param.trim().replace(/\$/g, '\\$'); 
    
    // normalize property names
    map = map.replace(new RegExp(param+'\\s*\\.\\s*([$_\\w]+)', 'g'), param+'["$1"]');
    
    map.replace(new RegExp(param+'\\s*\\[\\s*("|\')([\\s\\S]+?)\\1\\s*\\]', 'g'), function(__, __, name) {
      rel[name] = true;
    });
  });
  rel = Object.keys(rel);
  return rel.length && rel;
};

// mapReduce-like method for internal (and possibly external use) see README for usage
$tiny.fetch = function(rel, map, done, shallow) {
  var self = this, lookups;
  if (!Array.isArray(rel)) { shallow = done; done = map; map = rel; rel = undefined; }
  if (!rel) rel = examineMapFunction(map);
  lookups = self.getRealKeysByPropKeys(rel, shallow);
  self._lookupMany(lookups, function(docs) {
    var lookups = [], i = 0;
    for (var docKey in docs) {
      var r = map.call(self, docs[docKey], i++);
      if (r === 'break' || r === false) break;
      if (r === 'continue' || r == null) continue;
      if (r === true) {
        lookups = lookups.concat(self.getRealKeysByDocKey(docKey, shallow));
      } else {
        lookups = lookups.concat((docKey+'.'+r.join('|'+docKey+'.')).split('|'));
      }
    }
    self._lookupMany(lookups, function(docs) {
      docs = toArray(docs);
      if (!docs.length) return done.call(self, new Error('No Records'), docs);
      done.call(self, null, docs);
    });
  });
};

// mongo-style querying
$tiny.query = (function() {
  // deeply traverse the an entire object, gather up all non-operator, non-array/numeric keys
  var getRelevantKeys = function(statement) {
    var rel = {};
    (function getKeys(obj) {
      for (var k in obj) {
        if (k[0] !== '$' && !Array.isArray(obj)) rel[k] = true;
        if (obj[k] && typeof obj[k] === 'object') getKeys(obj[k]);
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
        var found = false;
        for (var k in a) {
          var va = a[k];
          for (var k in b) {
            var vb = b[k];
            if (va == vb) found = true;
          }
        }
        return found;
      },
      $nin: function(a, b) { // does not contain any of
        return !ops['$in'](a, b);
      },
      $all: function(a, b) { // contains all...
        var need = Object.keys(b).length;
        var found = 0;
        for (var k in a) {
          var va = a[k];
          for (var k in b) {
            var vb = b[k];
            if (va == vb) found++;
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
    // the back bone of the query really
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
  // sorting functions for queries, ...might as well expose them as class methods
  var sort = Tiny.sort = function(obj) {
    var keys = Array.prototype.slice.call(arguments, 1); 
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
$tiny.find = function() {
  var self = this, args = [].slice.call(arguments);
  if (!args[0]) args[0] = null;
  if (typeof args[1] === 'function') {
    return $tiny.query.apply(self, args);
  }
  var options = {};
  var chain = function(func) {
    return $tiny.query.apply(self, args.concat(func, options));
  };
  chain.select = function(select) {
    options.select = Array.isArray(select) 
      ? select 
      : [].slice.call(arguments)
    ;
    return chain;
  };
  chain.count = function() {
    options.count = true;
    options.select = ['_key']; // have it select a property we know exists
    return chain;
  };
  chain.desc = function() {
    options.desc = [].slice.call(arguments);
    return chain;
  };
  chain.asc = function() {
    options.asc = [].slice.call(arguments);
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

var toArray = function(obj) {
  var a = [];
  if (typeof obj.length === 'number') 
    return a.slice.call(obj);
  for (var k in obj) a.push(obj[k]);
  return a;
};