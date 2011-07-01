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

var fs = require('fs')
  , path = require('path');

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
  ? console.log
  : function() {};

// the tiny constructor, takes a `name`
// which is really a filename path
var Tiny = function(name, func) {
  if (!(this instanceof Tiny)) {
    return new Tiny(name, func);
  }
  this.name = name;
  this._load(func);
};

// this will cache database objects the same
// object will be returned every time
Tiny.open = (function() {
  var cache = {};
  return function(name, func) {
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
  var data = new Buffer(64 * 1024);

  var key = ''
    , state = 'KEY'
    , lines = {}
    , start = 0
    , pos = 0;

  fs.open(self.name, 'a+', 0666, function(err, fd) {
    if (err) return func && func.call(self, err);
    (function read(done) {
      fs.read(fd, data, 0, data.length, pos, function(err, bytes) {
        if (err || !bytes) return done(err);

        var i = 0, k = 0;

        for (; i < bytes; i++) {
          switch (state) {
            case 'KEY':
              if (data[i] === 0x09) { 
                state = 'DATA';
                start = pos + 1;
                key += data.slice(k, i).toString('ascii');
              }
              break;
            case 'DATA':
              if (data[i] === 0x0A) { 
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

      self._fd = fd;
      self._total = pos;

      debug('Done parsing.');
      debug('Loading small properties into memory.');

      para(lines, function(loop, index, realKey) {
        var s = realKey.split('.')
          , docKey = s[0]
          , propKey = s[1]
          , cache = self._cache;

        if (!cache[docKey]) cache[docKey] = {};

        if (index[1] < CACHE_LIMIT) {
          self._lookup(index, function(err, data) {
            cache[docKey][propKey] = data;
            loop();
          });
        } else {
          // create a lookup for large properties
          cache[docKey][propKey] = new Lookup(index);
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
  var self = this
    , data
    , queue;

  if (self._busy 
      || !self._queue.length 
      || !self._fd) return;

  debug('committing - %s items in queue.', self._queue.length);

  self._busy = true;
  queue = self._queue;
  self._queue = [];
  data = [];

  queue.forEach(function(item) {
    var total = 0
      , propKey
      , prop
      , realKey
      , propSize
      , realKeySize;
    
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
    self._busy = false;

    if (func) func.call(self, err);

    // check the queue again
    self.commit();
  });
};

// grab an text excerpt from the FD
Tiny.prototype._read = function(pos, length, func) {
  var data = new Buffer(length);
  fs.read(this._fd, data, 0, length, pos, function(err, bytes) {
    func(err, data.toString('utf8'));
  });
};

// lookup a property, ignore deleted properties, deep by default
Tiny.prototype._lookup = function(lookup, func) {
  var self = this;
  self._read(lookup[0], lookup[1], function(err, data) {
    if (err) return self._error(func, err);

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
  var self = this
    , doc = {} // the new stringified doc
    , cache = self._cache;

  if (!_doc || typeof _doc !== 'object') {
    return self._error(func, 'Bad object.');
  }

  debug('setting doc: %s', docKey);

  // if there are any properties in the cache
  // that were excluded on the input object
  // need to explicitly mark them as deleted
  // on the stringified object
  if (flag !== 'update' && cache[docKey]) {
    for (var k in cache[docKey]) {
      // implictly set _key to DELETED for 'delete'
      // this assumes ._key is in the cached object
      if (!hasOwnProperty.call(_doc, k)) {
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
        cache[docKey] = { _key: docKey };
      }

      // shallow copy
      var keys = Object.keys(_doc)
        , i = 0
        , l = keys.length
        , propKey;

      for (; i < l; i++) {
        propKey = keys[i];
        cache[docKey][propKey] = _doc[propKey];
        doc[propKey] = stringify(_doc[propKey]);
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
  var self = this
    , doc = {}
    , cache = self._cache[docKey];

  if (!cache) return self._error(func);

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
  var opt = { shallow: !deep };
  this.fetch(opt, function() {
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
  self.all(function(err, docs) { 
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
            self._load(func);
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
  var self = this
    , data = {};

  if (typeof pretty === 'function') {
    func = pretty;
    pretty = undefined;
  }

  // a hack in case the db hasnt loaded
  if (!self._total) { 
    return setTimeout(function() { 
      self.dump(pretty, func); 
    }, 500);
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
  }, true);
};

// # hypothetical features 
// note that these functions are
// completely inert at the moment

// "dont cache these properties"
// might not be especially helpful as a lookup
// object might use more memory than the data itself
Tiny.prototype.ignore = function() {
  if (!this._ignore) this._ignore = [];
  this._ignore = this._ignore.concat(slice.call(arguments));
};

// "only cache these properties"
Tiny.prototype.index = function() {
  if (!this._index) this._index = [];
  this._index = this._index.concat(slice.call(arguments));
};

// ========== QUERYING ========== //
Tiny.prototype.fetch = function(opt, filter, done) {
  var self = this
    , results = []
    , keys
    , cache = self._cache;

  if (!done) {
    done = filter;
    filter = opt;
    opt = {};
  }

  if (opt.count) results = 0;
  if (opt.desc || opt.asc) {
    keys = self._sort(
      opt.asc || opt.desc,
      opt.asc ? 'asc' : 'desc'
    );
  } else {
    keys = Object.keys(cache);
  }

  para(keys, function(loop, docKey) {
    if (filter.call(self, cache[docKey], docKey) === true) {
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
    // type coercion here
    if (results == 0) {
      return self._error(done, 'No Records', results);
    }

    if (opt.skip) results = results.slice(opt.skip);
    if (opt.limit) results = results.slice(0, opt.limit);
    if (opt.one || opt.single) results = results[0];

    if (done) done.call(self, null, results);
  });
};

Tiny.prototype._sort = function(prop, order) {
  var self = this
    , cache = self._cache
    , keys = Object.keys(cache);

  keys = keys.filter(function(k) {
    return cache[k][prop] !== undefined;
    //return hasOwnProperty.call(cache[k], prop);
  }).sort(function(a, b) {
    a = cache[a][prop];
    b = cache[b][prop];
    if (!/^-?[\d.]+$/.test(a)) {
      a = (a + '').toLowerCase().charCodeAt(0);
      b = (b + '').toLowerCase().charCodeAt(0);
    }
    return a > b ? 1 : (a < b ? -1 : 0);
  });

  if (order === 'desc') {
    keys = keys.reverse();
  }

  return keys;
};

// mongo-style querying
Tiny.prototype.query = (function() {
  // operator logic
  var ops = {
    $lt: function(a, b) {
      return a < b;
    },
    $lte: function(a, b) {
      return a <= b;
    },
    $gt: function(a, b) {
      return a > b;
    },
    $gte: function(a, b) {
      return a >= b;
    },
    $eq: function(a, b) {
      return a == b;
    },
    $ne: function(a, b) {
      return a != b;
    },
    $regex: function(a, b) {
      return b.test(a);
    },
    // contains any of...
    $in: function(a, b) { 
      for (var k in a) {
        var va = a[k];
        for (var k in b) {
          var vb = b[k];
          if (va == vb) {
            return true;
          }
        }
      }
      return false;
    },
    // does not contain any of...
    $nin: function(a, b) { 
      return !ops.$in(a, b);
    },
    // contains all...
    $all: function(a, b) { 
      var found = 0;
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
      return found === Object.keys(b).length;
    },
    $exists: function(a, b) {
      return b ? a !== undefined : a === undefined;
    },
    $size: function(a, b) { // why? because i can
      return Buffer.byteLength(a) === b;
    }
  };

  // test an object/statement to see
  // if it matches a document's properties.
  // this is the back bone of the query really.
  // some crazy recursion needs to be going
  // on because of the $or operator.
  var test = function(obj, doc) {
    if (Array.isArray(obj)) {
      var fails = 0
        , i = obj.length;
      while (i--) {
        if (!test(obj[i], doc)) fails++;
      }
      return fails < obj.length;
    }
    for (var propKey in obj) {
      var targetProp = obj[propKey];
      var prop = doc[propKey];
      if (propKey === '$or') {
        if (!test(targetProp, doc)) {
          return false;
        }
      } else if (targetProp && typeof targetProp === 'object') {
        var propOperations = targetProp;
        for (var operator in propOperations) {
          targetProp = propOperations[operator];
          if (operator === '$or') {
            if (!test(targetProp, doc)) return false;
          } else if (ops[operator] && !ops[operator](prop, targetProp)) {
            return false;
          }
        }
      } else {
        if (prop != targetProp) return false;
      }
    }
    return true;
  };

  // the actual .query() function
  return function query(where, func, opt) {
    where = where || {};
    opt = opt || {};
    this.fetch(opt, function(doc) {
      if (test(where, doc)) {
        return true;
      }
    }, func);
  };
})();

// the same thing as query except with a chainable interface
Tiny.prototype.find = function() {
  var self = this
    , args = slice.call(arguments)
    , opt = {};

  if (!args.length) args.push({});

  if (typeof args[1] === 'function') {
    return self.query.apply(self, args);
  }

  var chain = function(func) {
    return self.query.apply(self, args.concat(func, opt));
  };

  // doesnt work at the moment
  // may be removed entirely
  chain.select = function() {
    console.error('Tiny:'
      + ' Warning: `.select()` does not work currently.'
      + ' It may be removed in future versions.'
    );
    opt.select = Array.isArray(arguments[0])
      ? arguments[0]
      : slice.call(arguments);
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
  // if shallow is true, only properties less than
  // cache_limit are available to the doc object
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
var slice = [].slice
  , hasOwnProperty = Object.prototype.hasOwnProperty;

// a parallel foreach
var para = function(obj, looper, func) {
  var c = 0
    , i = 0
    , len
    , keys
    , key;

  if (typeof obj.length !== 'number' 
      || typeof obj === 'function') {
    keys = Object.keys(obj);
  }

  len = (keys || obj).length;
  if (!len) return func && func();

  var next = function() {
    if (++c === len) func && func();
  };

  for (; i < len; i++) {
    key = keys ? keys[i] : i;
    looper(next, obj[key], key, i);
  }
};

// safe stringify, JSON throws
// if you pass in non-scalars
var stringify = function(val) {
  var type = typeof val;
  if (type === 'undefined' 
      || type === 'function' 
      || val !== val) {
    val = null;
  }
  return JSON.stringify(val);
};

// expose
module.exports = Tiny;