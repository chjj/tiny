/**
 * node-tiny (https://github.com/chjj/node-tiny)
 * An embedded/in-process document/object store for node.
 * Copyright (c) 2011, Christopher Jeffrey (MIT Licensed) 
 * 
 * Largely inspired by nStore.
 * No schemas, just store your objects.
 */

var fs = require('fs')
  , path = require('path');

// debug messages?
var _DEBUG = !!process.env.TINY_DEBUG;

// the amount of bytes at which properties
// are no longer cached, 128b by default
var CACHE_LIMIT = 128;

// a token to recognize deleted properties
var DELETED = '\0';

var debug = _DEBUG
  ? function() {
    arguments[0] = 'Tiny: ' + arguments[0];
    return console.log.apply(console, arguments);
  }
  : function() {};

/**
 * Tiny
 */

var Tiny = function(name, func) {
  if (!(this instanceof Tiny)) {
    return Tiny.open(name, func);
  }
  this.name = name;
  this._load(func);
};

// this will cache database objects the same
// object will be returned every time
Tiny.open = function(name, func) {
  if (Tiny.db[name]) {
    func.call(Tiny.db[name], null, cache[name]);
  } else {
    Tiny.db[name] = new Tiny(name, func);
  }
  return Tiny.db[name];
};

Tiny.db = {};

// change the property cache limit
Tiny.limit = function(n) {
  CACHE_LIMIT = n;
  return this;
};

/**
 * Lookup
 */

var Lookup = function(index) {
  this[0] = index[0];
  this[1] = index[1];
  this._isLookup = Lookup;
};

Lookup.isLookup = function(obj) {
  return obj._isLookup === Lookup;
};

/**
 * Parsing and Loading
 */

// parse and load small properties into memory
Tiny.prototype._load = function(func) {
  var self = this;

  self._queue = [];
  self._busy = true;
  self._total = 0;
  self._cache = {};

  fs.open(self.name, 'a+', function(err, fd) {
    if (err) return func && func(err);
    self._fd = fd;
    self._build(function(err) {
      if (err) return func(err);

      debug('Done parsing.');
      debug('Loading small properties into memory.');

      self._busy = false;
      self.commit();

      func(null, self);
    });
  });
};

Tiny.prototype._build = function(func) {
  var self = this
    , index = {}
    , cache = {}
    , total
    , pending = 0;

  var done = function(err) {
    self._cache = cache;
    self._total = total;
    func(err);
  };

  self._parse(function(key, pos, length) {
    var previous = index[key];
    index[key] = [ pos, length ];

    pending++;

    self._lookup(index[key], function(err, data) {
      pending--;

      if (err) {
        if (err instanceof SyntaxError) {
          debug('Corrupt record found.', 
                'Using previous value.');
          index[key] = previous;
        } else {
          debug(err);
        }
        return !pending && done();
      }

      var pair = key.split('.')
        , docKey = pair[0]
        , propKey = pair[1];

      if (!cache[docKey]) cache[docKey] = {};
      if (length < CACHE_LIMIT) {
        cache[docKey][propKey] = data;
      } else {
        cache[docKey][propKey] = new Lookup(index[key]);
        data = undefined;
      }

      if (!pending) done();
    });
  }, function(err, pos) {
    total = pos;
    if (!pending) done(err);
  });
};

var FreeList = require('freelist').FreeList;
var buffers = new Freelist('data', 2, function() {
  return new Buffer(64 * 1024);
});

Tiny.prototype._parse = function(func, done) {
  var self = this
    , data = buffers.alloc();

  var key = ''
    , state = 'KEY'
    , start = 0
    , pos = 0
    , done_ = done;

  var done = function(err) {
    buffers.free(data);
    done_(err);
  };

  (function read() {
    fs.read(self._fd, data, 0, data.length, pos, function(err, bytes) {
      if (err || !bytes) {
        return done(err, pos);
      }

      for (var i = 0, k = 0; i < bytes; i++) {
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
              func(key, start, pos - start);
              key = '';
              k = i + 1;
            }
            break;
        }
        pos++;
      }

      if (state === 'KEY' && k < bytes) {
        key += data.slice(k).toString('ascii');
      }

      read();
    });
  })();
};

// curry on an error
Tiny.prototype._error = function(func, err, ret) {
  err = err || 'Not found.';
  debug('Error: %s', err + '');
  if (func) func.call(this, new Error(err), ret);
};

// commit the changes to the fd
Tiny.prototype.commit = function(func) {
  var self = this
    , cache = self._cache;

  if (self._busy 
      || !self._queue.length 
      || !self._fd) return;

  debug('committing - %d items in queue.', self._queue.length);

  var data = [];
  var queue = self._queue.map(function(item) {
    var docKey = item.docKey
      , doc = item.doc
      , cached = cache[docKey]
      , total = 0;

    Object.keys(item.doc).forEach(function(propKey) {
      var realKey = docKey + '.' + propKey
        , prop = realKey + '\t' + doc[propKey] + '\n'
        , propSize = Buffer.byteLength(prop)
        , realKeySize = Buffer.byteLength(realKey + '\t');

      if (propSize > CACHE_LIMIT) {
        cached[propKey] = new Lookup([
          self._total + total + (realKeySize - 1),
          propSize - realKeySize
        ]);
      }

      total += propSize;
      data.push(prop);
    });

    return item.func;
  });

  self._busy = true;
  self._queue = [];

  data = new Buffer(data.join(''));

  fs.write(self._fd, data, 0, data.length, self._total, function(err, bytes) {
    self._total += bytes;
    queue.forEach(function(func) {
      func.call(self, err);
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
    Object.keys(cache[docKey]).forEach(function(key) {
      // implictly set _key to DELETED for 'delete'
      // this assumes ._key is in the cached object
      if (!hasOwnProperty.call(_doc, key)) {
        doc[key] = stringify(DELETED);
      }
    });
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
      Object.keys(_doc).forEach(function(propKey) {
        cache[docKey][propKey] = _doc[propKey];
        doc[propKey] = stringify(_doc[propKey]);
      });

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

/**
 * Public API
 */

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
    , cached = self._cache[docKey];

  if (!cached) return self._error(func);

  // iterate over the properties and build the object
  // if there is a Lookup object, perform the lookup
  para(cached, function(loop, prop, propKey) {
    if (!shallow && Lookup.isLookup(cached[propKey])) {
      self._lookup(cached[propKey], function(err, data) {
        doc[propKey] = data;
        loop();
      });
    } else {
      doc[propKey] = cached[propKey];
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
  var opt = { 
    shallow: !deep 
  };
  this.fetch(opt, function() {
    return true;
  }, func);
};

// iterate through *every* document
Tiny.prototype.each = function(func, done, deep) {
  var self = this
    , cache = self._cache
    , keys = Object.keys(cache);

  para(keys, function(loop, docKey) {
    self.get(docKey, function(err, data) {
      func(data, docKey);
      loop();
    });
  }, done);
};

// close the FD
Tiny.prototype.close = function(func) {
  var self = this;
  fs.close(self._fd, function() {
    delete self._fd;
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
Tiny.prototype.compact = function(func) {
  var self = this
    , cache = self._cache
    , keys = Object.keys(cache)
    , name = self.name + '_';

  Tiny(name, function(err, tmp) {
    if (err) return func(err);
    para(keys, function(loop, docKey) {
      self.get(docKey, function(err, obj) {
        if (err) return func(err);
        tmp.set(docKey, obj, loop);
      });
    }, function() {
      self.close(function(err) {
        if (err) return func(err);
        fs.unlink(self.name, function(err) {
          if (err) return func(err);
          fs.rename(tmp.name, self.name, function(err) {
            if (err) return func(err);
            Object.keys(self).forEach(function(key) {
              self[key] = tmp[key];
            });
            delete Tiny.db[tmp.name];
            func();
          });
        });
      });
    });
  });
};

Tiny.prototype.compact_flat = function(func) {
  var self = this
    , cache = self._cache
    , keys = Object.keys(cache)
    , name = self.name + '_';

  var stream = fs.createWriteStream(name);

  stream.on('open', function(err) {
    if (err) return func(err);
    para(keys, function(loop, docKey) {
      self.get(docKey, function(err, obj) {
        if (err) return func(err);

        var k = Object.keys(obj)
          , i = 0
          , l = k.length
          , propKey
          , propVal;

        for (; i < l; i++) {
          propKey = k[i];
          propVal = obj[propKey];
          stream.write(docKey 
            + '.' 
            + propKey 
            + '\t' 
            + stringify(val) 
            + '\n');
        }
        stream.once('drain', loop);
      });
    }, function() {
      stream.end();
      stream.on('close', function(err) {
        if (err) return func(err);
        fs.unlink(self.name, function(err) {
          if (err) return func(err);
          fs.rename(name, self.name, function(err) {
            if (err) return func(err);
            self.name = name;
            self._load(func);
            func();
          });
        });
      });
    });
  });
};

// dump the entire database to a single JSON file
// the "pretty" parameter will pretty print the JSON
Tiny.prototype.dump = function(pretty, func) {
  var self = this
    , data = {};

  if (typeof pretty === 'function') {
    func = pretty;
    pretty = undefined;
  }

  var self = this
    , cache = self._cache
    , keys = Object.keys(cache)
    , name = self.name + '_'
    , pretty ? 2 : 0;

  var stream = fs.createWriteStream(self.name + '.json');

  stream.on('open', function(err) {
    if (err) return func(err);

    stream.write('{\n');

    para(keys, function(loop, docKey) {
      self.get(docKey, function(err, obj) {
        if (err) return func(err);
        stream.write(
          Array(pretty + 1).join(' ')  
          + '"' + docKey + '": ' 
          + JSON.stringify(obj, null, pretty)
          + ',\n'
        );
        stream.once('drain', loop);
      });
    }, function() {
      stream.end('\n}');
      stream.on('close', function() {
        func(null, stream.path);
      });
    });
  });
};

/**
 * Hypothetical Features
 */

Tiny.prototype.ignore = function() {
  if (!this._ignore) this._ignore = [];
  this._ignore = this._ignore.concat(slice.call(arguments));
};

// "only cache these properties"
Tiny.prototype.index = function() {
  if (!this._index) this._index = [];
  this._index = this._index.concat(slice.call(arguments));
};

/**
 * Querying
 */

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

    if (opt.skip) {
      results = results.slice(opt.skip);
    }

    if (opt.limit) {
      results = results.slice(0, opt.limit);
    }

    if (opt.one || opt.single) {
      results = results[0];
    }

    if (done) done.call(self, null, results);
  });
};

Tiny.prototype._sort = function(prop, order) {
  var self = this
    , cache = self._cache
    , keys = Object.keys(cache)
    , first = cache[keys[0]]
    , numeric = first && /^-?[\d.]+$/.test(first[prop]);

  keys = keys.filter(function(k) {
    return cache[k][prop] != null;
  }).sort(function(a, b) {
    a = cache[a][prop];
    b = cache[b][prop];
    if (!numeric) { 
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
      var keys = Object.keys(a)
        , i = 0
        , l = keys.length
        , val;

      for (; i < l; i++) {
        val = a[keys[i]];
        if (has(b, val)) return true;
      }

      return false;
    },
    // does not contain any of...
    $nin: function(a, b) { 
      return !ops.$in(a, b);
    },
    // contains all...
    $all: function(a, b) { 
      var found = 0
        , keys = Object.keys(a)
        , i = 0
        , l = keys.length
        , val;

      for (; i < l; i++) {
        val = a[keys[i]];
        if (has(b, val)) found++;
      }

      return found === Object.keys(b).length;
    },
    $exists: function(a, b) {
      return b 
        ? a !== undefined 
        : a === undefined;
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
      var i = obj.length
        , fails = 0;
      while (i--) {
        if (!test(obj[i], doc)) fails++;
      }
      return fails < obj.length;
    }

    var keys = Object.keys(obj)
      , i = 0
      , l = keys.length
      , propKey
      , targetProp
      , prop;

    for (; i < l; i++) {
      propKey = keys[i];
      targetProp = obj[propKey];
      prop = doc[propKey];

      if (propKey === '$or') {
        if (!test(targetProp, doc)) return false;
      } else if (targetProp && typeof targetProp === 'object') {
        if (!test.object(prop, targetProp, doc)) return false;
      } else {
        if (prop != targetProp) return false;
      }
    }

    return true;
  };

  test.object = function(prop, targetProp, doc) {
    var propOperations = targetProp;

    var keys = Object.keys(propOperations)
      , i = 0
      , l = keys.length
      , operator;

    for (; i < l; i++) { 
      operator = keys[i];
      targetProp = propOperations[operator];
      if (operator === '$or') {
        if (!test(targetProp, doc)) return false;
      } else if (ops[operator] && !ops[operator](prop, targetProp)) {
        return false;
      }
    }

    return true;
  };

  // the actual .query() function
  return function(where, func, opt) {
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

/**
 * Helpers
 */

var hasOwnProperty = Object.prototype.hasOwnProperty
  , slice = [].slice;

var has = function(obj, item) {
  var keys = Object.keys(obj)
    , i = 0
    , l = keys.length;

  for (; i < l; i++) {
    if (obj[keys[i]] === item) return true;
  }
};

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

module.exports = Tiny;
