/**
 * node-tiny (https://github.com/chjj/node-tiny)
 * An embedded/in-process document/object store for node.
 * Copyright (c) 2011, Christopher Jeffrey (MIT Licensed) 
 * 
 * Largely inspired by nStore.
 * No schemas, just store your objects.
 */

var fs = require('fs')
  , path = require('path')
  , FreeList = require('freelist').FreeList;

/**
 * Constant
 */

// the amount of bytes at which 
// properties are no longer 
// cached, 128b by default
var CACHE_LIMIT = 128;

// a token to recognize 
// deleted properties
var DELETED = '\0';

/**
 * Debug
 */

var debug = (function() {
  if (!!process.env.TINY_DEBUG) {
    return function() {
      arguments[0] = 'Tiny: ' + arguments[0];
      return console.log.apply(console, arguments);
    };
  } else {
    return function() {};
  }
})();

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

Tiny.open = function(name, func) {
  if (Tiny.db[name]) {
    func.call(Tiny.db[name], null, Tiny.db[name]);
  } else {
    Tiny.db[name] = new Tiny(name, func);
  }
  return Tiny.db[name];
};

Tiny.db = {};

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

Tiny.prototype._load = function(func) {
  var self = this;

  self._queue = [];
  self._busy = true;
  self._cache = {};
  self._total = 0;
  self._fd = null;

  fs.open(self.name, 'a+', function(err, fd) {
    if (err) return func && func.call(self, err);
    self._fd = fd;
    self._build(function(err) {
      if (err) return func(err);

      debug('Done parsing.');

      self._busy = false;
      self.commit();

      func.call(self, null, self);
    });
  });
};

Tiny.prototype._build = function(func) {
  var self = this
    , index = {}
    , cache = {};

  self._parse(function(key, pos, length) {
    var previous = index[key];
    index[key] = [ pos, length ];
    index[key].previous = previous;
  }, function(err, total) {
    if (err) return func.call(self, err);

    para(index, function(loop, slot, key) {
      var pos = slot[0]
        , length = slot[1]
        , previous = slot.previous;

      self._lookup(slot, function(err, data) {
        if (err) {
          if (err instanceof SyntaxError) {
            debug('Corrupt record found.', 
                  'Using previous value.');
            if (previous) {
              index[key] = previous;
            }
          } else {
            debug(err);
          }
          return loop();
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

        loop();
      });
    }, function() {
      self._cache = cache;
      self._total = total;
      func.call(self);
    });
  });
};

var pool = new FreeList('data', 2, function() {
  return new Buffer(64 * 1024);
});

Tiny.prototype._parse = function(func, done) {
  var self = this
    , data = pool.alloc();

  var key = ''
    , state = 'KEY'
    , start = 0
    , pos = 0
    , done_ = done;

  var done = function(err) {
    pool.free(data);
    done_(err, pos);
  };

  (function read() {
    fs.read(self._fd, data, 0, data.length, pos, function(err, bytes) {
      if (err || !bytes) {
        return done(err);
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

Tiny.prototype._ensureIntegrity = function(index, key, func) {
  var self = this
    , fd = self._fd
    , slot = index[key]
    , previous = slot.previous
    , pos = slot[0]
    , length = slot[1]
    , ch = new Buffer(1)
    , check
    , first
    , last;

  fs.read(fd, ch, 0, 1, pos, function(err, bytes) {
    if (err || !bytes) {
      return func.call(self, true);
    }

    first = ch[0];

    fs.read(fd, ch, 0, 1, pos + length - 1, function(err, bytes) {
      if (err || !bytes) {
        return func.call(self, true);
      }

      last = ch[0];

      switch (first) {
        case 0x22:
          check = last === 0x22;
          break;
        case 0x5B:
          check = last === 0x5D;
          break;
        case 0x7B:
          check = last === 0x7D;
          break;
        case 0x30:
        case 0x31:
        case 0x32:
        case 0x33:
        case 0x34:
        case 0x35:
        case 0x36:
        case 0x37:
        case 0x38:
        case 0x39:
          check === last >= 0x30 
                 && last <= 0x39;
          break;
        default:
          check = false;
          break;
      }

      if (check) {
        return func.call(self, null, index[key]);
      } else {
        if (!previous) {
          return func.call(self, true);
        }
        index[key] = previous;
        return self._ensureIntegrity(index, key, func);
      }
    });
  });
};

/**
 * Private API
 */

Tiny.prototype._error = function(func, err, ret) {
  err = err || 'Not found.';
  debug('Error: %s', err + '');
  if (func) func.call(this, new Error(err), ret);
};

Tiny.prototype._read = function(pos, length, func) {
  var self = this;
  if (!self._fd) {
    return process.nextTick(function() {
      self._read(pos, length, func);
    });
  }
  var data = new Buffer(length);
  fs.read(self._fd, data, 0, length, pos, function(err, bytes) {
    func.call(self, err, data.toString('utf8'));
  });
};

/**
 * Property Lookup
 */

Tiny.prototype._lookup = function(lookup, func) {
  var self = this;
  self._read(lookup[0], lookup[1], function(err, data) {
    if (err) return self._error(func, err);

    try {
      data = JSON.parse(data);
    } catch(err) {
      console.log(lookup);
      return self._error(func, err);
    }

    // make sure to ignore if its deleted
    if (data === DELETED) {
      return self._error(func);
    }

    if (func) func.call(self, null, data);
  });
};

/**
 * set/insert/save/update
 */

Tiny.prototype._set = function(docKey, _doc, func, flag) {
  var self = this
    , cache = self._cache
    , doc = {}; // stringified doc

  if (!_doc || typeof _doc !== 'object') {
    return self._error(func, 'Bad object.');
  }

  debug('setting doc: %s', docKey);

  // if there are any properties in the cache
  // that were excluded on the input object
  // need to explicitly mark them as deleted
  // on the stringified object
  if (flag !== 'update' && cache[docKey]) {
    // implictly set _key to DELETED for 'delete'
    // this assumes ._key is in the cached object
    Object.keys(cache[docKey]).forEach(function(key) {
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
 * Data Storage
 */

Tiny.prototype.set = function(docKey, doc, func) {
  return this._set(docKey, doc, func, 'set');
};

Tiny.prototype.update = function(docKey, doc, func) {
  if (!this._cache[docKey]) {
    return this._error(func, 'No such key.');
  }
  return this._set(docKey, doc, func, 'update');
};

Tiny.prototype.remove = function(docKey, func) {
  if (!this._cache[docKey]) {
    return this._error(func, 'No such key.');
  }
  return this._set(docKey, {}, func, 'delete');
};

/**
 * Commit Changes
 */

Tiny.prototype.commit = function(func) {
  var self = this;

  if (self._busy 
      || !self._queue.length 
      || !self._fd) return;

  debug('committing - %d items in queue.', self._queue.length);

  var cache = self._cache
    , data = []
    , total = 0;

  var queue = self._queue.map(function(item) {
    var docKey = item.docKey
      , doc = item.doc
      , cached = cache[docKey];

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

/**
 * Data Retrieval
 */

Tiny.prototype.get = function(docKey, func, shallow) {
  var self = this
    , doc = {}
    , cached = self._cache[docKey];

  if (!cached) return self._error(func);

  // iterate over the properties and build the object
  // if there is a Lookup object, perform the lookup
  para(cached, function(loop, prop, propKey) {
    if (Lookup.isLookup(prop)) {
      if (shallow) return loop();
      self._lookup(prop, function(err, data) {
        doc[propKey] = data;
        loop();
      });
    } else {
      doc[propKey] = prop;
      loop();
    }
  }, function() {
    if (func) func.call(self, null, doc);
  });
};

Tiny.prototype.all = function(func, deep) {
  this.fetch(
    {shallow:!deep}, 
    function() {
      return true;
    }, 
    func
  );
};

Tiny.prototype.each = function(func, done, deep) {
  var self = this
    , cache = self._cache
    , keys = Object.keys(cache);

  para(keys, function(loop, docKey) {
    self.get(docKey, function(err, data) {
      func.call(self, data, docKey);
      loop();
    }, !deep);
  }, done);
};

/**
 * Control
 */

Tiny.prototype.close = function(func) {
  var self = this;
  fs.close(self._fd, function() {
    delete self._fd;
    delete Tiny.db[self.name];
    if (func) func.call(self);
  });
};

Tiny.prototype.kill = function(func) {
  var self = this;
  self.close(function() {
    fs.unlink(self.name, function() {
      self._load();
      if (func) func.call(self);
    });
  });
};

Tiny.prototype.compact = function(func) {
  var self = this
    , cache = self._cache
    , keys = Object.keys(cache)
    , name = self.name + '_';

  Tiny(name, function(err, tmp) {
    if (err) return func.call(self, err);
    para(keys, function(loop, docKey) {
      self.get(docKey, function(err, obj) {
        if (err) return func.call(self, err);
        tmp.set(docKey, obj, loop);
      });
    }, function() {
      self.close(function(err) {
        if (err) return func.call(self, err);
        fs.unlink(self.name, function(err) {
          if (err) return func.call(self, err);
          fs.rename(name, self.name, function(err) {
            if (err) return func(err);
            Object.keys(tmp).forEach(function(key) {
              self[key] = tmp[key];
            });
            delete Tiny.db[name];
            func.call(self);
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

  stream.on('error', function(err) {
    stream.destroy();
    func.call(self, err);
  });

  stream.on('open', function(err) {
    if (err) return func.call(self, err);
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
          stream.write(
            docKey 
            + '.' 
            + propKey 
            + '\t' 
            + stringify(val) 
            + '\n'
          );
        }

        stream.once('drain', loop);
      });
    }, function() {
      stream.end();
      stream.on('close', function(err) {
        if (err) return func.call(self, err);
        fs.unlink(self.name, function(err) {
          if (err) return func(err);
          delete self._fd;
          fs.rename(name, self.name, function(err) {
            if (err) return func.call(self, err);
            self.name = name;
            self._load(func);
            func.call(self);
          });
        });
      });
    });
  });
};

/**
 * JSON Dump
 */

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
    , pretty = pretty ? 2 : 0;

  var stream = fs.createWriteStream(self.name + '.json');

  stream.on('error', function(err) {
    stream.destroy();
    func.call(self, err);
  });

  stream.on('open', function(err) {
    if (err) return func.call(self, err);

    stream.write('{\n');

    para(keys, function(loop, docKey) {
      self.get(docKey, function(err, obj) {
        if (err) return func.call(self, err);
        stream.write(
          '"' + docKey + '": ' 
          + JSON.stringify(obj, null, pretty)
          + ',\n'
        );
        stream.once('drain', loop);
      });
    }, function() {
      stream.end('\n}');
      stream.on('close', function() {
        func.call(self, null, stream.path);
      });
    });
  });
};

/**
 * Getters
 */

Tiny.prototype.__defineGetter__('length', function() {
  return Object.keys(this._cache).length;
});

Tiny.prototype.__defineGetter__('size', function() {
  return this._total || 0;
});

/**
 * Hypothetical Features
 */

Tiny.prototype.ignore = function() {
  if (!this._ignore) this._ignore = [];
  this._ignore = this._ignore.concat(slice.call(arguments));
};

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
    // type coercion
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

/**
 * Mongo-like Querying
 */

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
      var keys = Object.keys(b)
        , i = 0
        , l = keys.length
        , val;

      for (; i < l; i++) {
        val = b[keys[i]];
        if (has(a, val)) return true;
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
        , keys = Object.keys(b)
        , i = 0
        , l = keys.length
        , val;

      for (; i < l; i++) {
        val = b[keys[i]];
        if (has(a, val)) found++;
      }

      return found === l;
    },
    $exists: function(a, b) {
      return b 
        ? a !== undefined 
        : a === undefined;
    },
    $size: function(a, b) { 
      // why? because i can
      return Buffer.byteLength(a) === b;
    }
  };

  // test an object/statement to see
  // if it matches a document's properties.
  var test = function(obj, doc) {
    if (Array.isArray(obj)) {
      var i = obj.length;
      while (i--) {
        if (test(obj[i], doc)) return true;
      }
      return false;
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

var stringify = function(val) {
  var type = typeof val;
  if (type === 'undefined' 
      || type === 'function' 
      || val !== val) {
    val = null;
  }
  return JSON.stringify(val);
};

/**
 * Expose
 */

module.exports = Tiny;
