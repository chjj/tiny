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

/**
 * Tiny
 */

var Tiny = function(name, callback) {
  if (!(this instanceof Tiny)) {
    return Tiny.open(name, callback);
  }
  this.name = name;
  this._load(callback);
};

Tiny.open = function(name, callback) {
  if (Tiny.db[name]) {
    callback(null, Tiny.db[name]);
  } else {
    Tiny.db[name] = new Tiny(name, callback);
  }
  return Tiny.db[name];
};

Tiny.db = {};

// the amount of bytes at which
// properties are no longer cached
Tiny.cacheLimit = 128;

// a token to recognize
// deleted properties
// and property separators
Tiny.tokens = {
  deleted: '\0',
  delimeter: '\x03'
};

/**
 * Debug
 */

Tiny.debug = (function() {
  if (process.env.TINY_DEBUG) {
    return function() {
      var args = slice.call(arguments);
      args[0] = 'Tiny: ' + args[0];
      return console.log.apply(console, args);
    };
  } else {
    return function() {};
  }
})();

/**
 * Lookup
 */

function Lookup(index) {
  this[0] = index[0];
  this[1] = index[1];
  this._isLookup = Lookup;
}

Lookup.isLookup = function(obj) {
  return obj._isLookup === Lookup;
};

/**
 * Parsing and Loading
 */

Tiny.prototype._load = function(callback) {
  var self = this;

  self.name;
  self._queue = [];
  self._busy = true;
  self._cache = {};
  self._total = 0;
  self._fd = null;

  fs.open(self.name, 'a+', function(err, fd) {
    if (err) return callback && callback(err);
    self._fd = fd;
    self._build(function(err) {
      if (err) return callback && callback(err);

      Tiny.debug('Done parsing.');

      self._busy = false;
      self.commit();

      if (callback) callback(null, self);
    });
  });
};

Tiny.prototype._build = function(callback) {
  var self = this
    , index = {}
    , cache = {};

  self._parse(function(key, pos, length) {
    var previous = index[key];
    index[key] = [ pos, length ];
    index[key].previous = previous;
  }, function(err, total) {
    if (err) return callback(err);

    para(index, function(loop, val, key) {
      self._ensureIntegrity(index, key, function(err, slot) {
        if (err) return loop();

        var pair = key.split(Tiny.tokens.delimeter)
          , docKey = pair[0]
          , propKey = pair[1];

        if (!cache[docKey]) {
          cache[docKey] = {};
        }

        if (slot[1] < Tiny.cacheLimit) {
          self._lookup(slot, function(err, data) {
            if (err) return loop();
            cache[docKey][propKey] = data;
            loop();
          });
        } else {
          cache[docKey][propKey] = new Lookup(slot);
          loop();
        }
      });
    }, function() {
      self._cache = cache;
      self._total = total;
      callback();
    });
  });
};

Tiny.prototype._parse = function(on, callback) {
  var self = this
    , fd = self._fd
    , data = new Buffer(64 * 1024);

  var key = ''
    , state = 'key'
    , dstart = 0
    , pos = 0;

  var done = function(err) {
    callback(err, pos);
  };

  (function read() {
    fs.read(fd, data, 0, data.length, pos, function(err, bytes) {
      if (err || !bytes) return done(err);

      var kstart = 0
        , i = 0;

      for (; i < bytes; i++) {
        switch (state) {
          case 'key':
            switch (data[i]) {
              case 0x09:
                state = 'data';
                dstart = pos + 1;
                key += data.toString('ascii', kstart, i);
                break;
              case 0x0A:
                Tiny.debug('Unexpected byte at offset: %d', pos);
                kstart = i + 1;
                break;
            }
            break;
          case 'data':
            switch (data[i]) {
              case 0x0A:
                state = 'key';
                on(key, dstart, pos - dstart);
                key = '';
                kstart = i + 1;
                break;
              case 0x09:
                Tiny.debug('Unexpected byte at offset: %d', pos);
                dstart = pos + 1;
                break;
            }
            break;
        }
        pos++;
      }

      if (state === 'key' && kstart < bytes) {
        key += data.toString('ascii', kstart);
      }

      read();
    });
  })();
};

Tiny.prototype._ensureIntegrity = function(index, key, callback) {
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

  var recheck = function() {
    if (!previous) {
      Tiny.debug('Non-recoverable: %s:%d', key, slot[0]);
      return callback(true);
    }
    Tiny.debug('Corrupt record found: %s:%d.', key, slot[0],
          'Using previous value.');
    index[key] = previous;
    return self._ensureIntegrity(index, key, callback);
  };

  fs.read(fd, ch, 0, 1, pos, function(err, bytes) {
    if (err || !bytes) return recheck();

    first = ch[0];

    fs.read(fd, ch, 0, 1, pos + length - 1, function(err, bytes) {
      if (err || !bytes) return recheck();

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
        case 0x74:
        case 0x66:
          check = last === 0x65;
          break;
        case 0x6E:
          check = last === 0x6C;
          break;
        case 0x2D:
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
          check = last >= 0x30
               && last <= 0x39;
          break;
        default:
          check = false;
          break;
      }

      if (check) {
        return callback(null, index[key]);
      } else {
        return recheck();
      }
    });
  });
};

/**
 * Property Lookup
 */

Tiny.prototype._read = function(pos, length, callback) {
  var self = this
    , fd = self._fd
    , data;

  if (fd == null) {
    return process.nextTick(function() {
      self._read(pos, length, callback);
    });
  }

  data = new Buffer(length);
  fs.read(fd, data, 0, length, pos, function(err, bytes) {
    if (err) return callback(err);
    callback(null, data.toString('utf8'));
  });
};

Tiny.prototype._lookup = function(lookup, callback) {
  var self = this;
  self._read(lookup[0], lookup[1], function(err, data) {
    if (err) return callback(err);

    try {
      data = JSON.parse(data);
    } catch(err) {
      return callback(err);
    }

    if (data === Tiny.tokens.deleted) {
      return callback(new Error('Not found.'));
    }

    if (callback) callback(null, data);
  });
};

/**
 * Data Storage
 */

Tiny.prototype._set = function(docKey, data, callback, flag) {
  var self = this
    , cache = self._cache
    , doc = {};

  if (/[\x09\x0A]/.test(docKey) || ~docKey.indexOf(Tiny.tokens.delimeter)) {
    return callback(new Error('Bad key.'));
  }

  if (!data || typeof data !== 'object') {
    return callback(new Error('Bad object.'));
  }

  Tiny.debug('setting doc: %s', docKey);

  // if there are any properties in the cache
  // that were excluded on the input object
  // need to explicitly mark them as deleted
  // on the stringified object
  if (flag !== 'update' && cache[docKey]) {
    // implictly set _key to DELETED for 'delete'
    // this assumes ._key is in the cached object
    Object.keys(cache[docKey]).forEach(function(key) {
      if (!hasOwnProperty.call(data, key)) {
        doc[key] = stringify(Tiny.tokens.deleted);
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
      Object.keys(data).forEach(function(propKey) {
        cache[docKey][propKey] = data[propKey];
        doc[propKey] = stringify(data[propKey]);
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
    callback: callback
  });

  if (callback) self.commit();
};

Tiny.prototype.set = function(docKey, doc, callback) {
  return this._set(docKey, doc, callback, 'set');
};

Tiny.prototype.update = function(docKey, doc, callback) {
  if (!this._cache[docKey]) {
    return callback(new Error('No such key.'));
  }
  return this._set(docKey, doc, callback, 'update');
};

Tiny.prototype.remove = function(docKey, callback) {
  if (!this._cache[docKey]) {
    return callback(new Error('No such key.'));
  }
  return this._set(docKey, {}, callback, 'delete');
};

/**
 * Commit Changes
 */

Tiny.prototype.commit = function(callback) {
  var self = this
    , fd = self._fd
    , queue = self._queue;

  if (self._busy
      || !queue.length
      || fd == null) return;

  Tiny.debug('committing - %d items in queue.', queue.length);

  var cache = self._cache
    , data = []
    , total = 0;

  self._busy = true;
  self._queue = [];

  queue = queue.map(function(item) {
    var docKey = item.docKey
      , doc = item.doc
      , cached = cache[docKey];

    Object.keys(item.doc).forEach(function(propKey) {
      var realKey = docKey + Tiny.tokens.delimeter + propKey
        , prop = realKey + '\t' + doc[propKey] + '\n'
        , propSize = Buffer.byteLength(prop)
        , realKeySize = Buffer.byteLength(realKey + '\t');

      if (propSize > Tiny.cacheLimit) {
        cached[propKey] = new Lookup([
          self._total + total + (realKeySize - 1),
          propSize - realKeySize
        ]);
      }

      total += propSize;
      data.push(prop);
    });

    return item.callback;
  });

  if (callback) queue.push(callback);

  callback = function(err) {
    self._busy = false;
    queue.forEach(function(callback) {
      if (callback) callback(err);
    });
    self.commit();
  };

  data = new Buffer(data.join(''));

  fs.write(fd, data, 0, data.length, self._total, function on(err, bytes) {
    if (err) {
      if (err.code === 'EBADF') {
        return callback(err);
      }

      if (!on.attempt) {
        Tiny.debug('write error: %s', err);
        on.attempt = 0;
      }

      if (++on.attempt === 5) {
        err.message = 'Write Error:\n'
                      + err.message;
        return callback(err);
      }

      return setTimeout(function() {
        fs.write(fd, data, 0, data.length, self._total, on);
      }, 50);
    }

    self._total += bytes;

    callback();
  });
};

/**
 * Data Retrieval
 */

Tiny.prototype.get = function(docKey, callback, shallow) {
  var self = this
    , cache = self._cache
    , cached = cache[docKey]
    , doc = {};

  if (!cached) return callback(new Error('Not found.'));

  para(cached, function(loop, prop, propKey) {
    if (Lookup.isLookup(prop)) {
      if (shallow) return loop();
      self._lookup(prop, function(err, data) {
        if (err) return loop();
        doc[propKey] = data;
        loop();
      });
    } else {
      doc[propKey] = prop;
      loop();
    }
  }, function() {
    if (callback) callback(null, doc);
  });
};

Tiny.prototype.all = function() {
  throw new
    Error('`db.all()` has been removed.'
          + ' It is not memory efficient.'
          + ' Please use something else.');
};

Tiny.prototype.each = function(callback, done, deep) {
  var self = this
    , cache = self._cache
    , keys = Object.keys(cache);

  para(keys, function(loop, docKey) {
    self.get(docKey, function(err, data) {
      if (err) return loop();
      callback(data, docKey);
      loop();
    }, !deep);
  }, done);
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

  if (opt.desc || opt.asc) {
    keys = self._sort(
      opt.asc || opt.desc,
      opt.asc ? 'asc' : 'desc'
    );
  } else {
    keys = Object.keys(cache);
  }

  if (opt.count) {
    results = 0;
    keys.forEach(function(docKey) {
      if (filter(cache[docKey], docKey) === true) {
        results++;
      }
    });
    if (done) done(null, results);
    return;
  }

  para(keys, function(loop, docKey) {
    if (filter.length >= 3) {
      filter(cache[docKey], docKey, function(match) {
        if (match) {
          self.get(docKey, function(err, doc) {
            if (err) return loop();
            results.push(doc);
            loop();
          }, opt.shallow);
        } else {
          loop();
        }
      });
      return;
    }
    if (filter(cache[docKey], docKey) === true) {
      self.get(docKey, function(err, doc) {
        if (err) return loop();
        results.push(doc);
        loop();
      }, opt.shallow);
    } else {
      loop();
    }
  }, function() {
    if (opt.skip) {
      results = results.slice(opt.skip);
    }

    if (opt.limit) {
      results = results.slice(0, opt.limit);
    }

    if (opt.one || opt.single) {
      results = results[0];
    }

    if (done) done(null, results);
  });
};

Tiny.prototype._sort = function(prop, order) {
  var self = this
    , cache = self._cache
    , keys = Object.keys(cache)
    , first = cache[keys[0]]
    , numeric;

  if (first) {
    numeric = isFinite(first[prop]);
  }

  keys = keys
    .filter(function(k) {
      return cache[k][prop] != null;
    })
    .sort(function(a, b) {
      a = cache[a][prop];
      b = cache[b][prop];
      if (!numeric) {
      // if (isFinite(a)) {
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

  return function(where, callback, opt) {
    where = where || {};
    opt = opt || {};
    this.fetch(opt, function(doc) {
      if (test(where, doc)) {
        return true;
      }
    }, callback);
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

  var chain = function(callback) {
    return self.query.apply(self, args.concat(callback, opt));
  };

  chain.select = function() {
    throw new
      Error('`db.select()` has been removed.');
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
 * Control
 */

Tiny.prototype.close = function(callback) {
  var self = this;
  fs.close(self._fd, function() {
    delete self._fd;
    delete Tiny.db[self.name];
    if (callback) callback();
  });
};

Tiny.prototype.kill = function(callback) {
  var self = this;
  self.close(function() {
    fs.unlink(self.name, function() {
      self._load(callback);
    });
  });
};

Tiny.prototype.compact = function(callback) {
  var self = this
    , cache = self._cache
    , keys = Object.keys(cache)
    , name = self.name + '~';

  Tiny(name, function(err, tmp) {
    if (err) return callback && callback(err);
    serial(keys, function(loop, docKey) {
      self.get(docKey, function(err, obj) {
        if (err) return callback && callback(err);
        tmp.set(docKey, obj, loop);
      });
    }, function() {
      self.close(function(err) {
        if (err) return callback && callback(err);

        fs.unlink(self.name, function(err) {
          if (err) return callback && callback(err);

          fs.rename(name, self.name, function(err) {
            if (err) return callback && callback(err);

            Object.keys(tmp).forEach(function(key) {
              self[key] = tmp[key];
            });

            delete Tiny.db[name];
            if (callback) callback();
          });
        });
      });
    });
  });
};

/**
 * JSON Dump
 */

Tiny.prototype.dump = function(pretty, callback) {
  if (typeof pretty === 'function') {
    callback = pretty;
    pretty = undefined;
  }

  var self = this
    , cache = self._cache
    , keys = Object.keys(cache)
    , klength = keys.length
    , pretty = pretty ? 2 : 0
    , i = 0;

  var stream = fs.createWriteStream(self.name + '.json');

  stream.on('error', function(err) {
    stream.destroy();
    if (callback) callback(err);
  });

  stream.on('open', function(fd) {
    stream.write('{\n');

    serial(keys, function(loop, docKey) {
      i++;
      self.get(docKey, function(err, obj) {
        if (err) return callback && callback(err);

        var data = '"'
          + docKey
          + '": '
          + JSON.stringify(obj, null, pretty);

        if (i !== klength)
          data += ',\n';

        if (stream.write(data) === false) {
          stream.once('drain', loop);
        } else {
          loop();
        }
      });
    }, function() {
      stream.end('\n}');
      stream.on('close', function() {
        if (callback) callback(null, stream.path);
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
 * Helpers
 */

var hasOwnProperty = Object.prototype.hasOwnProperty
  , slice = [].slice;

function has(obj, item) {
  var keys = Object.keys(obj)
    , i = 0
    , l = keys.length;

  for (; i < l; i++) {
    if (obj[keys[i]] === item) return true;
  }
}

function para(obj, looper, callback) {
  if (!obj) return callback && callback();

  var j = 0
    , i = 0
    , l
    , keys
    , key;

  if (typeof obj.length !== 'number'
      || typeof obj === 'function') {
    keys = Object.keys(obj);
    l = keys.length;
  } else {
    l = obj.length;
  }

  if (!l) return callback && callback();

  var next = function() {
    if (++j === l) callback && callback();
  };

  for (; i < l; i++) {
    key = keys ? keys[i] : i;
    looper(next, obj[key], key, i);
  }
}

function serial(obj, looper, callback) {
  if (!obj) return callback && callback();

  var i = 0
    , keys
    , l;

  if (typeof obj.length !== 'number'
      || typeof obj === 'function') {
    keys = Object.keys(obj);
    l = keys.length;
    (function next() {
      if (i === l) return callback && callback();
      var j = i++, key = keys[j];
      looper(next, obj[key], key, j);
    })();
  } else {
    l = obj.length;
    (function next() {
      if (i === l) return callback && callback();
      var j = i++;
      looper(next, obj[j], j, j);
    })();
  }
}

function stringify(val) {
  var type = typeof val;
  if (type === 'undefined'
      || type === 'function'
      || val !== val) {
    val = null;
  }
  return JSON.stringify(val);
}

/**
 * Expose
 */

module.exports = Tiny;
