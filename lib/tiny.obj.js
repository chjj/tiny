/**
 * tiny (https://github.com/chjj/tiny)
 * An embedded/in-process document/object store for node.
 * Copyright (c) 2011-2014, Christopher Jeffrey (MIT Licensed)
 *
 * Largely inspired by nStore.
 * No schemas, just store your objects.
 */

var fs = require('fs')
  , path = require('path')
  , EventEmitter = require('events').EventEmitter
  , Stream = require('stream').Stream;

/**
 * Tiny
 */

function Tiny(options, callback) {
  if (!(this instanceof Tiny)) {
    return Tiny.open(options, callback);
  }

  var options = options || {};

  if (typeof options === 'string') {
    options = { file: options };
  }

  EventEmitter.call(this);

  this.options = options;
  this.tokens = options.tokens || Tiny.tokens;
  this.cacheLimit = options.cacheLimit || Tiny.cacheLimit;
  this.cacheNumber = options.cacheNumber || Tiny.cacheNumber;
  this.keyUnderscore = options.keyUnderscore !== false;
  this.name = Tiny._getName(options);

  this._load(callback);
}

Tiny.prototype.__proto__ = EventEmitter.prototype;

Tiny.open = function(options, callback) {
  var options = options || {}
    , name = Tiny._getName(options);

  if (Tiny.db[name]) {
    if (callback) {
      callback(null, Tiny.db[name]);
    }
  } else {
    Tiny.db[name] = new Tiny(options, callback);
  }

  return Tiny.db[name];
};

Tiny._getName = function(options) {
  var options = options || {}
    , name = options.file || options.name || options;

  if (typeof name !== 'string') {
    name = path.resolve(process.env.HOME, 'node-tiny.db');
  }

  return name;
};

Tiny.db = {};

// The amount of bytes at which
// properties are no longer cached.
Tiny.cacheLimit = 128;
Tiny.cacheNumber = 1000;

// A token to recognize
// deleted properties
// and property separators
Tiny.tokens = {
  deleted: '\x00'
};

/**
 * Debug
 */

Tiny.debug = (function() {
  if (process.env.NODE_DEBUG) {
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
 * Garbage Collection
 */

Tiny.prototype._collectGarbage = function() {
  var self = this;
  this._collector = setInterval(function() {
    var keys = Object.keys(self._cache);
    if (keys.length >= self.cacheNumber) {
      var cache = {};
      keys.slice(-(self.cacheNumber / 2 | 0)).forEach(function(docKey) {
        cache[docKey] = self._cache[docKey];
      });
      self._cache = cache;
    }
  }, 2 * 60 * 1000);
  if (this._collector.unref) this._collector.unref();
};

Tiny.prototype._cacheable = function(size) {
  if (typeof size === 'number') {
    ;
  } else if (Buffer.isBuffer(size)) {
    size = size.length;
  } else if (typeof size === 'string') {
    size = Buffer.byteLength(size);
  } else {
    size = JSON.stringify(size).length;
  }
  return (size <= this.cacheLimit)
    && (Object.keys(this._cache).length < this.cacheNumber);
};

/**
 * Parsing and Loading
 */

Tiny.prototype._load = function(callback) {
  var self = this;

  callback = callback || noop;

  this.name;
  this._queue = [];
  this._busy = true;
  this._index = {};
  this._cache = {};
  this._total = 0;
  this._fd = null;
  this._loaded = false;
  this._queries = [];

  this.emit('opening');

  return fs.open(this.name, 'a+', function(err, fd) {
    if (err) return callback(err);
    self._fd = fd;
    return self._build(function(err) {
      if (err) return callback(err);

      Tiny.debug('Done parsing.');

      self._busy = false;
      self._loaded = true;

      self.emit('open');
      self.emit('ready');

      self.commit();
      self._flushQueries();
      self._collectGarbage();

      return callback(null, self);
    });
  });
};

Tiny.prototype._build = function(callback) {
  var self = this
    , index = {}
    , cache = {};

  callback = callback || noop;

  return this._parse(function(docKey, pos, length) {
    var previous = index[docKey];
    index[docKey] = [pos, length];
    index[docKey].previous = previous;
  }, function(err, total) {
    if (err) return callback(err);

    return parallel(index, function(val, next, docKey) {
      return self._ensureIntegrity(index, docKey, function(err, slot) {
        if (err) return next();

        if (self._cacheable(slot[1])) {
          return self._lookup(slot, function(err, data) {
            if (err) return next();
            cache[docKey] = data;
            return next();
          });
        }

        return next();
      });
    }, function() {
      self._index = index;
      self._cache = cache;
      self._total = total;
      return callback();
    });
  });
};

Tiny.prototype._parse = function(on, callback) {
  var self = this
    , fd = this._fd
    , data = new Buffer(64 * 1024);

  callback = callback || noop;

  var key = ''
    , state = 'key'
    , dstart = 0
    , pos = 0;

  function done(err) {
    return callback(err, pos);
  }

  (function read() {
    return fs.read(fd, data, 0, data.length, pos, function(err, bytes) {
      if (err || !bytes) {
        return done(err);
      }

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

      return read();
    });
  })();
};

Tiny.prototype._ensureIntegrity = function(index, docKey, callback) {
  var self = this
    , fd = this._fd
    , slot = index[docKey]
    , previous = slot.previous
    , pos = slot[0]
    , length = slot[1]
    , ch = new Buffer(1)
    , check
    , first
    , last;

  callback = callback || noop;

  function recheck() {
    if (!previous) {
      Tiny.debug('Non-recoverable: %s:%d', docKey, slot[0]);
      self.emit('error', new Error(docKey + ' corrupt. Non-recoverable.'));
      return callback(true);
    }
    Tiny.debug('Corrupt record found: %s:%d.', docKey, slot[0],
          'Using previous value.');
    self.emit('corrupt', docKey, previous);
    index[docKey] = previous;
    return self._ensureIntegrity(index, docKey, callback);
  }

  return fs.read(fd, ch, 0, 1, pos, function(err, bytes) {
    if (err || !bytes) {
      return recheck();
    }

    first = ch[0];

    return fs.read(fd, ch, 0, 1, pos + length - 1, function(err, bytes) {
      if (err || !bytes) {
        return recheck();
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

      return check
        ? callback(null, index[docKey])
        : recheck();
    });
  });
};

Tiny.prototype._flushQueries = function() {
  var queries = this._queries.slice();
  this._queries.length = 0;
  for (var i = 0; i < queries.length; i++) {
    queries[i][0].apply(this, queries[i][1]);
  }
};

/**
 * Object Lookup
 */

Tiny.prototype._read = function(pos, length, callback) {
  var self = this
    , fd = this._fd
    , data;

  callback = callback || noop;

  data = new Buffer(length);
  return fs.read(fd, data, 0, length, pos, function(err, bytes) {
    if (err) return callback(err);
    return callback(null, data.toString('utf8'));
  });
};

Tiny.prototype._lookup = function(slot, callback) {
  var self = this;
  return this._read(slot[0], slot[1], function(err, data) {
    if (err) return callback(err);

    try {
      data = JSON.parse(data);
    } catch(err) {
      return callback(err);
    }

    if (data === self.tokens.deleted) {
      return callback(new Error('Not found.'));
    }

    return callback(null, data);
  });
};

/**
 * Data Storage
 */

Tiny.prototype._set = function(docKey, data, callback, action) {
  var self = this
    , index = this._index
    , json;

  if (!this._loaded) {
    this._queries.push([this._set, slice.call(arguments)]);
    return;
  }

  callback = callback || noop;

  if (/[\x09\x0A]/.test(docKey)) {
    return callback(new Error('Bad key.'));
  }

  if (!data || typeof data !== 'object') {
    return callback(new Error('Bad object.'));
  }

  Tiny.debug('setting doc: %s', docKey);

  switch (action) {
    case 'set':
    case 'update':
      data._key = docKey;
      json = JSON.stringify(data);
      break;
    case 'delete':
      delete cache[docKey];
      delete index[docKey];
      json = JSON.stringify(this.tokens.deleted);
      break;
  }

  this._queue.push({
    action: action,
    docKey: docKey,
    data: data,
    json: json,
    callback: callback
  });

  return this.commit();
};

Tiny.prototype.put =
Tiny.prototype.set = function(docKey, doc, callback) {
  return this._set(docKey, doc, callback, 'set');
};

Tiny.prototype.merge =
Tiny.prototype.update = function(docKey, doc, callback) {
  callback = callback || noop;
  if (!this._loaded) {
    this._queries.push([this.update, slice.call(arguments)]);
    return;
  }
  if (!this._index[docKey]) {
    return callback(new Error('No such key.'));
  }
  return this._set(docKey, doc, callback, 'update');
};

Tiny.prototype.del =
Tiny.prototype.remove = function(docKey, callback) {
  callback = callback || noop;
  if (!this._loaded) {
    this._queries.push([this.remove, slice.call(arguments)]);
    return;
  }
  if (!this._index[docKey]) {
    return callback(new Error('No such key.'));
  }
  return this._set(docKey, {}, callback, 'delete');
};

/**
 * Commit Changes
 */

Tiny.prototype.commit = function(callback) {
  var self = this
    , fd = this._fd
    , queue = this._queue;

  if (!this._loaded) {
    this._queries.push([this.commit, slice.call(arguments)]);
    return;
  }

  callback = callback || noop;

  if (this._busy
      || !queue.length
      || fd == null) return;

  Tiny.debug('committing - %d items in queue.', queue.length);
  this.emit('committing', queue);

  var data = []
    , total = 0;

  this._busy = true;
  this._queue = [];

  queue = queue.map(function(item) {
    var docKey = item.docKey
      , json = item.json;

    var line = docKey + '\t' + json + '\n'
      , lineSize = Buffer.byteLength(line)
      , keySize = Buffer.byteLength(docKey + '\t');

    if (self._cacheable(lineSize)) {
      self._cache[docKey] = item.data;
    }

    if (!self._index[docKey]) {
      self._index[docKey] = [];
    }

    self._index[docKey][0] = self._total + total + (keySize - 1);
    self._index[docKey][1] = lineSize - keySize;

    total += lineSize;

    data.push(line);

    return item;
  });

  queue.push({
    action: 'commit',
    docKey: null,
    data: null,
    json: null,
    callback: callback
  });

  callback = function(err) {
    self._busy = false;
    queue.forEach(function(item) {
      self.emit(item.action, item.docKey, item.data);

      // levelup-like events
      switch (item.action) {
        case 'set':
        case 'update':
          self.emit('put', item.docKey, item.data);
          break;
        case 'delete':
          self.emit('del', item.docKey, item.data);
          break;
        case 'commit':
          if (queue.length > 1 && item.callback !== noop) {
            self.emit('batch', queue);
          }
          break;
      }

      if (!item.callback) {
        return;
      }

      return err
        ? item.callback(err)
        : item.callback(null);
    });
    return self.commit();
  };

  data = new Buffer(data.join(''));

  return fs.write(fd, data, 0, data.length, this._total, function on(err, bytes) {
    if (err) {
      if (err.code === 'EBADF') {
        return callback(err);
      }

      if (!on.attempt) {
        Tiny.debug('write error: %s', err);
        on.attempt = 0;
      }

      if (++on.attempt === 5) {
        err.message = 'Write Error:\n' + err.message;
        return callback(err);
      }

      self.emit('retry', on.attempt);

      return setTimeout(function() {
        return fs.write(fd, data, 0, data.length, self._total, on);
      }, 50);
    }

    self._total += bytes;

    return callback();
  });
};

/**
 * Data Retrieval
 */

Tiny.prototype.get = function(docKey, callback) {
  var self = this
    , index = this._index
    , slot = this._index[docKey]
    , cache = this._cache
    , cached = cache[docKey]
    , doc = {};

  if (!this._loaded) {
    this._queries.push([this.get, slice.call(arguments)]);
    return;
  }

  callback = callback || noop;

  if (cached) {
    return callback(null, cached);
  }

  if (!slot) {
    return callback(new Error('Not found.'));
  }

  return this._lookup(slot, function(err, doc) {
    if (!self.keyUnderscore && doc._key) {
      doc.key = doc._key;
    }
    self.emit('get', docKey, doc);
    return callback(null, doc);
  });
};

Tiny.prototype.all = function() {
  throw new
    Error('`db.all()` has been removed.'
          + ' It is not memory efficient.'
          + ' Please use something else.');
};

Tiny.prototype.each = function(iter, done) {
  var self = this
    , index = this._index
    , keys = Object.keys(index);

  if (!this._loaded) {
    this._queries.push([this.each, slice.call(arguments)]);
    return;
  }

  iter = iter || noop;
  done = done || noop;

  return parallel(keys, function(docKey, next) {
    return self.get(docKey, function(err, data) {
      if (err) return next();
      iter(data, docKey);
      return next();
    });
  }, done);
};

Tiny.prototype.createReadStream = function(options, stream) {
  var self = this
    , index = this._index
    , keys = Object.keys(index)
    , options = options || {}
    , stream = stream || new Stream
    , total = 0;

  stream.readable = true;
  stream.writable = false;

  stream.pause = function() {
    this._paused = true;
  };

  stream.resume = function() {
    this._paused = false;
  };

  stream.destroy = function() {
    this._destroyed = true;
    this.emit('close');
    this.emit('end');
  };

  if (!this._loaded) {
    this._queries.push([this.createReadStream, [options, stream]]);
    return stream;
  }

  if (options.reverse) {
    keys = keys.reverse();
  }

  serial(keys, function iter(docKey, next, i) {
    if (stream._destroyed) return;

    if (options.start && !stream._started) {
      if (docKey.indexOf(options.start) !== 0) {
        return next();
      }
      stream._started = true;
    }

    if (stream._paused) {
      return setTimeout(function() {
        return iter(docKey, next, i);
      }, 50);
    }

    if (options.keys) {
      if (options.limit && ++total > options.limit) {
        return stream.destroy();
      }
      stream.emit('data', docKey);
      if (options.end && docKey === options.end) {
        return stream.destroy();
      }
      return next();
    }

    return self.get(docKey, function(err, data) {
      if (err) {
        stream.emit('error', err);
        return next();
      }

      if (options.limit && ++total > options.limit) {
        return stream.destroy();
      }
      if (options.values) {
        stream.emit('data', data);
      } else {
        stream.emit('data', { key: docKey, value: data });
      }

      if (options.end && docKey.indexOf(options.end) === 0) {
        return stream.destroy();
      }

      return next();
    });
  }, function() {
    return stream.destroy();
  });

  return stream;
};

Tiny.prototype.createKeyStream = function(options) {
  options.keys = true;
  return this.createReadStream(options);
};

Tiny.prototype.createValueStream = function(options) {
  options.values = true;
  return this.createReadStream(options);
};

Tiny.prototype.createWriteStream = function(options) {
  var self = this
    , options = options || {}
    , stream = new Stream

  stream.readable = false;
  stream.writable = true;

  stream.write = function(data) {
    var type = data.type || 'set';
    switch (type) {
      case 'put':
      case 'set':
        type = 'set';
        break;
      case 'update':
      case 'merge':
        type = 'update';
        break;
      case 'delete':
      case 'del':
        type = 'delete';
        break;
      default:
        stream.emit('error', new Error('Unrecognized action: ' + type));
        return;
    }
    return self._set(data.docKey, data.value, function(err) {
      if (err) stream.emit('error', err);
    }, type);
  };

  stream.end = function(data) {
    var ret;
    if (data) {
      ret = stream.write(data);
    }
    this.destroy();
    return ret;
  };

  stream.destroy = function() {
    this._destroyed = true;
    this.emit('close');
    this.emit('end');
  };

  return stream;
};


/**
 * Querying
 */

Tiny.prototype.fetch = function(opt, filter, done) {
  var self = this
    , results = []
    , keys
    , cache = this._cache;

  if (!done) {
    done = filter;
    filter = opt;
    opt = {};
  }

  done = done || noop;

  if (!this._loaded) {
    this._queries.push([this.fetch, slice.call(arguments)]);
    return;
  }

  keys = Object.keys(this._index);

  if (opt.count) {
    results = 0;
    return parallel(keys, function iter(docKey, next) {
      if (!cache[docKey]) {
        return self.get(docKey, function(err, data) {
          if (err) return next();
          cache[docKey] = data;
          return iter(docKey, next);
        });
      }
      if (filter.length >= 3) {
        return filter(cache[docKey], docKey, function(match) {
          if (match) results++;
          return next();
        });
      }
      if (filter(cache[docKey], docKey) === true) {
        results++;
      }
      return next();
    });
    return done(null, results);
  }

  return parallel(keys, function iter(docKey, next) {
    if (!cache[docKey]) {
      return self.get(docKey, function(err, data) {
        if (err) return next();
        cache[docKey] = data;
        return iter(docKey, next);
      });
    }

    if (filter.length >= 3) {
      return filter(cache[docKey], docKey, function(match) {
        if (match) {
          return self.get(docKey, function(err, doc) {
            if (err) return next();
            results.push(doc);
            return next();
          });
        }
        return next();
      });
    }

    if (filter(cache[docKey], docKey) === true) {
      return self.get(docKey, function(err, doc) {
        if (err) return next();
        results.push(doc);
        return next();
      });
    }

    return next();
  }, function() {
    if (opt.desc || opt.asc) {
      results = self._sortResults(results,
        opt.asc || opt.desc,
        opt.asc ? 'asc' : 'desc');
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

    return done(null, results);
  });
};

Tiny.prototype._sortResults = function(results, prop, order) {
  var self = this
    , first = results[0]
    , numeric;

  if (first) {
    numeric = isFinite(first[prop]);
  }

  results = results
    .filter(function(result) {
      return result[prop] != null;
    })
    .sort(function(a, b) {
      a = a[prop];
      b = b[prop];
      if (!numeric) {
      // if (isFinite(a)) {
        a = (a + '').toLowerCase().charCodeAt(0);
        b = (b + '').toLowerCase().charCodeAt(0);
      }
      return a > b ? 1 : (a < b ? -1 : 0);
    });

  if (order === 'desc') {
    results = results.reverse();
  }

  return results;
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
  function test(obj, doc) {
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
  }

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
    return this.fetch(opt, function(doc) {
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
    return this.query.apply(this, args);
  }

  var chain = function(callback) {
    callback = callback || noop;
    return this.query.apply(this, args.concat(callback, opt));
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
  callback = callback || noop;
  this.emit('closing');
  return fs.close(this._fd, function() {
    delete self._fd;
    delete Tiny.db[self.name];
    self.emit('closed');
    return callback();
  });
};

Tiny.prototype.kill = function(callback) {
  var self = this;
  callback = callback || noop;
  return this.close(function() {
    return fs.unlink(self.name, function() {
      return self._load(callback);
    });
  });
};

Tiny.prototype.compact = function(callback) {
  var self = this
    , index = this._index
    , keys = Object.keys(index)
    , name = this.name + '~';

  if (!this._loaded) {
    this._queries.push([this.compact, slice.call(arguments)]);
    return;
  }

  callback = callback || noop;

  return Tiny(name, function(err, tmp) {
    if (err) return callback(err);
    return serial(keys, function(docKey, next) {
      return self.get(docKey, function(err, obj) {
        if (err) return callback(err);
        return tmp.set(docKey, obj, next);
      });
    }, function() {
      return self.close(function(err) {
        if (err) return callback(err);

        return fs.unlink(self.name, function(err) {
          if (err) return callback(err);

          return fs.rename(name, self.name, function(err) {
            if (err) return callback(err);

            Object.keys(tmp).forEach(function(key) {
              self[key] = tmp[key];
            });

            delete Tiny.db[name];
            return callback();
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
  if (arguments.length === 1) {
    callback = pretty;
    pretty = undefined;
  }

  callback = callback || noop;

  var self = this
    , index = this._index
    , keys = Object.keys(index)
    , klength = keys.length
    , pretty = pretty ? 2 : 0
    , i = 0;

  if (!this._loaded) {
    this._queries.push([this.dump, slice.call(arguments)]);
    return;
  }

  var stream = fs.createWriteStream(this.name + '.json');

  stream.on('error', function(err) {
    stream.destroy();
    return callback(err);
  });

  stream.on('open', function(fd) {
    stream.write('{\n');

    return serial(keys, function(docKey, next) {
      i++;
      return self.get(docKey, function(err, obj) {
        if (err) return callback(err);

        var data = '"'
          + docKey
          + '": '
          + JSON.stringify(obj, null, pretty);

        if (i !== klength) {
          data += ',\n';
        }

        if (stream.write(data) === false) {
          return stream.once('drain', next);
        }

        return next();
      });
    }, function() {
      stream.end('\n}');
      return stream.on('close', function() {
        callback(null, stream.path);
      });
    });
  });
};

/**
 * Getters
 */

Tiny.prototype.__defineGetter__('length', function() {
  return Object.keys(this._index).length;
});

Tiny.prototype.__defineGetter__('size', function() {
  return this._total || 0;
});

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

function parallel(obj, iter, done) {
  done = done || noop;

  if (!obj) return done();

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

  if (!l) return done();

  function next() {
    if (++j === l) {
      return done();
    }
  }

  for (; i < l; i++) {
    key = keys ? keys[i] : i;
    iter(obj[key], next, key, i);
  }
}

function serial(obj, iter, done) {
  done = done || noop;

  if (!obj) return done();

  var i = 0
    , keys
    , l;

  if (typeof obj.length !== 'number'
      || typeof obj === 'function') {
    keys = Object.keys(obj);
    l = keys.length;
    (function next() {
      if (i === l) return done();
      var j = i++, key = keys[j];
      return nextTick(function() {
        return iter(obj[key], next, key, j);
      });
    })();
  } else {
    l = obj.length;
    (function next() {
      if (i === l) return done();
      var j = i++;
      return nextTick(function() {
        return iter(obj[j], next, j, j);
      });
    })();
  }
}

function nextTick(callback) {
  return global.setImmediate
    ? global.setImmediate(callback)
    : process.nextTick(callback);
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

function noop() {}

/**
 * Expose
 */

module.exports = Tiny;
