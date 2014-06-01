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
  , Stream = require('stream').Stream
  , util = require('util');

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
  this.cacheSize = options.cacheSize || Tiny.cacheSize;
  this.keyUnderscore = options.keyUnderscore !== false;
  //this.initialCache = options.initialCache;
  this.saveIndex = options.saveIndex;

  this.name = Tiny._getName(options);
  this._queue = [];
  this._busy = false;
  this._index = {};
  this._cache = {};
  this._total = 0;
  this._fd = null;
  this._loaded = false;
  this._queries = [];

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
Tiny.cacheSize = 1024;

// A token to recognize
// deleted properties
// and property separators
Tiny.tokens = {
  deleted: '\x00',
  delimiter: '\x01'
};

/**
 * Logging
 */

Tiny.print = function(msg) {
  var args = slice.call(arguments);
  if (typeof args[0] === 'object') {
    return process.stdout.write(inspect(args[0]) + '\n')
  }
  args[0] = 'Tiny: ' + args[0];
  return console.log.apply(console, args);
};

Tiny.prototype.print = Tiny.print;

Tiny.error = function() {
  var args = slice.call(arguments);
  if (typeof args[0] === 'object') {
    return process.stdout.write(inspect(args[0]) + '\n')
  }
  args[0] = 'Tiny: \x1b[41m' + args[0] + '\x1b[m';
  return console.error.apply(console, args);
};

Tiny.prototype.error = Tiny.error;

Tiny.debug = process.env.NODE_ENV === 'debug'
  ? Tiny.print
  : noop;

Tiny.prototype.debug = Tiny.debug;

/**
 * Lookup
 */

function Lookup(index) {
  this[0] = index[0];
  this[1] = index[1];
  this._isLookup = Lookup;
}

Lookup.isLookup = function(obj) {
  return obj && obj._isLookup === Lookup;
};

/**
 * Parsing and Loading
 */

Tiny.prototype._load = function(callback) {
  var self = this;

  callback = callback || noop;

  this.emit('opening');

  this._busy = true;
  this._loaded = false;

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

      return callback(null, self);
    });
  });
};

Tiny.prototype._build = function(callback) {
  var self = this;

  callback = callback || noop;

  function buildIndex(callback, build, data) {
    var idx = self.name + '.idx'
      , data;

    if (data) {
      self._total = data[0];
      self._index = data[1];
    }

    if (self.saveIndex && !build && !data) {
      Tiny.debug('Reading index...');
      return fs.stat(idx, function(err, stat) {
        if (err || stat.size <= 5) {
          Tiny.debug('Index is failing to read. Rebuilding...');
          return buildIndex(callback, true);
        }
        return fs.readFile(idx, function(err, buff) {
          if (err) {
            Tiny.debug('Index is failing to read. Rebuilding...');
            return buildIndex(callback, true);
          }

          return fs.stat(self.name, function(err, stat) {
            if (err) {
              Tiny.debug('DB is failing to read. Returning...');
              return calbback(new Error('Cannot stat() DB file.'));
            }

            try {
              data = JSON.parse(buff.toString('utf8'));
            } catch (e) {
              Tiny.debug('Index is failing to parse. Rebuilding...');
              return buildIndex(callback, true);
            }

            self._total = data[0];
            self._index = data[1];

            if (stat.size > self._total) {
              Tiny.debug('Index is too small. Building where we left off...');
              return buildIndex(callback, true, data);
            }

            return callback();
          });
        });
      });
    }

    Tiny.debug('Building index...');

    /*
    return self._parseSync(self._total, function(err, total) {
      if (err) return callback(err);

      self._total = total;

      if (!self.saveIndex) {
        return callback();
      }

      Tiny.debug('Writing index...');
      return fs.writeFile(idx, JSON.stringify([self._total, self._index]), function() {
        return callback();
      });
    });
    */

    return self._parse(self._total, function(realKey, pos, length) {
      //var previous = self._index[realKey];
      self._index[realKey] = [pos, length];
      //self._index[realKey].previous = previous;
    }, function(err, total) {
      if (err) return callback(err);

      self._total = total;

      if (!self.saveIndex) {
        return callback();
      }

      Tiny.debug('Writing index...');
      return fs.writeFile(idx, JSON.stringify([self._total, self._index]), function() {
        return callback();
      });
    });
  }

  function buildCache(callback) {
    //if (!self.initialCache) return callback();
    return parallel(self._index, function(val, next, realKey) {
      return self._ensureIntegrity(realKey, function(err, slot) {
        if (err) return next();

        var pair = realKey.split(self.tokens.delimiter)
          , docKey = pair[0]
          , propKey = pair[1];

        if (!self._cache[docKey]) {
          self._cache[docKey] = {};
        }

        if (slot[1] < self.cacheLimit) {
          return self._lookup(slot, function(err, data) {
            if (err) return next();
            self._cache[docKey][propKey] = data;
            return next();
          });
        }

        self._cache[docKey][propKey] = new Lookup(slot);

        return next();
      });
    }, function() {
      return callback();
    });
  }

  if (self.saveIndex) {
    process.on('exit', function() {
      Tiny.debug('Writing index...');
      return fs.writeFileSync(idx, JSON.stringify([self._total, self._index]));
    });
  }

  return buildIndex(function(err) {
    if (err) return callback(err);
    return buildCache(function(err) {
      if (err) return callback(err);
      //Object.keys(self._index).forEach(function(key) {
      //  delete self._index[key].previous;
      //});
      return callback();
    });
  });
};

Tiny.prototype._parse = function(start, on, callback) {
  var self = this
    , fd = this._fd
    , data = new Buffer(64 * 1024);

  callback = callback || noop;

  var key = ''
    , state = 'key'
    , dstart = 0
    , pos = start;

  function done(err) {
    return callback(err, pos);
  }

  // To make sure we start *on* a key.
  if (start) pos = start - 1;

  (function read() {
    return fs.read(fd, data, 0, data.length, pos, function(err, bytes) {
      if (err || !bytes) {
        return done(err);
      }

      var kstart = 0
        , i = 0;

      // If we don't start on a key, ignore
      // everything until the next field.
      if (start && pos === start - 1) {
        if (data[0] !== 0x0A) {
          state = 'unknown';
        }
        pos += 1;
        i += 1;
      }

      for (; i < bytes; i++) {
        switch (state) {
          case 'unknown':
            if (data[0] === 0x0A) {
              state = 'key';
              key = '';
              kstart = i + 1;
            }
            break;
          case 'key':
            switch (data[i]) {
              case 0x09:
                state = 'data';
                dstart = pos + 1;
                key += data.toString('ascii', kstart, i);
                break;
              case 0x0A:
                Tiny.debug('Unexpected byte at offset: %d', pos);
                key = '';
                kstart = i + 1;
                break;
            }
            break;
          case 'data':
            switch (data[i]) {
              case 0x0A:
                state = 'key';
                if (key) {
                  on(key, dstart, pos - dstart);
                }
                key = '';
                kstart = i + 1;
                break;
              case 0x09:
                Tiny.debug('Unexpected byte at offset: %d', pos);
                key = '';
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

Tiny.prototype._parseSync = function(start, callback) {
  var self = this
    , fd = this._fd
    , data = new Buffer(64 * 1024)
    , key = ''
    , state = 'key'
    , dstart = 0
    , pos = start || 0
    , err
    , bytes
    , kstart
    , i;

  // XXX Maybe use self._total as pos.
  // self._total = start || 0;

  // To make sure we start *on* a key.
  if (start) pos = start - 1;

  while (bytes) {
    try {
      bytes = fs.readSync(fd, data, 0, data.length, pos);
    } catch (e) {
      err = e;
      break;
    }

    kstart = 0;
    i = 0;

    // If we don't start on a key, ignore
    // everything until the next field.
    if (start && pos === start - 1) {
      if (data[0] !== 0x0A) {
        state = 'unknown';
      }
      pos += 1;
      i += 1;
    }

    for (; i < bytes; i++) {
      switch (state) {
        case 'unknown':
          if (data[0] === 0x0A) {
            state = 'key';
            key = '';
            kstart = i + 1;
          }
          break;
        case 'key':
          switch (data[i]) {
            case 0x09:
              state = 'data';
              dstart = pos + 1;
              key += data.toString('ascii', kstart, i);
              break;
            case 0x0A:
              Tiny.debug('Unexpected byte at offset: %d', pos);
              key = '';
              kstart = i + 1;
              break;
          }
          break;
        case 'data':
          switch (data[i]) {
            case 0x0A:
              state = 'key';
              if (key) {
                this._index[key] = [dstart, pos - dstart];
              }
              key = '';
              kstart = i + 1;
              break;
            case 0x09:
              Tiny.debug('Unexpected byte at offset: %d', pos);
              key = '';
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
  }

  if (callback) {
    return err
      ? callback(err)
      : callback(null, pos);
  }

  if (err) throw err;

  return pos;
};

Tiny.prototype._ensureIntegrity = function(key, callback) {
  var self = this
    , fd = this._fd
    , slot = this._index[key]
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
      Tiny.debug('Non-recoverable: %s:%d', key, slot[0]);
      self.emit('error', new Error(key + ' corrupt. Non-recoverable.'));
      return callback(true);
    }
    Tiny.debug('Corrupt record found: %s:%d.', key, slot[0],
          'Using previous value.');
    self.emit('corrupt', key, previous);
    self._index[key] = previous;
    return self._ensureIntegrity(key, callback);
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
        ? callback(null, self._index[key])
        : recheck();
    });
  });
};

Tiny.prototype._ensureIntegritySync = function(keu, callback) {
  var self = this
    , fd = this._fd
    , slot = this._index[keu]
    , previous = slot.previous
    , pos = slot[0]
    , length = slot[1]
    , ch = new Buffer(1)
    , check
    , first
    , last
    , bytes
    , err;

  function recheck() {
    err = null;
    if (!previous) {
      Tiny.debug('Non-recoverable: %s:%d', keu, slot[0]);
      self.emit('error', new Error(keu + ' corrupt. Non-recoverable.'));
      return callback(true);
    }
    Tiny.debug('Corrupt record found: %s:%d.', keu, slot[0],
          'Using previous value.');
    self.emit('corrupt', keu, previous);
    self._index[keu] = previous;
    return self._ensureIntegrity(keu, callback);
  }

  try {
    bytes = fs.readSync(fd, ch, 0, 1, pos);
  } catch (e) {
    err = e;
  }

  if (err || !bytes) {
    return recheck();
  }

  first = ch[0];

  try {
    bytes = fs.readSync(fd, ch, 0, 1, pos + length - 1);
  } catch (e) {
    err = e;
  }

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

  if (callback) {
    return check
      ? callback(null, self._index[keu])
      : recheck();
  }

  return check
    ? self._index[keu]
    : recheck();
};

Tiny.prototype._flushQueries = function() {
  var queries = this._queries.slice();
  this._queries.length = 0;
  for (var i = 0; i < queries.length; i++) {
    queries[i][0].apply(this, queries[i][1]);
  }
};

/**
 * Property Lookup
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

Tiny.prototype._lookup = function(lookup, callback) {
  var self = this;
  return this._read(lookup[0], lookup[1], function(err, data) {
    if (err) return callback(err);

    try {
      data = JSON.parse(data);
    } catch(err) {
      return callback(new Error(err.message + '\n\nData: ' + data));
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
    , cache = this._cache
    , doc = {};

  if (!this._loaded) {
    this._queries.push([this._set, slice.call(arguments)]);
    return;
  }

  callback = callback || noop;

  if (/[\x09\x0A]/.test(docKey) || ~docKey.indexOf(this.tokens.delimiter)) {
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
  if (action !== 'update' && cache[docKey]) {
    // implictly set _key to DELETED for 'delete'
    // this assumes ._key is in the cached object
    Object.keys(cache[docKey]).forEach(function(key) {
      if (!hasOwnProperty.call(data, key)) {
        doc[key] = stringify(self.tokens.deleted);
      }
    });
  }

  // this works in such a way that it will not
  // alter the original object at all.
  // we *could* just have the cache be a reference
  // to the original object, but if the
  // user is unaware of this, it might produce
  // unexpected results.
  switch (action) {
    case 'set':
    case 'update':
      if (action === 'set' || !cache[docKey]) {
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
      Object.keys(this._index).forEach(function(key) {
        var parts = key.split(self.tokens.delimiter);
        if (parts[0] === docKey) delete self._index[key];
      });
      delete cache[docKey];
      break;
  }

  this._queue.push({
    action: action,
    docKey: docKey,
    doc: doc,
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
  if (!this._cache[docKey]) {
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

  var cache = this._cache
    , data = []
    , total = 0;

  this._busy = true;
  this._queue = [];

  queue = queue.map(function(item) {
    var docKey = item.docKey
      , doc = item.doc
      , cached = cache[docKey];

    Object.keys(item.doc).forEach(function(propKey) {
      var realKey = docKey + self.tokens.delimiter + propKey
        , prop = realKey + '\t' + doc[propKey] + '\n'
        , propSize = Buffer.byteLength(prop)
        , realKeySize = Buffer.byteLength(realKey + '\t');

      if (propSize > self.cacheLimit) {
        cached[propKey] = new Lookup([
          self._total + total + (realKeySize - 1),
          propSize - realKeySize
        ]);
      }

      if (!self._index[realKey]) {
        self._index[realKey] = [];
      }

      self._index[realKey][0] = self._total + total + (realKeySize - 1);
      self._index[realKey][1] = propSize - realKeySize;

      total += propSize;
      data.push(prop);
    });

    return item;
  });

  queue.push({
    action: 'commit',
    docKey: null,
    doc: null,
    callback: callback
  });

  callback = function(err) {
    self._busy = false;
    queue.forEach(function(item) {
      self.emit(item.action, item.docKey, item.doc);

      // levelup-like events
      switch (item.action) {
        case 'set':
        case 'update':
          self.emit('put', item.docKey, item.doc);
          break;
        case 'delete':
          self.emit('del', item.docKey, item.doc);
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
        err.message = 'Write Error:\n'
                      + err.message;
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

Tiny.prototype.get = function(docKey, callback, shallow) {
  var self = this
    , cache = this._cache
    , cached = cache[docKey]
    , doc = {};

  if (!this._loaded) {
    this._queries.push([this.get, slice.call(arguments)]);
    return;
  }

  callback = callback || noop;

  if (!cached) {
    return callback(new Error('Not found.'));
  }

  return parallel(cached, function(prop, next, propKey) {
    if (Lookup.isLookup(prop)) {
      if (shallow) return next();
      return self._lookup(prop, function(err, data) {
        if (err) return next();
        doc[propKey] = data;
        return next();
      });
    }
    doc[propKey] = prop;
    return next();
  }, function() {
    if (!self.keyUnderscore && doc._key) {
      doc.key = doc._key;
    }
    self.emit('get', docKey, doc);
    //if (self._cacheable(slot[1])) {
    //  self._cache[docKey] = doc;
    //}
    return callback(null, doc);
  });
};

Tiny.prototype.all = function() {
  throw new
    Error('`db.all()` has been removed.'
          + ' It is not memory efficient.'
          + ' Please use something else.');
};

Tiny.prototype.each = function(iter, done, deep) {
  var self = this
    , cache = this._cache
    , keys = Object.keys(cache);

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
    }, !deep);
  }, done);
};

Tiny.prototype.createReadStream = function(options, stream) {
  var self = this
    , cache = this._cache
    , keys = Object.keys(cache)
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
    var val = cache[docKey];

    if (stream._destroyed) return;

    if (options.start && docKey.indexOf(options.start) !== 0) {
      return next();
    }

    if (stream._paused) {
      return setTimeout(function() {
        return iter(docKey, next, i);
      }, 50);
    }

    if (options.keys || options.values === false) {
      if (options.limit && ++total > options.limit) {
        return stream.destroy();
      }
      stream.emit('data', docKey);
      if (options.end && docKey.indexOf(options.end) === 0) {
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

      if (options.values || options.keys === false) {
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

  if (opt.desc || opt.asc) {
    keys = this._sort(
      opt.asc || opt.desc,
      opt.asc ? 'asc' : 'desc'
    );
  } else {
    keys = Object.keys(cache);
  }

  if (opt.count) {
    results = 0;
    return parallel(keys, function(docKey, next) {
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

  return parallel(keys, function(docKey, next) {
    if (filter.length >= 3) {
      return filter(cache[docKey], docKey, function(match) {
        if (match) {
          return self.get(docKey, function(err, doc) {
            if (err) return next();
            results.push(doc);
            return next();
          }, opt.shallow);
        }
        return next();
      });
    }

    if (filter(cache[docKey], docKey) === true) {
      return self.get(docKey, function(err, doc) {
        if (err) return next();
        results.push(doc);
        return next();
      }, opt.shallow);
    }

    return next();
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

    return done(null, results);
  });
};

Tiny.prototype._sort = function(prop, order) {
  var self = this
    , cache = this._cache
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
  callback = callback || noop;
  this.emit('closing');
  return fs.close(this._fd, function() {
    delete self._fd;
    delete Tiny.db[self.name];
    self.emit('closed');
    return callback();
  });
};

Tiny.prototype.compact = function(callback) {
  var self = this
    , cache = this._cache
    , keys = Object.keys(cache)
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
    , keys = Object.keys(this._cache)
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
  return Object.keys(this._cache).length;
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

function inspect(obj) {
  return typeof obj !== 'string'
    ? util.inspect(obj, false, 20, true)
    : obj;
}

/**
 * Expose
 */

exports = Tiny;
exports.json = require('./tiny.json.js');

module.exports = Tiny;
