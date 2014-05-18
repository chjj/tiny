# tiny

__tiny__ is an in-process document/object store for node.js.

It is largely inspired by [nStore](https://github.com/creationix/nstore),
however, its goal was to implement real querying which goes easy on the memory.

Tiny is very simple, there are no schemas, just store your objects. It supports
mongo-style querying, or alternatively a "mapreduce-like" interface similar to
CouchDB's views.

## Install

``` bash
$ npm install tiny
```

## How Tiny works...

Tiny takes advantage of the fact that, normally, when you query for records in
a database, you're only comparing small properties (<128b) in the query itself.
For example, when you query for articles on a weblog, you'll usually only be
comparing the timestamp of the article, the title, the author, the category,
the tags, etc. - pretty much everything except the content of the article
itself.

Tiny stores each document/object's property individually in the DB file and
caches all the small properties into memory when the DB loads, leaving anything
above 128b behind. When a query is performed, Tiny only lets you compare the
properties stored in memory, which is what you were going to do anyway. Once
the query is complete, Tiny will perform lookups on the FD to grab the large
properties and put them in their respective objects before results are returned
to you.

This my attempt at combining what I think the best aspects of nStore and
node-dirty are. node-dirty is incredibly fast and simple (everything is
in-memory), and nStore is very memory effecient, (but this only lasts until you
perform a query). node-tiny allows for queries that perform lookups on the db
file, and it selectively caches properties as well, so it's fast and easier on
memory.

## Example Querying

``` js
var Tiny = require('./tiny');
Tiny('articles.tiny', function(err, db) {
  var time = Date.now()
    , low = time - (60*60*1000)
    , high = time - (30*60*1000);

  // mongo-style query
  db.find({$or: [
    { timestamp: { $lte: low } },
    { timestamp: { $gte: high } }
  ]})
  .desc('timestamp')
  .limit(3)(function(err, results) {
    console.log('Results:', results);
  });

  // is equivalent to...
  db.fetch({
    desc: 'timestamp',
    limit: 3
  }, function(doc, key) {
    if (doc.timestamp <= low
        || doc.timestamp >= high) {
      console.log('Found:', key);
      return true;
    }
  }, function(err, results) {
    console.log('Results:', results);
  });
});
```

The mongo-style querying should be fairly self-explanatory. The second query is
supposed to be similar to a mapreduce interface, but it's the rough equivalent
of a `.filter` function.

Note: there is a `shallow` parameter for `.fetch`, `.find`, and `.get`, wherein
it will __only__ lookup properties that are under 128b in size. This is to go
easy on the memory. `.each` and `.all` are shallow by default, but they do have
a `deep` parameter, (which I don't recommend using).

## Other Usage

``` js
// save a document
db.set('myDocument', {
  title: 'a document',
  content: 'hello world'
}, function(err) {
  console.log('set!');
});

// .each will iterate through
// every object in the database
// it is shallow by default
db.each(function(doc) {
  console.log(doc.title);
});

// returns every object in the DB
// in an array, this is shallow
// by default
db.all(function(err, docs) {
  console.log(docs.length);
});

// remove a doc
db.remove('myDocument', function(err) {
  console.log('deleted');
});

// retrieve an object from the database
db.get('someOtherThing', function(err, data) {
  // data._key is a property which
  // holds the key of every object
  console.log('found:', data._key);
});

// updates the object
// without overwriting its other properties
db.update('article_1', {
  title: 'new title'
}, function(err) {
  console.log('done');
});

// close the file descriptor
db.close(function(err) {
  console.log('db closed');
});

// clean up the mess
db.compact(function(err) {
  console.log('done');
});

// dump the entire database to a JSON file
// in the same directory as the DB file
// (with an optional pretty-print parameter)
db.dump(true, function(err) {
  console.log('dump complete');
});
```

## Making data more memory efficient

Because of the way Tiny works, there are ways to alter your data to make it
more memory efficient. For example, if you have several properties on your
objects that aren't necessary to for queries, its best to nest them in an
object.

``` js
user: {
  name: 'joe',
  prop1: 'data',
  prop2: 'data',
  prop3: 'data'
}

user: {
  name: 'joe',
  data: {
    prop1: 'data',
    prop2: 'data',
    prop3: 'data'
  }
}
```

That way, the data will not be cached if it exceeds 128b collectively.
Eventually there may be an `ignore` method or an `index` method, which will be
explicitly inclusive or exclusive to which properties are cached and which
properties are able to be referenced within a query.

## Documentation

### Database
- [Construction](#construction)
- [dump](#dump)
- [close](#close)
- [kill](#kill)

### Querying
- [set](#set)
- [each](#each)
- [all](#all)

## Database

<a name="construction"></a>

### Tiny(name, callback)
Creates and returns a database with the given name.

__Arguments__

- name - filename to store and load the Tiny database
- callback(err, db) - Called after the database file is opened and loaded

__Example__

``` js
var db;
Tiny('./articles.tiny', function(err, db_) {
  if (err) throw err;
  db = db_;
  ...
});
```

---------------------------------------

<a name="dump"></a>

### dump(pretty, func) or dump(func)

Dumps the a database to a JSON file with the name as name.json. Pretty
specifies whether to indent each line with two spaces or not. Alternatively,
dump(func) can be called.

__Arguments__

- pretty - if true, the JSON file will be indented with two spaces
- func(err) - called after the dump is complete.

__Example__

``` js
db.dump(true, function(err) {
  console.log('dump complete');
});
```

---------------------------------------

<a name="close"></a>

### close(func)

Closes the Tiny database file handle. A new Tiny object must be made to reopen
the file.

__Arguments__

- func() - callback function after the database has been closed

__Example__

``` js
db.close(function(err) {
  console.log('db closed');
});
```

---------------------------------------

<a name="kill"></a>

### kill(func)

Closes the Tiny database file, deletes the file and all the data in the
database, and then creates a new database with the same name and file.

__Arguments__

- func() - callback function after the database has been reloaded

__Example__

``` js
db.kill(function(err) {
  console.log('db has been destroyed and a new db has been loaded');
});
```

## Querying

<a name="set"></a>

### set(docKey, doc, func)

Saves a object `doc` to database under the key `docKey`. Ideally, docKey should
be 128b or smaller.

__Arguments__

- docKey - a key to search the database for
- doc - an object to save to the database under the given key
- func - callback function after the doc object has been saved to the database

__Example__

``` js
db.set('myDocument', {
  title: 'a document',
  content: 'hello world'
}, function(err) {
  console.log('set!');
});
```

---------------------------------------

<a name="each"></a>

### each(func, deep) or each(func)

Iterates through every object in the database.

__Arguments__

- func(doc) - Callback function that is called with every iterated object `doc`
  from the database
- done() - Callback to be executed after the iterations complete.
- deep - `true` if every object should be returned, `false` or unset if only
  cacheable objects should be returned (ones smaller than 128b)

__Example__

``` js
db.each(function(doc) {
  console.log(doc.title);
}, function() {
  console.log('done');
});
```

### Contribution and License Agreement

If you contribute code to this project, you are implicitly allowing your code
to be distributed under the MIT license. You are also implicitly verifying that
all code is your original work. `</legalese>`

## License

Copyright (c) 2011-2014, Christopher Jeffrey. (MIT License)

See LICENSE for more info.
