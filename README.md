# node-tiny

__node-tiny__ is an in-process document/object store for node.js.

It is largely inspired by [nStore](https://github.com/creationix/nstore), 
however, its goal was to implement real querying which goes easy on the memory. 
It stores data as flattened stringified objects. This allows queries to select specific 
byte-ranges from the file descriptor to lookup individual properties. The main 
benefit of this is that it doesn't cache properties larger than 1kb in size. This 
may make it more realistic to perhaps run a large weblog with, for example.

It supports mongo-style querying, or alternatively a mapreduce-like
interface similar to CouchDB's views (this isn't needless fluff by the way, 
the function is used internally). 

* * *

It's called node-tiny because I challenged myself to keep it to a single JS file
with no dependencies. I also wanted it to be simple: There are no schemas and 
nothing you need to know to get started, just store your data: a portable database 
that you can drag around in a single file.

## How Tiny works...

node-tiny takes advantage of the fact that, normally, when you query for 
records in a database, you're only comparing small properties (<1kb) in the 
query itself. For example, when you query for articles on a weblog, you'll 
usually only be comparing the timestamp of the article, the title, the author, 
the category, the tags, etc. - pretty much everything except the content of 
the article itself.

Tiny stores each document/object's property individually in the DB file and 
caches all the small properties into memory when the DB loads, leaving anything 
above 1kb behind. When a query is performed, Tiny only lets you compare the 
properties stored in memory, which is what you were going to do anyway. Once 
the query is complete, Tiny will perform lookups on the FD to grab the large 
properties and put them in their respective objects before results are returned 
to you.

This my attempt at combining what I think the best aspects of nStore and 
node-dirty are. node-dirty is incredibly fast and simple (everything is in-memory), 
and nStore is very memory effecient, (but this only lasts until you perform a 
query). node-tiny allows for queries that perform lookups on the db file, and it 
selectively caches properties as well, so its fast and easy on memory.

The benefits you receive from using node-tiny depend on the kind of data you're 
working with. With the blog example mentioned above, if you consider that the 
metadata for a blog post may be as little as 200 bytes, a __half-million__ articles 
would use less than 100mb of memory. 

## Example Querying

    var Tiny = require('./tiny');
    Tiny('articles.tiny', function(err, db) {
      var time = Date.now(),
          low = time - (60*60*1000), 
          high = time - (30*60*1000);
      
      // mongo-style query
      db.find({$or: [ 
        { timestamp: { $lte: low } }, 
        { timestamp: { $gte: high } }  
      ]}).desc('timestamp')
      .limit(3)(function(err, results) {
        console.log('Results:', results);
      });
      
      // is equivalent to:
      db.fetch({
        desc: 'timestamp',
        limit: 3
      }, function(doc, key) {
        if (doc.timestamp <= low || doc.timestamp >= high) {
          console.log('Found:', key); 
          return true;
        }
      }, function(err, results) {
        console.log('Results:', results);
      });
    });

The mongo-style querying should be fairly self-explanatory. The second query is supposed to be 
similar to a mapreduce interface, but it's the rough equivalent of a `.filter` function.

Note: there is a `shallow` parameter for `.fetch`, `.find`, and `.get`, wherein it will __only__ 
lookup properties that are under 1kb in size. This is to go easy on the memory. `.each` and 
`.all` are shallow by default, but they do have a `deep` parameter, (which I don't recommend using).

You can configure the limit at which properties are no longer cached by calling `Tiny.limit`, 
which accepts a number of bytes. e.g. `Tiny.limit(1024);`

## Other Usage

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

## Making data more efficient

Because of the way Tiny works, there are ways to alter your data to make it more 
memory efficient. For example, if you have several properties on your objects 
that aren't necessary to for queries, its best to nest them in an object.

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

That way, the data will not be cached if it exceeds 1kb collectively. Eventually there may be
an `ignore` method or an `index` method, which will be explicitly inclusive or exclusive to 
which properties are cached and which properties are able to be referenced within a query.

## License

See LICENSE (MIT).