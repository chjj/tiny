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

## A new direction taken with node-tiny

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

This my attempt at combining what I thought the best aspects of nStore and 
node-dirty are. node-dirty is incredibly fast and simple (everything is in-memory), 
and nStore is very memory effecient, (but this only lasts until you perform a 
query). node-tiny allows for queries that perform lookups on the db file, and it 
selectively caches properties as well, so its fast and easy on memory.

The benefits you receive from using node-tiny depend on the kind of data you're 
working with. With the blog example mentioned above, if you consider that the 
metadata for a blog post may be as little as 200 bytes, a __half-million__ articles 
would use less than 100mb of memory, and I don't know of any blog that actually 
has that many posts. You can configure the limit at which properties are no 
longer cached by calling `Tiny.limit`, which accepts a number of bytes. 
e.g. `Tiny.limit(1024);`

## Example Querying

    var Tiny = require('./tiny');
    Tiny('articles.tiny', function(err, db) {
      var TIME = Date.now();
      var low = TIME - (60*60*1000), high = TIME - (30*60*1000);
      
      // mongo-style query
      db.find({$or: [ 
        { timestamp: { $lte: low } }, 
        { timestamp: { $gte: high } }  
      ]}).desc('timestamp')
      .limit(3)(function(err, results) {
        console.log('RESULTS:', results);
      });
      
      // is equivalent to:
      db.fetch(function(doc, total) {
        if (total === 3) return;
        if (doc.timestamp <= low || doc.timestamp >= high) {
          console.log('found', doc._key); 
          return true;
        }
      }, function(err, results) {
        console.log('RESULTS:', Tiny.sort.desc(results, 'timestamp'));
      });
    });

The mongo-style querying should be fairly self-explanatory. The second query is supposed to be 
similar to a mapreduce interface, but it's the rough equivalent of a `.filter` function.

Note: there is a `shallow` parameter for `.fetch`, `.find`, and `.get`, wherein if you don't explicitly 
it will __only__ lookup properties that are under 1kb in size. This is to go easy on the memory. `.each` 
and `.all` are shallow by default, but they do have a `deep` parameter, (which I don't recommend using).

## Other Usage

    db.set('myDocument', {
      title: 'a document',
      content: 'hello world'
    }, function() {
      // .each is shallow by default
      db.each(function(doc) { 
        console.log(doc.title);
      });
      db.remove('myDocument'); // delete the object/doc
    });
    
    db.get('someOtherThing', function(err, data) {
      console.log(data._key);
    });
    
    // updates the object 
    // without overwriting its other properties
    db.update('article_1', { 
      title: 'new title'
    }, function(err) {
      console.log('done');
    });
    
    db.close(function() {
      console.log('db closed');
    });
    
    db.compact(function() {
      console.log('done');
    });

## License

See LICENSE (MIT).