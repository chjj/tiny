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

## Note:

__This is experimental.__ I can't speak for how durable this is or isn't.
There may be bugs, memory leaks, etc. I'm not able to test this thoroughly by
myself. I was on the verge of abandoning this project just recently, which is why 
I uploaded it. I would love to see people trying this out for small side projects, 
forking, giving feedback, etc. The goal here was efficiency and low memory usage.
I wanted to be able to realistically use an in-process database that I could run 
a blog with. Unfortunately, I don't think I'm able to fully judge how 
durable it is by myself.

I apologize for the code, it's sparsely commented, and a lot of the comments are 
notes to myself. The tests I've written are arbitrary and messy. Once again, this 
isn't ready for production use. I don't even know if it was ready for an upload 
(I was almost about to just gist it), it is just an experiment I have been working on. 
It still has some maturing to do. The code is slightly messy, there are bugs, etc. 
It may be very unreliable.

## Example Querying

    var Tiny = require('./tiny');
    Tiny('articles.tiny', function(err, db) {
      var TIME = Date.now();
      var low = TIME - (60*60*1000), high = TIME - (30*60*1000);
      
      // mongo-style query
      db.find({$or: [ 
        { timestamp: { $lte: low } }, 
        { timestamp: { $gte: high } }  
      ] }).desc('timestamp').select('title', 'timestamp').limit(3)(function(err, results) {
        console.log('RESULTS:', results);
      });
      
      // is equivalent to:
      db.fetch(function(doc, total) {
        if (total === 3) return 'break';
        if (doc.timestamp <= low || doc.timestamp >= high) {
          console.log('found', doc._key); // doc._key is always available no matter what the context
          return ['title', 'timestamp'];
        }
      }, function(err, results) {
        console.log('RESULTS:', Tiny.sort.desc(results, 'timestamp'));
      });
    });

The mongo-style querying should be fairly self-explanatory.

The first argument of the `.fetch()` method takes an array of property names. These 
are the names of the properties that are __relevant to the comparisons you are going to 
make__ in the "map" function below. The second parameter is just like a CouchDB map function,
except theres no `emit()`. You simply return an array of property names you want to select for
that document. If `true` is returned, all properties are selected. 

__UPDATE__: If you don't provide an array of the property names relevant to the query, Tiny will 
now examine the map function using `Function.prototype.toString` and gather up the property names. 

Note: there is a "shallow" option for both `.fetch()` and `.find()`, wherein if you don't explicitly 
select the properties you want from the object, it will __only__ lookup properties that are under 1kb 
in size. This is to go easy on the memory. 

## Other Usage

    db.set('myDocument', {
      title: 'a document',
      content: 'hello world'
    }, function() {
      // note: .each() is probably (and hopefully) inefficient 
      // compared to querying if you have a huge amount of data
      db.each(function(doc) { 
        console.log(doc.title);
      });
      db.remove('myDocument'); // delete the object/doc
    });
    
    db.get('someOtherThing', function(err, data) {
      console.log(data._key);
    });
    
    // extends/updates the object 
    // without overwriting its other properties
    db.update('article_1', { 
      title: 'new title'
    }, function(err) {
      console.log('done');
    });
  
## License

See LICENSE.