var Tiny = require('../tiny');

// in the words of doc emmit brown, i apologize for the crudity of this model

// ive only been testing querying here, i havent started doing any serious benchmarks
// or performance tests

// also, making up fake data is annoying

var obj = [
	{
		num: 200,
		test3: 'test property lorem ipsum',
		test4: 'test property lorem ipsum',
	},
	{
		num: 82,
		two: 'hi',
		three: { uhoh: 'uhoh' },
		four: 'whats up',
		five: {
			one: 'test',
			hi: 'hello',
			world: { hey: 'whats up' }
		}
	},
	{
		num: 102,
		again: 'hi',
		hey: 'whats up',
	},
	{
		num: 150,
		more: 'text',
		and: 'what not',
	}
];

//Tiny.precache(); // cache properties smaller than 1kb in size on load

Tiny('test.tiny', function(err, db) {
	if(0) db.set('one', obj[0], function() {
		console.log('done');
		db.set('two', obj[1], function() {
			console.log('done');
			db.set('three', obj[2], function() {
				console.log('done');
				db.set('four', obj[3], function() {
					console.log('done');
				});
			});
		});
	});
	if(1) db.get('one', function(err, data) {
		console.log(data);
	});
	// db.query is the non-chainable version of db.find
	if(0) db.query({two: 'hi'}, function(err, results) {
		console.log('RESULTS:', results);
	}, {select: ['four']});
	if(0) db.find({two: 'hi'}).select('four', 'three')(function(err, results) {
		console.log('RESULTS:', results);
	});
	if(0) db.find({two: { $eq: 'hi' }}).select('four', 'three')(function(err, results) {
		console.log('RESULTS:', results);
	});
	if(0) db.find({num: { $gte: 72 }, two: { $eq: 'hi' }}).desc('num').limit(2)(function(err, results) {
		console.log('RESULTS:', results);
	});
	if(0) db.find({$or: [ { num: { $gt: 82 } }, { num: { $eq: 82 } }  ] }).desc('num').limit(3)(function(err, results) {
		console.log('RESULTS:', results);
	});
	
	
	// the function below is equivalent to the above query
	// the first parameter is the name of the 
	// properties relevant to your "map" function
	// if it is omitted, all properties will be available 
	// for comparison, however, this may come at a cost
	// the map function (second param) is similar to CouchDB's
	// mapreduce/view interface except there is no emit() function,
	// you simply return the property names you wish to select in an
	// array e.g.
	// "return ['title', 'content'];"
	// returning "true" will lookup all properties for that document
	// the final parameter is the callback which is executed once the
	// map iterations have completed and the results have 
	// been compiled.
	// this is an internal method used by Tiny 
	// the mongo-style querying is built on top of it
	if(0) db.fetch(['num'], function(doc, total) {
		if (total === 3) return 'break';
		if (doc.num > 82 || doc.num === 82) {
			console.log('found', doc._key); // doc._key is always available no matter what the context
			return true;
		}
	}, function(err, results) {
		console.log('RESULTS:', Tiny.sort.desc(results, 'num'));
	});

	
	// these will only display something meaningful here if precaching is enabled
	//console.log('CACHE:', db._cache);
	//console.log('CACHE SIZE:', db._cacheSize + ' bytes');
});



// test to make sure big properties arent cached
//obj1.test1 = 'test property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsumtest property lorem ipsum';