var ora = require('../');

var con;

describe('connect()', function() {



	it('connects to database', function() {
		var connected = false;

		runs(function() {
			con = ora.connect(require('./connectData.json'));
			con.then(function(){
					connected = true;
				}).catch(function(e) { throw e; });
		});

		waitsFor(function() {
			return connected;
		}, 'Connection should be established', 5000);
	});




	it('may execute queries', function() {
		var done = false;

		runs(function() {
			con.query('select * from dual')
				.exec()
				.then(function(result) {
					expect(result).toEqual([{dummy: 'X'}]);
					done = true;
				}).catch(function(e) { throw e; });
		});

		waitsFor(function() {
			return done;
		}, 'Query result should be returned', 5000);
	});






	var query = 'select * from dual union all select * from dual',
		column = ['X', 'X'],
		row = {dummy: 'X'},
		scalar = 'X';


	it('may fetch only a single column as array', function() {
		var done = false;

		runs(function() {
			con.query(query).column()
				.then(function(result) {
					expect(result).toEqual(column);
					done = true;
				}).catch(function(e) { throw e; });
		});

		waitsFor(function() {
			return done;
		}, 'Query result should be returned', 5000);
	});


	it('may fetch only a single row as object', function() {
		var done = false;

		runs(function() {
			con.query(query).row()
				.then(function(result) {
					expect(result).toEqual(row);
					done = true;
				}).catch(function(e) { throw e; });
		});

		waitsFor(function() {
			return done;
		}, 'Query result should be returned', 5000);
	});


	it('may fetch only a single value', function() {
		var done = false;

		runs(function() {
			con.query(query).scalar()
				.then(function(result) {
					expect(result).toEqual(scalar);
					done = true;
				}).catch(function(e) { throw e; });
		});

		waitsFor(function() {
			return done;
		}, 'Query result should be returned', 5000);
	});

});