var q = require('q'),
	ora = require('oracle'),
	qb = require('oraqb');

exports.connect = function(connectData) {
	return new Connection(connectData);
};










var Connection = function(connectData) {
	this._con = q.ninvoke(ora, 'connect', connectData).catch(function(e) { throw e; });
};

Connection.prototype = {

	then: function(fn) {
		return this._con.then(fn);
	},

	catch: function(fn) {
		return this._con.catch(fn);
	},

	query: function(sql, params) {
		return new Query(this, sql, params);
	}
};








var Query = function(connection, sql, params) {
	if (params === undefined) params = [];
	this._con = connection._con;
	this._query = qb.query(sql, params);
};

Query.prototype = {

	limit: function(num) {
		this._query.limit(num);
		return this;
	},

	offset: function(num) {
		this._query.offset(num);
		return this;
	},

	exec: function() {
		return this.all();
	},

	all: function () {
		return this._exec('all');
	},

	column: function() {
		return this._exec('column');
	},

	row: function() {
		return this._exec('row');
	},

	scalar: function() {
		return this._exec('scalar');
	},

	_exec: function(mode) {
		var query = this._query.build(),
			self = this;
		return self._con.then(function(con){
			return q.ninvoke(con, 'execute', query.sql, query.params)
				.then(function(rows) {
					return self._process[mode](rows);
				});
		});
	},

	_process: {
		all: function(rows) {
			if (!rows || !rows.length) return [];
			for (var rownum in rows) {
				var row = rows[rownum],
					rowFixed = {};
				for (var colname in row) {
					rowFixed[this._fixNameCase(colname)] = row[colname];
				}
				rows[rownum] = rowFixed;
			}
			return rows;
		},

		column: function(rows) {
			if (!rows || !rows.length) return [];
			var colname = Object.keys(rows[0])[0];
			for (var rownum in rows) {
				rows[rownum] = rows[rownum][colname];
			}
			return rows;
		},

		row: function(rows) {
			if (!rows || !rows.length) return undefined;
			rows = this.all([rows[0]]);
			return rows[0];
		},

		scalar: function(rows) {
			if (!rows || !rows.length) return undefined;
			var colname = Object.keys(rows[0])[0];
			return rows[0][colname];
		},

		_fixNameCase: function(name) {
			if (name.toUpperCase() === name)
				name = name.toLowerCase();
			return name;
		},
	},
};