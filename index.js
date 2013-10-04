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

	query: function(text, params) {
		return new Query(this, text, params);
	},

	rawQuery: function(text) {
		return new RawQuery(this, text);
	},

	close: function() {
		return this._con.then(function(c) {
			c.close();
		});
	}
};



var Query = function(connection, text, params) {
	if (params === undefined) params = [];
	this._con = connection._con;
	this._query = qb.query(text, params);
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
			return q.ninvoke(con, 'execute', query.text, query.params)
				.then(function(rows) {
					for (var rowNum in rows) {
						for (var colName in rows[rowNum]) {
							if (rows[rowNum][colName] instanceof Date)
								rows[rowNum][colName] = self._fixDate(rows[rowNum][colName]);
						}
					}
					return self._process[mode](rows);
				});
		});
	},

	_fixDate: function(date) {
		return new Date(date.getTime() + new Date().getTimezoneOffset() * 60 * 1000);
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




var RawQuery = function(connection, text) {
	this._con = connection._con;
	this._text = text;
};

RawQuery.prototype = {
	_exec: function() {
		var self = this;
		return self._con.then(function(con){
			return q.ninvoke(con, 'execute', self._text, [])
				.then(function(rows) {
					return self._process[mode](rows);
				});
		});
	},
	exec: Query.prototype.exec,
	all: Query.prototype.all,
	column: Query.prototype.column,
	row: Query.prototype.row,
	scalar: Query.prototype.scalar,
	_process: Query.prototype._process
}