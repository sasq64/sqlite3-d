import std.typecons : RefCounted, tuple, Tuple;
import std.traits;
import std.string : toStringz;
import std.conv : to;
import core.stdc.string : strcmp ;
import etc.c.sqlite3;
import std.exception : enforce;

import utils;
pragma(lib, "sqlite3");

class db_exception : Exception {
	this(string msg, string file = __FILE__, size_t line = __LINE__) { super(msg, file, line); }
};

alias toz = std.string.toStringz;

/// Setup code for tests
mixin template TEST(string dbname)
{
	SQLite3 db = () {
		tryRemove(dbname ~ ".db");
		return new SQLite3(dbname ~ ".db");
	}();
}

/// An sqlite3 database
class SQLite3
{
	struct Statement
	{
		~this() { if(s) sqlite3_finalize(s); s = null; } 
		sqlite3_stmt* s = null;
		alias s this;
	}

	/// Represents a sqlite3 statement
	struct Query
	{
		private sqlite3* db;
		private RefCounted!Statement stmt;
		public int lastCode = -1;
	
		/// Construct a query from the string 'sql' into database 'db'
		this(ARGS...)(sqlite3* db, string sql, ARGS args)			
		{
			sqlite3_stmt* s = null;
			int rc = sqlite3_prepare_v2(db, toz(sql), -1, &s, null);
			checkError("Prepare failed: ", rc);
			stmt.s = s;
			bind(args);
		}

		private int bindArg(int pos, string arg) 
		{
			return sqlite3_bind_text(stmt, pos, arg.ptr, cast(int)arg.length, null); 
		}

		private int bindArg(int pos, double arg) 
		{
			return sqlite3_bind_double(stmt, pos, arg);
		}

		private int bindArg(T)(int pos, T arg) if(isIntegral!T) {
			return sqlite3_bind_int64(stmt, pos, arg);
		}

		private int bindArg(int pos, void[] arg)
		{
			return sqlite3_bind_blob(stmt, pos, arg.ptr, cast(int)arg.length, null);
		}

		/// Bind these args in order to '?' marks in statement
		public void bind(ARGS...)(ARGS args)
		{
			int bi = 1;
			foreach(i, a ; args) {
				int rc = bindArg(bi++, a);
				checkError("Bind failed: ", rc);
			}
		}

		private T getArg(T)(int pos)
		{
			auto typ = sqlite3_column_type(stmt, pos);
			static if(isIntegral!T) {
				enforce!(db_exception)(typ == SQLITE_INTEGER,
						"Column is not an integer");
				return cast(T)sqlite3_column_int64(stmt, pos);
			} else static if(isSomeString!T) {
				enforce!(db_exception)(typ == SQLITE3_TEXT,
						"Column is not an string");
				return to!string(sqlite3_column_text(stmt, pos));
			} else static if(isFloat!T) {
				enforce!(db_exception)(typ == SQLITE_REAL,
						"Column is not an real");
				return sqlite3_column_double(stmt, pos);
			} else {
				enforce!(db_exception)(typ == SQLITE_BLOB,
						"Column is not a blob");
				void* ptr = cast(void*)sqlite3_column_blob(stmt, pos);
				int size = sqlite3_column_bytes(stmt, pos);
				return ptr[0..size].dup;
			}
		}

		private void getArg(T)(int pos, ref T t)
		{
			t = getArg!(T)(pos);
		}

		// Find column by name
		private int findName(string name)
		{
			auto zname = toz(name);
			for(int i=0; i<sqlite3_column_count(stmt); i++) {
				if(strcmp(sqlite3_column_name(stmt, i), zname) == 0)
					return i;
			}
			return -1;
		}

		/// Get current row (and column) as a basic type
		public T get(T, int COL = 0)() if(!(isAggregateType!T))
		{
			if(lastCode == -1)
				step();
			return getArg!T(COL);
		}

		/// Map current row to the fields of the given STRUCT
		public T get(T, int _ = 0)() if(isAggregateType!T)
		{
			if(lastCode == -1)
				step();
			T t;
			foreach(N ; FieldNameTuple!T) {
				enum ATTRS = __traits(getAttributes, __traits(getMember, T, N));
				static if(ATTRS.length > 0 && is(typeof(ATTRS[0]) == sqlname))
					enum colName = ATTRS[0].name;
				else
					enum colName = N;
				getArg(findName(colName), __traits(getMember, t, N));
			}
			return t;
		}

		/// Get current row as a tuple
		public Tuple!T get(T...)()
		{
			Tuple!(T) t;
			foreach(I, Ti ; T)
				t[I] = get!(Ti, I)();
			return t;
		}

		/// Step the SQL statement; move to next row of the result set. Return `false` if there are no more rows
		public bool step()
		{
			lastCode = sqlite3_step(stmt);
			checkError("Step failed", lastCode);
			return (lastCode == SQLITE_ROW);
		}

		/// Reset the statement, to step through the resulting rows again.
		public void reset()
		{
			sqlite3_reset(stmt);
		}

		private void checkError(string prefix, int rc, string file = __FILE__, int line = __LINE__)
		{
			if(rc < 0)
				rc = sqlite3_errcode(db);
			if(rc != SQLITE_OK && rc != SQLITE_ROW && rc != SQLITE_DONE) {
				throw new db_exception(prefix ~ " (" ~ to!string(rc) ~ "): " ~ to!string(sqlite3_errmsg(db)), file, line);
			}	
		}
	}

	///
	unittest {
		mixin TEST!"query";

		auto q = Query(db, "create table TEST(a INT, b INT)");
		assert(!q.step());

		q = Query(db, "insert into TEST values(?, ?)");
		q.bind(1,2);
		assert(!q.step());
		q = Query(db, "select b from TEST where a == ?", 1);
		assert(q.step());
		assert(q.get!int() == 2);
		assert(!q.step());

		q = Query(db, "select a,b from TEST where b == ?", 2);
		// Try not stepping... assert(q.step());
		assert(q.get!(int,int)() == tuple(1,2));

		struct Test {
			int a;
			int b;
		}

		auto test = q.get!Test();
		assert(test.a == 1 && test.b == 2);

		assert(!q.step());

		q.reset();
		assert(q.step());
		assert(q.get!(int, int)() == tuple(1,2));

		// Test exception
		bool caught = false;
		try {
			q.get!(string);
		} catch(db_exception e) {
			caught = true;
		}
		assert(caught);
	}

	/** Create a SQLite3 from a database file. If file does not exist, the
	  * database will be initialized as new
	 */
	public this(string dbFile)
	{
		int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
		int rc = sqlite3_open_v2(toz(dbFile), &db, flags, null);
		if(rc != SQLITE_OK)
			throw new db_exception("Could not open database");
	}

	/// Execute an sql statement directly, binding the args to it
	public bool exec(ARGS...)(string sql, ARGS args)
	{
		auto q = Query(db, sql);
		q.bind(args);
		q.step();
		return (q.lastCode == SQLITE_DONE || q.lastCode == SQLITE_ROW);
	}

	///
	unittest {
		mixin TEST!("exec");
		assert(db.exec("CREATE TABLE Test(name STRING)"));
		assert(db.exec("INSERT INTO Test VALUES (?)", "hey"));
	}

	/// Return 'true' if database contains the given table
	public bool hasTable(string table)
	{
		return query(
			"SELECT name FROM sqlite_master WHERE type='table' AND name=?",
			table).step();
	}

	///
	unittest {
		mixin TEST!("hastable");
		assert(!db.hasTable("MyTable"));
		db.exec("CREATE TABLE MyTable(id INT)");
		assert(db.hasTable("MyTable"));
	}

	/// Return the 'rowid' produced by the last insert statement
	public long lastRowid()
	{
		return sqlite3_last_insert_rowid(db);
	}

	///
	unittest {
		mixin TEST!("lastrowid");
		assert(db.exec("CREATE TABLE MyTable(name STRING)"));
		assert(db.exec("INSERT INTO MyTable VALUES (?)", "hey"));
		assert(db.lastRowid() == 1);
		assert(db.exec("INSERT INTO MyTable VALUES (?)", "ho"));
		assert(db.lastRowid() == 2);
		// Only insert updates the last rowid
		assert(db.exec("UPDATE MyTable SET name=? WHERE rowid=?", "woo", 1));
		assert(db.lastRowid() == 2);
	}

	/// Create query from string and args to bind
	public Query query(ARGS...)(string sql, ARGS args)
	{
		auto q = Query(db, sql);
		q.bind(args);
		return q;
	}

	/// Create query from QueryBuilder like class
	public Query query(SOMEQUERY)(SOMEQUERY sq) if(hasMember!(SOMEQUERY, "sql") && hasMember!(SOMEQUERY, "binds"))
	{
		auto q = Query(db, sq.sql);
		q.bind(sq.binds.expand);
		return q;
	}

	auto commit() { return exec("commit"); }
	auto begin() { return exec("begin"); }
	auto rollback() { return exec("rollback"); }

	unittest {
		mixin TEST!"transaction";
		db.begin();
		assert(db.exec("CREATE TABLE MyTable(name STRING)"));
		assert(db.exec("INSERT INTO MyTable VALUES (?)", "hey"));
		db.rollback();
		assert(!db.hasTable("MyTable"));
		db.begin();
		assert(db.exec("CREATE TABLE MyTable(name STRING)"));
		assert(db.exec("INSERT INTO MyTable VALUES (?)", "hey"));
		db.commit();
		assert(db.hasTable("MyTable"));
	}

	protected sqlite3 *db;
	alias db this;
}

