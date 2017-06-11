import std.typecons : RefCounted, tuple, Tuple;
import std.traits;
//import std.string;
import std.conv : to;
import core.stdc.string : strcmp ;
import etc.c.sqlite3;
//import std.stdio;

import utils;
pragma(lib, "sqlite3");

class db_exception : Exception {
	this(string msg, string file = __FILE__, size_t line = __LINE__) { super(msg, file, line); }
};

alias toz = std.string.toStringz;

/// Setup code for tests
mixin template TEST(string dbname)
{
	SQLiteDb db = () {
		tryRemove(dbname ~ ".db");
		return new SQLiteDb(dbname ~ ".db");
	}();
}

/// An SQLITE3 database
class SQLiteDb
{
	struct Statement
	{
		this(sqlite3_stmt *s) { this.s = s; }
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
		this(sqlite3* db, string sql)			
		{
			sqlite3_stmt* s = null;
			int rc = sqlite3_prepare_v2(db, toz(sql), -1, &s, null);
			checkError("Prepare failed: ", rc);
			stmt.s = s;
		}

		private int bindArg(int pos, string arg) {
			return sqlite3_bind_text(stmt, pos, arg.ptr, cast(int)arg.length, null); 
		}

		private int bindArg(int pos, double arg) {
			return sqlite3_bind_double(stmt, pos, arg);
		}

		private int bindArg(T)(int pos, T arg) if(isIntegral!T) {
			return sqlite3_bind_int64(stmt, pos, arg);
		}

		private int bindArg(int pos, void[] arg) {
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
			static if(isIntegral!T)
				return sqlite3_column_int(stmt, pos);
			else static if(isSomeString!T)
				return to!string(sqlite3_column_text(stmt, pos));
			else {
				void* ptr = cast(void*)sqlite3_column_blob(stmt, pos);
				int size = sqlite3_column_bytes(stmt, pos);
				return ptr[0..size].dup;
			}
		}

		private void getArg(T)(int pos, ref T t)
		{
			t = getArg!(T)(pos);
		}

		private int findName(string name) {
			auto zname = toz(name);
			for(int i=0; i<sqlite3_column_count(stmt); i++) {
				if(strcmp(sqlite3_column_name(stmt, i), zname) == 0)
					return i;
			}
			return -1;
		}

		// Get current row as the given type T;
		// * if T is a struct, try to map each field to a column name and assign
		// * if T is a typle of structs, do the above for each struct
		// * if T is a tuple, assign each column to each type of the tuple

		/** Get current row and map it to T.
		  * - If T is a Fundamental type, get the single column as that type
		  * - If T is an Aggregate type, try to map all column names to field names
		  */
		public T get(T, int COL = 0)()
		{
			if(lastCode == -1)
				step();
			static if(isAggregateType!T) {
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
			} else
				return getArg!T(COL);
		}

		/// Get current row as a tuple
		public Tuple!T get(T...)()
		{
			Tuple!(T) t;
			foreach(I, Ti ; T)
				t[I] = get!(Ti, I)();
			return t;
		}

		/// Step the SQL statement, return `false` if there are no more rows
		public bool step()
		{
			lastCode = sqlite3_step(stmt);
			checkError("Step failed", lastCode);
			return (lastCode == SQLITE_ROW);
		}

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
		q = Query(db, "select b from TEST where a == ?");
		q.bind(1);
		assert(q.step());
		assert(q.get!int() == 2);
		assert(!q.step());

		q = Query(db, "select a,b from TEST where b == ?");
		q.bind(2);
		assert(q.step());
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
	}

	/** Create a SQLiteDb from a database file. If file does not exist, the
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

	public Query query(ARGS...)(string sql, ARGS args)
	{
		auto q = Query(db, sql);
		q.bind(args);
		return q;
	}

	public Query query(SOMEQUERY)(SOMEQUERY sq) if(hasMember!(SOMEQUERY, "sql") && hasMember!(SOMEQUERY, "binds"))
	{
		auto q = Query(db, sq.sql);
		q.bind(sq.binds.expand);
		//auto q = Query(db, "select name from user");
		return q;
	}

	auto commit() { return exec("commit"); }
	auto begin() { return exec("begin"); }
	auto rollback() { return exec("rollback"); }

	protected sqlite3 *db;
	alias db this;
}

