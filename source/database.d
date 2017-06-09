import std.typecons;
import std.traits;
import std.string;
import std.conv;
import core.stdc.string;
import etc.c.sqlite3;
import std.stdio;
import std.meta;

import querybuilder;

//pragma(lib, "sqlite3");

alias QB = QueryBuilder!(Empty);

class db_exception : Exception {
	this(string msg, string file = __FILE__, size_t line = __LINE__) { super(msg, file, line); }
};

/// Try to remove 'name', return true on success
public bool tryRemove(string name) {
	import std.file;
	try {
		std.file.remove(name);
	} catch (FileException e) {
		return false;
	}
	return true;
}

alias toz = std.string.toStringz;

/// Setup code for tests
mixin template TEST(string dbname)
{
	struct User { 
		string name = ""; 
		int age = 0;
	};

	struct Message {
		@sqlname("rowid") int id;
		string content;
		int byUser;
	};

	Database db = () {
		tryRemove(dbname ~ ".db");
		return new Database(dbname ~ ".db");
	}();
}

/// An SQLITE3 database
class Database
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
	
		/// Construct a query from 'sql' into database 'db'
		this(sqlite3* db, string sql)			
		{
			writeln(sql);
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
			writefln("Binding BLOB at %d size %d", pos, arg.length);
			return sqlite3_bind_blob(stmt, pos, arg.ptr, cast(int)arg.length, null);
		}

		/// Bind fields of given struct in order to '?' marks in statement
		public void bind(STRUCT)(STRUCT s, uint bits = 0xffffffff) if(isAggregateType!STRUCT)
		{
			int bi = 1;
			foreach(i, N ; FieldNameTuple!STRUCT) {
				if(bits & 1<<i) {
					int rc = bindArg(bi++, __traits(getMember, s, N));
					checkError("Bind failed: ", rc);
				}
			}
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
					enum colName = ColumnName!(T, N);
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

	struct QueryIterator(T)
	{
		Query query;
		bool finished;
		this(Query q)
		{
			query = q;
			finished = !query.step();
		}

		bool empty() { return finished; }
		void popFront() { finished = !query.step(); }
		T front() {
			return query.get!T();
		}
	}

	public this(string dbFile)
	{
		int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
		int rc = sqlite3_open_v2(toz(dbFile), &db, flags, null);
		if(rc != SQLITE_OK)
			throw new db_exception("Could not open database");
	}

	public bool hasTable(string table)
	{
		return query("SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).step();
	}

	public long lastRowid()
	{
		return sqlite3_last_insert_rowid(db);
	}

	public bool exec(T...)(string sql, T args)
	{
		auto q = Query(db, sql);
		q.bind(args);
		return q.step();
	}

	public Query query(T...)(string sql, T args)
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

	public bool create(T)()
	{
		auto q = Query(db, QB.create!T());
		return q.step();
	}

	public QueryIterator!T selectAllWhere(T, string WHERE, ARGS...)(ARGS args)
	{
		auto q = Query(db, QB.selectAllFrom!T.where!WHERE(args));
		q.bind(args);
		return QueryIterator!T(q);
	}

	public T selectOneWhere(T, string WHERE, ARGS...)(ARGS args)
	{
		auto q = Query(db, QB.selectAllFrom!T().where!WHERE(args));
		q.bind(args);
		if(q.step())
			return q.get!T();
		else
			throw new db_exception("No match");
	}

	unittest {
		mixin TEST!("select");
		import std.array;
		import std.algorithm.iteration;
	
		db.create!User();
		db.insert(User("jonas", 55));
		db.insert(User("oliver", 91));
		db.insert(User("emma", 12));
		db.insert(User("maria", 27));

		User[] users = array(db.selectAllWhere!(User, "age > ?")(20));
		auto total = fold!((a,b) => User("", a.age + b.age))(users);
	
		assert(total.age == 55 + 91 + 27);
	};

	bool insert(T)(T row)
	{
		auto qb = QB.insert(row);
		Query q;
		try {
			q = Query(db, qb);
		} catch(db_exception dbe) {
			if(!hasTable(TableName!T)) {
				create!T();
				q = Query(db, qb);
			} else
				return false;
		}

		q.bind(qb.binds.expand);
		return q.step();
	}

	unittest {
		mixin TEST!("insert");
		User user = { "jonas", 45 };
		db.insert(user);
		assert(db.exec("select name from User where age = 45"));
		assert(!db.exec("select age from User where name = 'xxx'"));
	};

	auto commit() { return exec("commit"); }
	auto begin() { return exec("begin"); }
	auto rollback() { return exec("rollback"); }

	private bool autoCreateTable = true;
	private sqlite3 *db;
	alias db this;

}

unittest
{
	mixin TEST!"testdb";

	db.create!User();

	db.exec("INSERT INTO 'user' (name, age) VALUES (?,?)", "spacey", 15);
	db.exec("INSERT INTO 'user' (name, age) VALUES (?,?)", "joker", 42);
	db.exec("INSERT INTO 'user' (name, age) VALUES (?,?)", "rastapopoulos", 67);


	auto q = db.query(QB.select!("name", "age").from!"user".where!"age == ?"(42));
	//auto q = db.query("SELECT name, age FROM 'user' WHERE age == ?", 42);


	string[] names;
//	foreach(user ; db.select!User("where age > ?", 20))
//		names ~= user.name;


	assert(q.step());

	auto u = q.get!User;
	assert(u.name == "joker" && u.age == 42);

	db.create!Message();
	Message m = { -1, "Some text", 11 };
	db.insert(m);
	m.id = cast(int)db.lastRowid();
	writeln(m.id);

	auto qi = db.selectAllWhere!(Message, "byUser == ?")(11);
	writeln(qi.front());

	assert(qi.front().id == m.id);


	//QueryBuilder().select!("name", "page").from!User.where!"age == ?";

	//auto t2 = q.get!(string, int);
	//assert(t[0] == "joker");
	//assert(!q.step());
}


