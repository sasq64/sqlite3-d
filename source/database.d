import std.typecons;
import std.traits;
import std.string;
import std.conv;
import std.stdio;
import std.meta;

import sqlite;
import querybuilder;
import utils : tryRemove, sqlname;

alias QB = QueryBuilder!(Empty);

alias db_exception = sqlite.db_exception;

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

/// An Database with query building capabilities
class Database : SQLiteDb
{
	// Returned from select-type methods where the row type is known
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

	public this(string name)
	{
		super(name);
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

	public T selectRow(T)(ulong row)
	{
		return selectOneWhere!(T, "rowid=?")(row);
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
	
		writeln("##" ~ to!string(users.length) ~ " " ~ to!string(total));
		assert(total.age == 55 + 91 + 27);
	};

	bool insert(int OPTION = OR.None, T)(T row)
	{
		auto qb = QB.insert!OPTION(row);
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
		assert(db.query("select name from User where age = 45").step());
		assert(!db.query("select age from User where name = 'xxx'").step());

	};

	private bool autoCreateTable = true;

}

unittest
{
	mixin TEST!"testdb";
	struct Group {
		int Group;
	}

	db.create!Group();
	Group g = { 3 };
	db.insert(g);
	Group gg = db.selectOneWhere!(Group, "\"Group\"=3");
	assert(gg.Group == g.Group);
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


