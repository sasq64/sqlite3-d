import utils : tryRemove, sqlname;
import sqlite;
import querybuilder;

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

alias db_exception = sqlite.db_exception;

/// An Database with query building capabilities
class Database : SQLite3
{
	alias QB = QueryBuilder!(Empty);
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
		import std.array : array;
		import std.algorithm.iteration : fold;
	
		db.create!User();
		db.insert(User("jonas", 55));
		db.insert(User("oliver", 91));
		db.insert(User("emma", 12));
		db.insert(User("maria", 27));

		User[] users = array(db.selectAllWhere!(User, "age > ?")(20));
		auto total = fold!((a,b) => User("", a.age + b.age))(users);
	
		assert(total.age == 55 + 91 + 27);

		assert(db.selectOneWhere!(User, "age == ?")(27).name == "maria");

		assert(db.selectRow!User(2).age == 91);

	};

	bool insert(int OPTION = OR.None, T)(T row)
	{
		auto qb = QB.insert!OPTION(row);
		Query q;
		if(autoCreateTable) {
			try {
				q = Query(db, qb);
			} catch(db_exception dbe) {
				if(!hasTable(TableName!T)) {
					create!T();
					q = Query(db, qb);
				} else
					return false;
			}
		} else
			q = Query(db, qb);

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
	// Test quoting by using keyword as table and column name
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

