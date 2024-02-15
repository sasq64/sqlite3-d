import std.traits;
import std.typecons : tuple, Tuple ;
import std.string : join;
import std.algorithm.iteration : map;
import std.array : array;
import utils;

struct sqlkey { string key; }

version(unittest)
{
	struct User {
		string name;
		int age;
	}

	@sqlname("msg") struct Message {
		@sqlname("rowid") int id;
		string contents;
	}
}

static size_t countChars(string s, char c) {
	size_t matches = 0;
	foreach (character; s) {
		if (character == c) {
			matches++;
		}
	}

	return matches;
}

/// Get the tablename of `STRUCT`
static template TableName(STRUCT) {
	enum ATTRS = __traits(getAttributes, STRUCT);
	static if(ATTRS.length > 0 && is(typeof(ATTRS[0]) == sqlname)) {
		enum TableName = ATTRS[0].name;
	} else
		enum TableName = STRUCT.stringof;
};
///
unittest {
	struct User { string name; } 
	@sqlname("msg") struct Message { }

	assert(TableName!User == "User");
	assert(TableName!Message == "msg");
}

/// Generate a column name given a FIELD in STRUCT.
static template ColumnName(STRUCT, string FIELD) if(isAggregateType!STRUCT) {
	enum ATTRS = __traits(getAttributes, __traits(getMember, STRUCT, FIELD));
	static if(ATTRS.length > 0 && is(typeof(ATTRS[0]) == sqlname))
		enum ColumnName = ATTRS[0].name;
	else
		enum ColumnName = FIELD;
}

/// Return the qualifed column name of the given struct field
static template ColumnName(alias FIELDNAME)
{
	enum ATTRS = __traits(getAttributes, FIELDNAME);
	static if(ATTRS.length > 0 && is(typeof(ATTRS[0]) == sqlname))
		enum CN = ATTRS[0].name;
	else
		enum CN = FIELDNAME.stringof;

	enum ColumnName = quote(TableName!(__traits(parent, FIELDNAME))) ~ "." ~ quote(CN);
}
///
unittest {
	struct User { int age; } 
	@sqlname("msg") struct Message { @sqlname("txt") string contents; }
	assert(ColumnName!(User, "age") == "age");
	assert(tuple(ColumnName!(Message.contents), ColumnName!(User.age)) == tuple("'msg'.'txt'", "'User'.'age'"));
}

enum {
	Select, Set, Empty, SetWhere, From, SelectWhere, Update, Create, Insert, Delete
};

enum OR {
	None, Rollback, Abort, Replace, Fail, Ignore
}

/** An instance of a query building process */
struct QueryBuilder(int STATE = Empty, BINDS = Tuple!(), string[] SELECTS = [])
{
	BINDS args;
	public string sql;
	alias sql this;

	@property public BINDS binds() { return args; }

	private static bool checkField(string F, TABLES...)() {
		bool ok = false;
		foreach(TABLE ; TABLES) {
			enum tableName = TableName!TABLE;
			foreach(N ; FieldNameTuple!TABLE) {
				enum colName = ColumnName!(TABLE,N);
				ok |= ((colName == F) || (tableName ~ "." ~ colName == F)) 
				|| ((quote(colName) == F) || (quote(tableName) ~ "." ~ quote(colName) == F));
			}
		}
		return ok;
	}

	private static bool checkFields(string[] FIELDS, TABLES...)()
	{
		static if(FIELDS.length > 1)
			return checkField!(FIELDS[0], TABLES) && checkFields!(FIELDS[1..$], TABLES);
		else
			return checkField!(FIELDS[0], TABLES);
	}

	private static string sqlType(T)() if(isSomeString!T) { return "TEXT"; }
	private static string sqlType(T)() if(isFloatingPoint!T) { return "REAL"; }
	private static string sqlType(T)() if(isIntegral!T) { return "INT"; }
	private static string sqlType(T)() if(is(T == void[])) { return "BLOB"; }

	private static auto make(int STATE = Empty, string[] SELECTS = [], BINDS)(string sql, BINDS binds)
	{
		return QueryBuilder!(STATE, BINDS, SELECTS)(sql, binds);
	}

	private mixin template VerifyParams(string what, ARGS...)
	{
		static assert(countChars(what, '?') == A.length, "Incorrect number parameters: ");
	}

	this(string sql, BINDS args)
	{
		this.sql = sql;
		this.args = args;
	}

	public static auto create(STRUCT)() if(isAggregateType!STRUCT)
	{
		enum TABLE = TableName!STRUCT;
		alias FIELDS = Fields!STRUCT;
		string[] fields;
		string[] keys;

		foreach(I, N ; FieldNameTuple!STRUCT) {
			alias colName = ColumnName!(STRUCT, N);
			static if(colName != "rowid")
				fields ~= quote(colName) ~ " " ~ sqlType!(FIELDS[I]);
			enum ATTRS = __traits(getAttributes, __traits(getMember, STRUCT, N));
			foreach(A ; ATTRS)
				static if(is(typeof(A) == sqlkey)) {
					static if(A.key == "")
						keys ~= "PRIMARY KEY(" ~ colName ~ ")";
					else
						keys ~= "FOREIGN KEY(" ~ colName ~ ") REFERENCES " ~ A.key;
				}
		}

		fields ~= keys;

		return make!(Create)("CREATE TABLE IF NOT EXISTS " ~ quote(TABLE) ~ "(" ~ join(fields, ", ") ~ ")", tuple());
	}

	///
	unittest {
		assert(QueryBuilder.create!User() == "CREATE TABLE IF NOT EXISTS 'User'('name' TEXT, 'age' INT)");
		assert(!__traits(compiles, QueryBuilder().create!int));
	}

	// Get all field names in `s` to `fields`, and return the contents
	// of all fields as a tuple. Skips "rowid" fields.
	static auto getFields(STRUCT, int n = 0)(STRUCT s, ref string []fields)
	{
		enum L = (Fields!STRUCT).length;
		static if(n == L) {
			return(tuple());
		} else {
			enum NAME = (FieldNameTuple!STRUCT)[n];
			enum CN = ColumnName!(STRUCT, NAME);
			static if(CN == "rowid") {
				return tuple(getFields!(STRUCT, n+1)(s, fields).expand);
			} else {
				fields ~= CN;
				return tuple(s.tupleof[n], getFields!(STRUCT, n+1)(s, fields).expand);
			}
		}
	}

	static const string[] options = [
		"", "OR ROLLBACK ", "OR ABORT ", "OR REPLACE ", "OR FAIL "
	];

	public static auto insert(int OPTION = OR.None, STRUCT)(STRUCT s) if(isAggregateType!STRUCT)
	{
		string[] fields;
		auto t = getFields(s, fields);
		auto qms = map!(a => "?")(fields);
		return make!(Insert)("INSERT " ~ options[OPTION] ~ "INTO " ~ quote(TableName!STRUCT) ~ "(" ~ join(quote(fields), ",") ~ ") VALUES(" ~ join(qms, ",") ~ ")", t);
	}

	///
	unittest {
		User u = { name : "jonas", age : 13 };
		Message m  = { contents : "some text" };
		assert(QueryBuilder.insert(u) == "INSERT INTO 'User'('name','age') VALUES(?,?)");
		assert(QueryBuilder.insert(m) == "INSERT INTO 'msg'('contents') VALUES(?)");
	}

	///
	public static auto select(STRING...)()
	{
		auto sql = "SELECT " ~ join([STRING], ", ");
		return make!(Select, [STRING])(sql, tuple());
	}
	///
	unittest {
		assert(QueryBuilder.select!("only_one") == "SELECT only_one");
		assert(QueryBuilder.select!("hey", "you") == "SELECT hey, you");
	}

	///
	public static auto selectAllFrom(STRUCTS...)()
	{
		string[] fields;
		string[] tables;
		foreach(I, Ti ; STRUCTS) {
			enum TABLE = TableName!Ti;

			alias NAMES = FieldNameTuple!Ti;
			foreach(N ; NAMES) {
				fields ~= quote(TABLE) ~ "." ~ quote(ColumnName!(Ti, N));
			}

			tables ~= TABLE;
		}
		auto sql = "SELECT " ~ join(fields, ", ") ~ " FROM " ~ join(quote(tables), ",");
		return make!(From, [])(sql, tuple());
	}
	///
	unittest {
		assert(QueryBuilder.selectAllFrom!(Message, User) == "SELECT 'msg'.'rowid', 'msg'.'contents', 'User'.'name', 'User'.'age' FROM 'msg','User'");
	}

	///
	public auto from(TABLES...)() if(STATE == Select && allString!TABLES)
	{
		sql = sql ~ " FROM " ~ join([TABLES], ",");

		return make!(From, SELECTS)(sql, args);
	}

	///
	public auto from(TABLES...)() if(STATE == Select && allAggregate!TABLES)
	{
		static assert(checkFields!(SELECTS, TABLES), "Not all selected fields match column names");
		string[] tables;
		foreach(T ; TABLES) {
			tables ~= TableName!T;
		}
		sql = sql ~ " FROM " ~ join(quote(tables), ",");
		return make!(From, SELECTS)(sql, args);
	}

	///
	public auto set(string what, A...)(A a) if(STATE == Update)
	{
		mixin VerifyParams!(what, A);
		return make!(Set)(sql ~ " SET " ~ what, tuple(a));
	}

	///
	public static auto update(string table)()
	{
		return make!(Update)("UPDATE " ~ table, tuple());
	}

	///
	public static auto update(STRUCT)()
	{
		return make!(Update)("UPDATE " ~ TableName!STRUCT, tuple());
	}

	///
	public static auto update(STRUCT)(STRUCT s)
	{
		string[] fields;
		auto t = getFields(s, fields);
		return make!(Set)("UPDATE " ~ TableName!STRUCT ~ " SET " ~ join(fields, "=?, ") ~ "=?", t);
	}
	///
	unittest {
		User user = { name : "Jonas", age : 34 };
		assert(QueryBuilder.update(user) == "UPDATE User SET name=?, age=?");
	}

	///
	public auto where(string what, A...)(A args) if(STATE == Set)
	{
		mixin VerifyParams!(what, A);
		return make!(SetWhere, SELECTS)(sql ~ " WHERE " ~ what, tuple(this.args.expand, args));
	}

	///
	public auto where(string what, A...)(A args) if(STATE == From)
	{
		mixin VerifyParams!(what, A);
		return make!(SelectWhere, SELECTS)(sql ~ " WHERE " ~ what, tuple(this.args.expand, args));
	}

	///
	public auto where(string what, A...)(A args) if(STATE == Delete)
	{
		mixin VerifyParams!(what, A);
		return make!(SelectWhere, SELECTS)(sql ~ " WHERE " ~ what, tuple(this.args.expand, args));
	}

	///
	public static auto delete_(TABLE)() if(isAggregateType!TABLE)
	{
		return make!(Delete)("DELETE FROM " ~ TableName!TABLE, tuple());
	}

	///
	public static auto delete_(string tablename)()
	{
		return make!(Delete)("DELETE FROM " ~ tablename);
	}
	///
	unittest {
		QueryBuilder.delete_!User.where!"name=?"("greg");
	}
}

///
unittest
{
	alias Q = QueryBuilder!(Empty);
	alias C = ColumnName;

	// This will map to a "User" table in our database
	struct User {
		string name;
		int age;
	}

	assert(Q.create!User() == "CREATE TABLE IF NOT EXISTS 'User'('name' TEXT, 'age' INT)");

	auto qb0 = Q.select!"name".from!User.where!"age=?"(12);

	// The properties `sql` and `bind` can be used to access the generated sql and the
	// bound parameters
	assert(qb0.sql == "SELECT name FROM 'User' WHERE age=?");
	assert(qb0.binds == tuple(12));

	/// We can decorate structs and fields to give them different names in the database.
	@sqlname("msg") struct Message {
		@sqlname("rowid") int id;
		string contents;
	}

	// Note that virtual "rowid" field is handled differently -- it will not be created
	// by create(), and not inserted into by insert()

	assert(Q.create!Message() == "CREATE TABLE IF NOT EXISTS 'msg'('contents' TEXT)");

	Message m = { id : -1 /* Ingored */, contents : "Some message" };
	auto qb = Q.insert(m);
	assert(qb.sql == "INSERT INTO 'msg'('contents') VALUES(?)");
	assert(qb.binds == tuple("Some message"));


}

unittest
{
	import std.algorithm.iteration : uniq;
	import std.algorithm.searching : count;
	alias Q = QueryBuilder!(Empty);
	alias C = ColumnName;

	// Make sure all these generate the same sql statement
	string[] sql = [
		Q.select!("'msg'.'rowid'", "'msg'.'contents'").from!("'msg'").where!"'msg'.'rowid'=?"(1).sql,
		Q.select!("'msg'.'rowid'", "'msg'.'contents'").from!Message.where!(C!(Message.id) ~ "=?")(1).sql,
		Q.select!(C!(Message.id), C!(Message.contents)).from!Message.where!"'msg'.'rowid'=?"(1).sql,
		Q.selectAllFrom!Message.where!"'msg'.'rowid'=?"(1).sql
	];
	assert(count(uniq(sql)) == 1);
}
