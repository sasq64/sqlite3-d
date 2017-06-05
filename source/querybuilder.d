import std.typecons;
import std.traits;
import std.string;
import std.conv;
import std.meta;


enum QueryState {
	Empty,
	Create,
	Insert
};

struct sqlname { string name; }; 

///
mixin template TEST() {
	struct Message {
		int id;
		string content;
	}

	struct User {
		string name; 
		@sqlname("len") int lengthInFeet;
	}

	@sqlname("DATA") struct MyData {
		int id;
		@sqlname("data") void[] blob;
	};
}

/// Common setup
unittest {
	struct Message {
		int id;
		string content;
	}

	struct User {
		string name; 
		@sqlname("len") int lengthInFeet;
	}

	@sqlname("DATA") struct MyData {
		int id;
		@sqlname("data") void[] blob;
	}
}


/// Utility class for generating SQL statements from compile time information.
struct QueryBuilder
{
	/** Generate the table name given STRUCT. Will return the STRUCT name,
	 * unless the struct has an @sqlname property renaming it to something else.
	 */
	static public template TableName(STRUCT) {
		enum ATTRS = __traits(getAttributes, STRUCT);
		static if(ATTRS.length > 0 && is(typeof(ATTRS[0]) == sqlname)) {
			enum TableName = ATTRS[0].name;
		} else
			enum TableName = STRUCT.stringof;
	};

	/// Generate a column name given a FIELD in STRUCT.
	static public template ColumnName(STRUCT, string FIELD) {
		enum ATTRS = __traits(getAttributes, __traits(getMember, STRUCT, FIELD));
		static if(ATTRS.length > 0 && is(typeof(ATTRS[0]) == sqlname))
			enum ColumnName = ATTRS[0].name;
		else
			enum ColumnName = FIELD;
	}

	/// Represents a set of fields for a SELECT statement
	struct Selection(FIELDS...)
	{
		/// Generate a $(B SELECT) statement by appending a $(B FROM) to the fields in the selection, verifying at compile time that each field maps to some field in some STRUCT.
		QueryBuilder from(TABLES...)()
		{
			pure bool isOK(string F)() {
				bool ok = false;
				foreach(TABLE ; TABLES) {
					foreach(N ; FieldNameTuple!TABLE) {
						ok |= (ColumnName!(TABLE,N) == F);
					}
				}
				return ok;
			}

			foreach(F ; FIELDS) {
				static assert(isOK!F(), "Field '" ~ F ~ "' not found in " ~ join([TABLES.stringof], ","));
			}

			string[] tables;

			foreach(I, STRUCT ; TABLES)
				tables ~= TableName!STRUCT;

			return This("SELECT " ~ join([FIELDS], ",") ~ " FROM " ~ join(tables, ","));
		}
	}

	alias This = QueryBuilder;
	public const string sql;
	alias sql this;

	this(string sql)
	{
		this.sql = sql;
	}

	private string sqlType(T)() if(is(T == string)) { return "TEXT"; }
	private string sqlType(T)() if(is(T == int)) { return "INT"; }
	private string sqlType(T)() if(is(T == void[])) { return "BLOB"; }


	/// Generate a CREATE TABLE statement from the given $(I STRUCT)
	@property public This create(STRUCT)() if(isAggregateType!STRUCT)
	{
		enum TABLE = TableName!STRUCT;
		alias FIELDS = Fields!STRUCT;
		string[] fields;

		foreach(I, N ; FieldNameTuple!STRUCT) {
			enum colName = ColumnName!(STRUCT, N);
			static if(colName != "rowid")
				fields ~= colName ~ " " ~ sqlType!(FIELDS[I])();
		}
		return QueryBuilder("CREATE TABLE " ~ TABLE ~ "(" ~ join(fields, ",") ~ ")");
	}

	///
	unittest {
		mixin TEST;

		assert(QueryBuilder().create!User() == "CREATE TABLE User(name TEXT,len INT)");
		assert(!__traits(compiles, QueryBuilder().create!int));
	}



	/// Append $(B IF NOT EXIST) to the $(B CREATE) statement
	@property public This ifNotExists() {
		return This(sql ~ " IF NOT EXISTS");
	}

	/** Generate the start of an $(B INSERT) statement from the given $(I STRUCT). Needs
	* to be completed with a $(B VALUES) suffix.
	*/
	@property public This insert(STRUCT)()
	{
		string[] fields;
		string[] qms;
		foreach(N ; FieldNameTuple!STRUCT) {
			enum cn = ColumnName!(STRUCT, N);
			static if(cn != "rowid") {
				fields ~= cn;
				qms ~= "?";
			}
		}
		return This("INSERT INTO " ~ TableName!STRUCT ~ "(" ~ join(fields, ",") ~ ") VALUES(" ~ join(qms, ",") ~ ")");
	}

	///
	unittest {
		mixin TEST;
		assert(QueryBuilder().insert!User() == "INSERT INTO User(name,len) VALUES(?,?)");
	}

	@property static private bool allString(STRING...)() {
		bool ok = true;
		foreach(S ; STRING)
			ok &= isSomeString!(typeof(S));
		return ok;
	}


	/** Generate the start of a $(B SELECT) statement for selecting the given fields. */
	public Selection!(FIELD) select(FIELD...)() if(allString!FIELD) {
		return Selection!(FIELD)();

	}

	private static string[] fieldNames(TABLES...)()
	{
		string[] fields;
		foreach(T ; TABLES) {
			foreach(F ; FieldNameTuple!T) {
				fields ~= ColumnName!(T, F);
			}
		}
		return fields;
	}

	///
	unittest {
		mixin TEST;
		import std.stdio;

		writeln(QueryBuilder().select!"name".from!User);
	}


	/// SELECT all FROM
	@property public This selectAllFrom(T...)()
	{
		string[] fields;
		string[] tables;
		foreach(I, Ti ; T) {
			enum TABLE = TableName!Ti;

			alias NAMES = FieldNameTuple!Ti;
			foreach(N ; NAMES) {
				fields ~= TABLE ~ "." ~ ColumnName!(Ti, N);
			}

			tables ~= TABLE;
		}
		return This("SELECT " ~ join(fields, ",") ~ " FROM " ~ join(tables, ","));
	}


	///
	unittest {
		mixin TEST;
		import std.stdio;
		writeln(QueryBuilder().selectAllFrom!(Message,User,MyData)());
	}

	@property public This from(T...)()
	{
		string[] tables;
		foreach(I, Ti ; T) {
			tables ~= TableName!Ti.stringof;
		}
		return This(sql ~ " FROM " ~ join(tables, ","));
	}

	public This where(string T)() {
		return This(sql ~ " WHERE " ~ T);
	}

} // QueryBuilder


///
unittest {

	import std.stdio;

	mixin TEST;

	alias QB = QueryBuilder;

	assert(QB().select!"content".from!Message.where!"idx == ?" == "SELECT content FROM Message WHERE idx == ?");
	assert(QB().create!Message == "CREATE TABLE Message(id INT,content TEXT)");

	assert(__traits(compiles, QB().select!("id", "content").from!Message));
	assert(!__traits(compiles, QB().select!("id", "conxtent").from!Message));


}


