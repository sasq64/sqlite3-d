
This is a small, simple yet powerful interface on top of sqlite3.
It is intended for simplicty and safety when using a database from
application code. It is not intended for server side / enterprise
database access.

TLDR
====

```D
struct User {
	@sqlname("rowid") id;
	string name;
	int age;
}

auto db = new Database("test.db");

User user = { name : "jake", age : 45 }; // rowid is not inserted, no need to set

db.insert(user); // Rely on auto table creation when insert fails
User[] users = array(db.selectAllWhere!(User, "age > ?")(30));
auto jake = db.selectRowId!User(1);
assert(jake.name == users[0].name == "jake");
```


BASIC USAGE
===========

The `SQLite3` class is how you create or open a sqlite3 database.

```D
auto db = new SQLite3("datafile.db");
```

Use `exec()` for commands that you do not need results from. Parameter types must
match database types or an exception will be thrown.

```D
db.exec("CREATE TABLE IF NOT EXISTS user (name TEXT, id INT, image BLOB)");

string userName = "james";
ulong id = 123;

db.exec("INSERT INTO user (name, id) VALUES (?, ?)", name, id);
db.exec("INSERT INTO user (name, id) VALUES (?, ?)", id, name); // <-- Will throw an exception
```

Use the  method `query()` when you want to read results.

```D
auto q = db.query("SELECT id,name FROM user WHERE name like ?", "a%");
```

Use `step()` and `get(T)()` to fetch rows from the result. `get()` will
use compile time reflection to create an object of the given class using the row result.

```D
struct User {
    ulong id;
    string name;
    void[] pixels;
};

User[] users;
auto q = db.query("SELECT id,name FROM user");
while(q.step()) {
    users ~= q.get!User;
}
```

Blobs are supported using `void[]`.

```D
void[] pixels;
db.exec("INSERT INTO user (image) VALUES (?)", pixels);
```

Note that `get()` can be called with a struct as above, or a type tuple, or
just a single type when that is all you want. 

```D
auto q = db.query("SELECT image FROM user WHERE id=?", id);
q.step();
auto pixels2 = q.get!(void[])();
```

If you haven't called `step()` on a query, it will be called automatically by `get()`

```D
auto id = db.query("SELECT id FROM user WHERE name=?", userName).get!ulong();
```


QUERYBUILDER
============

Use the `QueryBuilder` class to construct sql queries using compile time
arguments and information;

```D
alias Q = QueryBuilder!();

struct User {
    string name;
    int age;
}

User u = { name : "jake", age : 45 };

// Generate and perform a CREATE TABLE User(name STRING, age INT) ...
db.exec(Q.create!User());

// Generate and perform an INSERT
db.exec(Q.insert(user));

// Generate a SELECT statement and create a query from it;
auto query = db.query(Q.select!"name".from!User.where!"age>?"(30));
// (Column names are checked against field in structs , so if "name" had been
// misspelled the above would not compile).
```


DATABASE
========

The `Database` class combines the functionality of `QueryBuilder` with `SQLite3` and
allows for usage such as;

```D
	db.create!User;
	db.insert(user);
	foreach(user ; db.selectAllWhere(User, "age > ?")("30"))
		writeln(user.name);
	int age = db.selectOneWhere(User, "name == ?")("jake").age;
```
