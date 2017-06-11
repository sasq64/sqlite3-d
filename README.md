
This is a small, D interface to sqlite3, on top of the old C api

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
auto query = db.query(Q.select!"name".from!User.where!"age>?"(30);
// (Fields are checked against tables, so if "name" had been misspelled
// the above would not compile).
```

