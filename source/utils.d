import std.traits;

/// Try to remove 'name', return true on success
bool tryRemove(string name) {
	import std.file;
	try {
		std.file.remove(name);
	} catch (FileException e) {
		return false;
	}
	return true;
}

struct sqlname { string name; }

static string quote(string s, string q = "'")
{
	return q ~ s ~ q;
}

static string[] quote(string[] s, string q = "'")
{
	string[] res;
	foreach(t ; s)
		res ~= q ~ t ~ q;
	return res;
}

@property bool allString(STRING...)() {
	bool ok = true;
	foreach(S ; STRING)
		static if(is(S))
		ok = false;
	else
		ok &= isSomeString!(typeof(S));
	return ok;
}

@property bool allAggregate(ARGS...)() {
	bool ok = true;
	foreach(A ; ARGS)
		static if(is(A))
		ok &= isAggregateType!A;
	else
		ok = false;
	return ok;
}

