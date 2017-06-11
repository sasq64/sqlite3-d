
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

