import database;
import std.bitmanip;
import std.typecons;
import std.file;
import std.stdio;
import std.datetime;
import std.array;


/*
   msgbits - Bits are set for _unread_ messages.
   By default the bitset is clear - all messages considered read
   When joining a group, all group messages tagged as unread
   Benefit - finding first unread message is easy, which
   makes it easy to find next unread group. ('Next unread topic in group'
   requires stepping through group messages and comparing to bits however).
   Benefit - Most messages will be read, bitset is easy to compress.
*/

class MessageBoard
{
	class msgboard_exception : Throwable {
		this(string msg) { super(msg); }
	};



	struct Message
	{
		@sql("ROWID") ulong id;
		@sql("contents") string text;
		@sql("topicid") ulong topic;
		@sql("creatorid") ulong creator;
		@sql("parentid") ulong parent;
		ulong timestamp;
	}

	@sql("msggroup") struct Group
	{
		this(int id, string name)
		{
			this.id = id;
			this.name = name;
		}
		@sql("ROWID") ulong id;
		string name;
	}

	@sql("msgtopic") struct Topic
	{
		this(ulong id, ulong firstMsg, ulong group, string name, ulong creator)
		{
			this.id = id;
			this.firstMsg = firstMsg;
			this.group = group;
			this.name = name;
			this.creator = creator;
		}
	
		@sql("ROWID") ulong id;
		ulong firstMsg;
		@sql("groupid") ulong group;
		string name;
		ulong creator;
	}

	this(Database db, ulong userId)
	{
		this.db = db;
		this.currentUser = userId;
		init();
		auto query = db.query("SELECT bits FROM msgbits WHERE user=?", userId);
		void[] bits = query.get!(void[])();
		byte[] bytes = cast(byte[])bits;
		writefln("Length %d", bytes.length);
		unreadMessages = BitArray(bits, bits.length * 8);
		//foreach(i,b ; unreadMessages)
		//	writefln("%d : %s", i, b);
	}

	ulong getTimestamp() 
	{
		return Clock.currTime(UTC()).stdTime;
	}

	void init()
	{
		db.exec("CREATE TABLE IF NOT EXISTS msggroup (name TEXT, creatorid INT)");
		db.exec("CREATE TABLE IF NOT EXISTS msgtopic (name TEXT, creatorid INT, groupid INT, firstmsg INT, FOREIGN KEY(groupid) REFERENCES msggroup(ROWID), FOREIGN KEY(firstmsg) REFERENCES message(ROWID))");
		db.exec("CREATE TABLE IF NOT EXISTS message (contents TEXT, creatorid INT, parentid INT, topicid INT, timestamp INT, FOREIGN KEY(parentid) REFERENCES message(ROWID), FOREIGN KEY(topicid) REFERENCES msgtopic(ROWID))");
		db.exec("CREATE TABLE IF NOT EXISTS joinedgroups (user INT, groupid INT, FOREIGN KEY(groupid) REFERENCES msggroup(ROWID))");
		db.exec("CREATE TABLE IF NOT EXISTS msgbits (user INT, highmsg INT, bits BLOB, PRIMARY KEY(user))");
	}

	ulong createGroup(string name)
	{
		db.exec("INSERT INTO msggroup (name, creatorid) VALUES (?, ?)", name, currentUser);
		return db.last_rowid();
	}

	bool joinGroup(ulong groupId)
	{
		auto exists = db.query("SELECT EXISTS(SELECT 1 FROM joinedgroups WHERE user=? AND groupid=?)", currentUser, groupId).get!ulong();
		if(!exists) {
			db.exec("INSERT OR REPLACE INTO joinedgroups(user,groupid) VALUES (?,?)", currentUser, groupId);
			auto q = db.query("SELECT message.ROWID FROM message,msgtopic WHERE msgtopic.groupid=? AND message.topicid=msgtopic.ROWID", groupId);
			while(q.step()) {
				unreadMessages[q.get!ulong()-1] = true;
			}
		}
		return !exists;
	}

	Group getGroup(ulong id) {
		auto groups = db.select!Group("WHERE ROWID=?", id);
		auto q = db.query("SELECT ROWID,name,creatorid FROM msggroup WHERE ROWID=?", id);
		if(q.step())
			return q.get!Group();
		else
			throw new msgboard_exception("No such group");
	};

	Group getGroup(string name) {
		auto q = db.query("SELECT ROWID,name,creatorid FROM msggroup WHERE name=?", name);
		if(q.step())
			return q.get!Group();
		else
			throw new msgboard_exception("No such group");
	};

	Group enterGroup(ulong id) {
		currentGroup = getGroup(id);
		return currentGroup;
	}

	Group enterGroup(string groupName) {
		currentGroup = getGroup(groupName);
		return currentGroup;
	}

	Topic getTopic(ulong id) {
		Topic topic;
		auto q = db.query("SELECT ROWID,firstmsg,groupid,name,creatorid FROM msgtopic WHERE ROWID=?", id);
		if(q.step())
			return q.get!Topic();
		else
			throw new msgboard_exception("No such topic");
	};

	Message getMessage(ulong id)
	{
		auto q = db.query("SELECT ROWID,contents,topicid,creatorid,parentid,timestamp FROM message WHERE ROWID=?", id);
		if(q.step())
			return q.get!Message();
		else
			throw new msgboard_exception("No such message");
	}

	ulong post(string topicName, string text)
	{
		db.begin();
		scope(failure) db.rollback();
		scope(success) db.commit();

		auto ts = getTimestamp();
		if(currentGroup.id < 1)
			throw new msgboard_exception("No current group");

		db.exec("INSERT INTO msgtopic (name,creatorid,groupid) VALUES (?, ?, ?)", topicName, currentUser, currentGroup.id);
		auto topicid = db.last_rowid();
		db.exec("INSERT INTO message (contents, creatorid, parentid, topicid, timestamp) VALUES (?, ?, 0, ?, ?)", text, currentUser, topicid, ts);
		auto msgid = db.last_rowid();
		db.exec("UPDATE msgtopic SET firstmsg=? WHERE ROWID=?", msgid, topicid);
		setMessageRead(msgid);
		return msgid;
	}

	ulong reply(ulong msgid, string text)
	{
		ulong topicid = db.query("SELECT topicid FROM message WHERE ROWID=?", msgid).get!ulong();
		if(topicid == 0)
			throw new msgboard_exception("Repy failed, no such topic");
	
		auto ts = getTimestamp();
		db.exec("INSERT INTO message (contents, creatorid, parentid, topicid, timestamp) VALUES (?, ?, ?, ?, ?)", text, currentUser, msgid, topicid, ts);
		msgid = db.last_rowid();
		setMessageRead(msgid);
		return msgid;		
	}

	Topic[] listTopics(ulong group)
	{
		Topic[] topics;
		bool[ulong] found;
		auto q = db.query("SELECT message.ROWID,topicid,message.creatorid,timestamp FROM message,msgtopic WHERE topicid=msgtopic.ROWID AND msgtopic.groupid=?", group);
		while(q.step()) {
			auto t = q.get!(ulong,ulong,ulong,ulong);
			auto topicid = t[1];
			if(!(topicid in found)) {
				topics ~= getTopic(topicid);
				found[topicid] = true;
			}
		}
		return topics;
	}

	Message[] listMessages(ulong topicId) {
		Message[] messages;	

		messages = array(db.select!Message("where topicid = ?", topicId));
/*
		auto q = db.query("SELECT ROWID,contents,topicid,creatorid,parentid,timestamp FROM message WHERE topicid=?", topicId);
		while(q.step()) {
			messages ~= q.get!Message();
		}
*/
		return messages; // NOTE: std::move ?		
	}

	void flushBits() {
		db.select2!Message().where!"message.ROWID=?"(3);
		db.select2!(Message,Topic)().where!"topicid=msgtopic.ROWID AND msgtopic.groupid=?"(1);
		db.exec("INSERT OR REPLACE INTO msgbits(user, highmsg, bits) VALUES (?,?,?)", currentUser, 0, cast(void[])unreadMessages);
	}

	void setMessageRead(ulong no, bool read = true) {
		if(no >= unreadMessages.length)
			unreadMessages.length = no+1;
		unreadMessages[no] = !read;
	}

	bool isMessageRead(ulong no) {
		if(no >= unreadMessages.length)
			return true;
		return unreadMessages[no] == 0;
	}

	Database db;
	BitArray unreadMessages;
	ulong currentUser;
	Group currentGroup;

}


void doTests()
{
	writefln("HEY");
	std.file.remove("test.db");
	auto db = new Database("test.db");
	auto mb = new MessageBoard(db, 0);
	assert(mb.isMessageRead(42));
	mb.setMessageRead(42, false);
	assert(!mb.isMessageRead(42));
	mb.flushBits();


	db = new Database("test.db");
	auto mb2 = new MessageBoard(db, 0);
	assert(!mb2.isMessageRead(42));
	assert(mb2.isMessageRead(41));

	auto gid = mb2.createGroup("coding");
	assert(mb2.joinGroup(gid));
	auto group = mb2.getGroup(gid);
	assert(group.name == "coding");

	mb2.enterGroup("coding");
	auto mid = mb2.post("First post", "test message");
	mb2.reply(mid, "And I am replying");
	mb2.post("Second post", "test moar message");

	foreach(topic ; mb2.listTopics(gid)) {
		writefln("%s", topic.name);
	}

	auto ml = mb2.listMessages(1);
	foreach(t ; ml)
		writefln("Text: %s", t.text);
}

unittest {
	try {
		doTests();
	} catch (Exception e) {
		writefln("%s:%d : %s", e.file, e.line, e.msg);
	}
}


void main()
{
}
