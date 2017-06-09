import std.bitmanip;
import std.typecons;
import std.file;
import std.stdio;
import std.datetime;
import std.array;
import std.traits;

import querybuilder;
import database;

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
		@sqlname("rowid") ulong id;
		@sqlname("contents") string text;
		@sqlname("topicid") @sqlkey("msgtopic(rowid)") ulong topic;
		@sqlname("creatorid") ulong creator;
		@sqlname("parentid") @sqlkey("message(rowid)") ulong parent;
		ulong timestamp;
	}

	@sqlname("msggroup") struct Group
	{
		@sqlname("rowid") ulong id;
		@sqlname("creatorid") ulong creator;
		string name;
	}

	@sqlname("msgtopic") struct Topic
	{
		@sqlname("rowid") ulong id;
		@sqlkey("message(rowid)") ulong firstMsg;
		@sqlname("groupid") @sqlkey("msggroup(rowid)") ulong group;
		string name;
		@sqlname("creatorid") ulong creator;
	}

	@sqlname("joinedgroups") struct JoinedGroup
	{
		ulong user;
		@sqlkey("msggroup(rowid)") ulong groupid;
	}

	alias BLOB = void[];

	this(Database db, ulong userId)
	{
		this.db = db;
		this.currentUser = userId;
		init();
		auto query = db.query(QB.select!"bits".from!"msgbits".where!"user=?"(userId));
		//auto query = db.query("SELECT bits FROM msgbits WHERE user=?", userId);
		if(query.step()) {
			void[] bits = query.get!BLOB();
			unreadMessages = BitArray(bits, bits.length * 8);
		} else
			writeln("Could not read bits");
	}

	ulong getTimestamp() 
	{
		return Clock.currTime(UTC()).stdTime;
	}

	struct MsgBits
	{
		@sqlkey() int user;
		int highmsg;
		void[] bits;
	};

	void init()
	{
		import std.typetuple;
		foreach(TABLE ; TypeTuple!(Group, Topic, Message, JoinedGroup, MsgBits))
			db.create!TABLE();
	
		//db.exec("CREATE TABLE IF NOT EXISTS msggroup (name TEXT, creatorid INT)");
		//db.exec("CREATE TABLE IF NOT EXISTS msgtopic (name TEXT, creatorid INT, groupid INT, firstmsg INT, FOREIGN KEY(groupid) REFERENCES msggroup(rowid), FOREIGN KEY(firstmsg) REFERENCES message(ROWID))");
		//db.exec("CREATE TABLE IF NOT EXISTS message (contents TEXT, creatorid INT, parentid INT, topicid INT, timestamp INT, FOREIGN KEY(parentid) REFERENCES message(rowid), FOREIGN KEY(topicid) REFERENCES msgtopic(ROWID))");
		//db.exec("CREATE TABLE IF NOT EXISTS joinedgroups (user INT, groupid INT, FOREIGN KEY(groupid) REFERENCES msggroup(rowid))");
		//db.exec("CREATE TABLE IF NOT EXISTS msgbits (user INT, highmsg INT, bits BLOB, PRIMARY KEY(user))");
	}

	ulong createGroup(string name)
	{
		db.insert(Group(0, currentUser, name));
		//db.exec("INSERT INTO msggroup (name, creatorid) VALUES (?, ?)", name, currentUser);
		return db.lastRowid();
	}

	bool joinGroup(ulong groupId)
	{
		auto q0 = db.selectAllWhere!(JoinedGroup, "user=? AND groupid=?")(currentUser, groupId);

		//auto exists = db.query("SELECT EXISTS(SELECT 1 FROM joinedgroups WHERE user=? AND groupid=?)", currentUser, groupId).get!ulong();
		//if(!exists) {
		if(q0.empty()) {
			//db.exec("INSERT OR REPLACE INTO joinedgroups(user,groupid) VALUES (?,?)", currentUser, groupId);
			JoinedGroup jg = { user : currentUser, groupid : groupId } ;
			db.insert(jg);
			//auto q = db.query("SELECT message.rowid FROM message,msgtopic WHERE msgtopic.groupid=? AND message.topicid=msgtopic.ROWID", groupId);
			auto q = db.query(QB.select!"Message.rowid".from!(Message,Topic).where!"msgtopic.groupid=? AND message.topicid=msgtopic.rowid"(groupId));
			while(q.step()) {
				unreadMessages[q.get!ulong()-1] = true;
			}
			return true;
		}
		return false;
		//return !exists;
	}

	Group getGroup(ulong id) {
		auto groups = db.selectAllWhere!(Group,"rowid=?")(id);
		return groups.front();

		//enum fields = Fields!(Group.id, Group.name, Group.creator);
		//pragma(msg, "FIELDS " ~ fields);

		//auto q = db.select!(Fields!(Group.id, Group.name, Group.creator)).from!Group.where!"rowid=?"(id);
		//auto q = db.selectAllFrom!Group.where!"rowid=?"(id);

		//auto q = db.query("SELECT rowid,name,creatorid FROM msggroup WHERE ROWID=?", id);
		//if(q.step())
		//	return q.get!Group();
		//else
		//	throw new msgboard_exception("No such group");
	};

	Group getGroup(string name) {

		return db.selectOneWhere!(Group, "name=?")(name);

		/* auto q = db.query("SELECT rowid,name,creatorid FROM msggroup WHERE name=?", name); */
		/* if(q.step()) */
		/* 	return q.get!Group(); */
		/* else */
		/* 	throw new msgboard_exception("No such group"); */
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
		return db.selectOneWhere!(Topic, "rowid=?")(id);
		/* Topic topic; */
		/* auto q = db.query("SELECT rowid,firstmsg,groupid,name,creatorid FROM msgtopic WHERE ROWID=?", id); */
		/* if(q.step()) */
		/* 	return q.get!Topic(); */
		/* else */
		/* 	throw new msgboard_exception("No such topic"); */
	};

	Message getMessage(ulong id)
	{
		return db.selectOneWhere!(Message, "rowid=?")(id);
		/* auto q = db.query("SELECT rowid,contents,topicid,creatorid,parentid,timestamp FROM message WHERE ROWID=?", id); */
		/* if(q.step()) */
		/* 	return q.get!Message(); */
		/* else */
		/* 	throw new msgboard_exception("No such message"); */
	}

	ulong post(string topicName, string text)
	{
		db.begin();
		scope(failure) db.rollback();
		scope(success) db.commit();

	//	auto ts = getTimestamp();
		if(currentGroup.id < 1)
			throw new msgboard_exception("No current group");
		
		Topic topic = { 0, 0, currentGroup.id, topicName, currentUser };
		db.insert(topic);

		//db.exec("INSERT INTO msgtopic (name,creatorid,groupid) VALUES (?, ?, ?)", topicName, currentUser, currentGroup.id);
		auto topicid = db.lastRowid();
		Message msg = { text : text, creator : currentUser, topic : topicid, timestamp : getTimestamp() };
		db.insert(msg);
		//db.exec("INSERT INTO message (contents, creatorid, parentid, topicid, timestamp) VALUES (?, ?, 0, ?, ?)", text, currentUser, topicid, ts);
		auto msgid = db.lastRowid();
		//db.update!Topic.set"firstmsg=?"(msgid).where!"rowid=?"(topicid);

		db.exec("UPDATE msgtopic SET firstmsg=? WHERE rowid=?", msgid, topicid);
		//db.update!(Topic).set!"firstmsg=?"(msgid).where!"rowid=?"(topicid);
		setMessageRead(msgid);
		return msgid;
	}

	ulong reply(ulong msgid, string text)
	{
		//db.select!"topicid".from!Message.where!"rowid=?"(msgid);
		//ulong topicid = db.query("SELECT topicid FROM message WHERE rowid=?", msgid).get!ulong();
		enum field = ColumnName!(Message.topic);
		auto topicid = db.query(QB.select!(ColumnName!(Message.topic)).from!Message.where!"rowid=?"(msgid)).get!ulong();


		if(topicid == 0)
			throw new msgboard_exception("Repy failed, no such topic");
	
		//auto ts = getTimestamp();
		//db.exec("INSERT INTO message (contents, creatorid, parentid, topicid, timestamp) VALUES (?, ?, ?, ?, ?)", text, currentUser, msgid, topicid, ts);
		Message msg = { text : text, creator : currentUser, parent : msgid, topic : topicid, timestamp : getTimestamp() };
		db.insert(msg);

		msgid = db.lastRowid();
		setMessageRead(msgid);
		return msgid;		
	}

	Topic[] listTopics(ulong group)
	{
		Topic[] topics;
		bool[ulong] found;
		//auto q = db.query("SELECT topicid FROM message,msgtopic WHERE topicid=msgtopic.ROWID AND msgtopic.groupid=?", group);
		//auto q = db.selectAllFrom!(Message, Topic).where!"topicid=msgtopic.rowid AND msgtopic.groupid=?"(group);
		auto q = db.query(QB.select!"topicid".from!(Message, Topic).where!"topicid=msgtopic.rowid AND msgtopic.groupid=?"(group));

		while(q.step()) {
			auto message = q.get!(Message);
			//auto t = q.get!(ulong,ulong,ulong,ulong);
			//auto topicid = t[1];
			if(!(message.topic in found)) {
				topics ~= getTopic(message.topic);
				found[message.topic] = true;
			}
		}
		return topics;
	}

	Message[] listMessages(ulong topicId) {
		Message[] messages;	

		messages = array(db.selectAllWhere!(Message,"topicid = ?")(topicId));
/*
		auto q = db.query("SELECT rowid,contents,topicid,creatorid,parentid,timestamp FROM message WHERE topicid=?", topicId);
		while(q.step()) {
			messages ~= q.get!Message();
		}
*/
		return messages; // NOTE: std::move ?		
	}

	void flushBits() {
		/* unreadMessages[1] = true; */
		/* unreadMessages[11] = true; */
		/* unreadMessages[21] = true; */
		/* unreadMessages[31] = true; */
		db.exec("INSERT OR REPLACE INTO msgbits(user, highmsg, bits) VALUES (?,?,?)", currentUser, 4, cast(void[])unreadMessages);
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


unittest {
	writefln("HEY");
	tryRemove("test.db");
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
	writefln("MID %d", mid);
	mb2.reply(mid, "And I am replying");
	mb2.post("Second post", "test moar message");

	foreach(topic ; mb2.listTopics(gid)) {
		writefln("%s", topic.name);
	}

	auto ml = mb2.listMessages(1);
	foreach(t ; ml)
		writefln("Text: %s", t.text);
}


