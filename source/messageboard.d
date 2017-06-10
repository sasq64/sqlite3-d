import std.bitmanip;
import std.typecons;
import std.file;
import std.stdio;
import std.datetime;
import std.array;
import std.traits;

import querybuilder;
import database;

alias QB = QueryBuilder!(Empty);

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
	class msgboard_exception : Throwable 
	{
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

	struct Group
	{
		@sqlname("rowid") ulong id;
		@sqlname("creatorid") ulong creator;
		string name;
	}

	@sqlname("msgtopic") struct Topic
	{
		@sqlname("rowid") ulong id;
		@sqlkey("message(rowid)") ulong firstMsg;
		@sqlname("groupid") @sqlkey("'Group'(rowid)") ulong group;
		string name;
		@sqlname("creatorid") ulong creator;
	}

	struct JoinedGroup
	{
		ulong user;
		@sqlkey("'Group'(rowid)") ulong groupid;
	}

	alias BLOB = void[];

	struct MsgBits
	{
		@sqlkey() int user;
		int highmsg;
		void[] bits;
	};

	this(Database db, ulong userId)
	{
		this.db = db;
		this.currentUser = userId;
		init();

		try {
			MsgBits bits = db.selectOneWhere!(MsgBits, "user=?")(userId);
			unreadMessages = BitArray(bits.bits, bits.bits.length * 8);
		} catch(db_exception) { }
	}

	ulong getTimestamp() 
	{
		return Clock.currTime(UTC()).stdTime;
	}

	void init()
	{
		import std.typetuple;
		foreach(TABLE ; AliasSeq!(Group, Topic, Message, JoinedGroup, MsgBits))
			db.create!TABLE();
	}

	ulong createGroup(string name)
	{
		db.insert(Group(0, currentUser, name));
		return db.lastRowid();
	}

	bool joinGroup(ulong groupId)
	{
		auto q0 = db.selectAllWhere!(JoinedGroup, "user=? AND groupid=?")
			(currentUser, groupId);
		if(!q0.empty())
			return false;

		JoinedGroup jg = { user : currentUser, groupid : groupId } ;
		db.insert!(OR.Replace)(jg);
		auto q = db.query(QB.select!"Message.rowid".from!(Message,Topic).where!
				"msgtopic.groupid=? AND message.topicid=msgtopic.rowid"(groupId));
		while(q.step()) {
			unreadMessages[q.get!ulong()-1] = true;
		}
		return true;
	}

	Group getGroup(ulong id) 
	{
		return db.selectRow!Group(id);
	}

	Group getGroup(string name) 
	{
		return db.selectOneWhere!(Group, "name=?")(name);
	}

	Group enterGroup(ulong id) 
	{
		currentGroup = getGroup(id);
		return currentGroup;
	}

	Group enterGroup(string groupName) 
	{
		currentGroup = getGroup(groupName);
		return currentGroup;
	}

	Topic getTopic(ulong id)
	{
		return db.selectRow!Topic(id);
	}

	Message getMessage(ulong id)
	{
		return db.selectRow!Message(id);
	}

	ulong post(string topicName, string text)
	{
		db.begin();
		scope(failure) db.rollback();
		scope(success) db.commit();

		if(currentGroup.id < 1)
			throw new msgboard_exception("No current group");
		
		Topic topic = { 
			firstMsg : 0,
			group : currentGroup.id, 
			name : topicName, 
			creator : currentUser
		};
		db.insert(topic);
		auto topicid = db.lastRowid();
	
		Message msg = {
			text : text,
			creator : currentUser,
			topic : topicid,
			timestamp : getTimestamp()
		};
		db.insert(msg);
		auto msgid = db.lastRowid();

		db.exec(QB.update!Topic.set!"firstmsg=?"(msgid).where!"rowid=?"(topicid));
		setMessageRead(msgid);
		return msgid;
	}

	ulong reply(ulong msgid, string text)
	{
		auto topicid = db.selectRow!Message(msgid).topic;

		if(topicid == 0)
			throw new msgboard_exception("Repy failed, no such topic");
	
		Message msg = {
			text : text,
			creator : currentUser,
			parent : msgid,
			topic : topicid, timestamp : getTimestamp()
		};
		db.insert(msg);

		msgid = db.lastRowid();
		setMessageRead(msgid);
		return msgid;		
	}

	Topic[] listTopics(ulong group)
	{
		Topic[] topics;
		bool[ulong] found;
		auto q = db.query(QB.selectAllFrom!(Message, Topic).
				where!"topicid=msgtopic.rowid AND msgtopic.groupid=?"(group));

		while(q.step()) {
			auto tid = q.get!Message.topic;
			if(!(tid in found)) {
				topics ~= q.get!Topic;
				found[tid] = true;
			}
		}
		return topics;
	}

	Message[] listMessages(ulong topicId)
	{
		return  array(db.selectAllWhere!(Message,"topicid=?")(topicId));
	}

	void flushBits()
	{
		MsgBits mb = {
			user : cast(int)currentUser,
			highmsg : 0,
			bits : cast(void[])unreadMessages
		};
		db.insert!(OR.Replace)(mb);
	}

	void setMessageRead(ulong no, bool read = true)
	{
		if(no >= unreadMessages.length)
			unreadMessages.length = no+1;
		unreadMessages[no] = !read;
	}

	bool isMessageRead(ulong no)
	{
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
	mb2.reply(mid, "And I am replying");
	mb2.post("Second post", "test moar message");

	auto topics = mb2.listTopics(gid);
	assert(topics[0].name == "First post" && topics[1].name == "Second post");
	foreach(topic ; mb2.listTopics(gid)) {
		writefln("%s", topic.name);
	}

	auto ml = mb2.listMessages(1);
	foreach(t ; ml)
		writefln("Text: %s", t.text);
}


