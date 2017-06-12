
all :
	dub test

clean :
	dub clean
	rm -f *.db *.lst sqlite3-test-library
