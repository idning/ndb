# we have to use g++ to static link libsnappy.a
LD=g++

#basic
CFLAGS=-std=c99 -pedantic -Wall -O0 -g -ggdb -D_POSIX_C_SOURCE -D_GNU_SOURCE -DNC_DEBUG_LOG -DHAVE_ASSERT_PANIC
CFLAGS+= -I./util/
LDLIBS= -lm -ldl
LDFLAGS=-g -ggdb -rdynamic

#lua
LDLIBS+= ../deps/lua/src/liblua.a
CFLAGS+= -I../deps/lua/src

#leveldb 
CFLAGS+= -I../deps/leveldb/include/
LDLIBS+= ../deps/leveldb/libleveldb.a ../deps/snappy/.libs/libsnappy.a -lpthread  
#LDLIBS+= ../deps/leveldb/libleveldb.a -lsnappy -lpthread  # this work with gcc

#hiredis
CFLAGS+= -I ../deps/hiredis/
LDLIBS+= ../deps/hiredis/libhiredis.a 

UTIL_SRC=$(wildcard util/*.c)
UTIL_OBJ=$(patsubst %.c,%.o,$(UTIL_SRC) )

all: bin
	@echo "done"
	@echo "run 'make test'"

%.o: %.c *.h util/*.h
	$(CC) -c -o $@ $(CFLAGS) $<

clean:
	rm -f $(ALL_BIN) 
	rm -f *.o *.gcda *.gcno *.gcov
	find . -name *.o | xargs rm -f
	find . -name *~ | xargs rm -f
	#rm -f cscope.*

#ndb-server
KV_BIN=ndb
KV_OBJ=$(UTIL_OBJ) ndb.o ndb_message.o ndb_leveldb.o ndb_command.o ndb_job.o ndb_cursor.o ndb_oplog.o ndb_repl.o ndb_stat.o
$(KV_BIN): $(KV_OBJ)
	$(LD) $(LDFLAGS) -o $@ $^ $(LDLIBS)

TEST_CONF_BIN=test_conf
TEST_CONF_OBJ=$(UTIL_OBJ) test/test_conf.o
$(TEST_CONF_BIN): $(TEST_CONF_OBJ)
	$(LD) $(LDFLAGS) -o $@ $^ $(LDLIBS)

#test-misc
TEST_ARRAY_BIN=test_misc
TEST_ARRAY_OBJ=$(UTIL_OBJ) test/test_misc.o 
$(TEST_ARRAY_BIN): $(TEST_ARRAY_OBJ)
	$(LD) $(LDFLAGS) -o $@ $^ $(LDLIBS)

#test-event
TEST_EVENT_BIN=test_event
TEST_EVENT_OBJ=$(UTIL_OBJ) test/test_event.o
$(TEST_EVENT_BIN): $(TEST_EVENT_OBJ)
	$(LD) $(LDFLAGS) -o $@ $^ $(LDLIBS)

#test-oplog
TEST_OPLOG_BIN=test_oplog
TEST_OPLOG_OBJ=$(UTIL_OBJ) test/test_oplog.o 
$(TEST_OPLOG_BIN): $(TEST_OPLOG_OBJ) 
	$(LD) $(LDFLAGS) -o $@ $^ $(LDLIBS)
test/test_oplog.o: ndb_oplog.c test/test_oplog.c

#test-repl
TEST_REPL_BIN=test_repl
TEST_REPL_OBJ=$(UTIL_OBJ) test/test_repl.o ndb_leveldb.o ndb_oplog.o ndb_job.o ndb_stat.o
$(TEST_REPL_BIN): $(TEST_REPL_OBJ) 
	$(LD) $(LDFLAGS) -o $@ $^ $(LDLIBS)
test/test_repl.o: ndb_repl.c test/test_repl.c

#summary
ALL_BIN=$(KV_BIN) $(TEST_CONF_BIN) $(TEST_ARRAY_BIN) $(TEST_EVENT_BIN) $(TEST_OPLOG_BIN) $(TEST_REPL_BIN) 
bin: $(ALL_BIN)

test: bin
	for number in $(ALL_BIN) ; do \
		echo $$number ; \
		./$$number 2>&1 | grep tests ; \
	done

.PHONY: test
