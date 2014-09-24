# we have to use g++ to static link libsnappy.a
LD=g++

CFLAGS=-std=c99 -pedantic -Wall -O0 -g -ggdb -D_POSIX_C_SOURCE -D_GNU_SOURCE -DNC_DEBUG_LOG -DHAVE_ASSERT_PANIC
CFLAGS+= -I../deps/lua/src -I./util/

LDFLAGS=-g -ggdb -rdynamic

LDLIBS= -lm -ldl
LDLIBS+= ../deps/lua/src/liblua.a

#leveldb 
CFLAGS+= -I../deps/leveldb/include/
LDLIBS+= ../deps/leveldb/libleveldb.a ../deps/snappy/.libs/libsnappy.a -lpthread  
#LDLIBS+= ../deps/leveldb/libleveldb.a -lsnappy -lpthread  # this work with gcc

UTIL_SRC=$(wildcard util/*.c)
UTIL_OBJ=$(patsubst %.c,%.o,$(UTIL_SRC) )

all: bin
	@echo "done"
	@echo "run 'make test'"

%.o: %.c 
	$(CC) -c -o $@ $(CFLAGS) $<

clean:
	rm -f $(ALL_BIN) 
	rm -f *.o *.gcda *.gcno *.gcov
	find . -name *.o | xargs rm -f
	find . -name *~ | xargs rm -f
	#rm -f cscope.*

#kv-server
KV_BIN=ndb
KV_OBJ=$(UTIL_OBJ) ndb.o ndb_message.o ndb_leveldb.o ndb_command.o ndb_job.o ndb_cursor.o
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

#summary
ALL_BIN=$(KV_BIN) $(TEST_CONF_BIN) $(TEST_ARRAY_BIN) $(TEST_EVENT_BIN)
bin: $(ALL_BIN)

test: bin
	for number in $(ALL_BIN) ; do \
		echo $$number ; \
		./$$number 2>&1 | grep tests ; \
	done

.PHONY: test
