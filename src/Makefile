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
	rm -rf $(ALL_BIN) 
	rm -rf *.o *.gcda *.gcno *.gcov
	find . -name *.o | xargs rm -f
	find . -name *~ | xargs rm -f
	rm -f tags cscope.*

#kv-server
KV_BIN=ndb
KV_OBJ=$(UTIL_OBJ) ndb.o ndb_message.o ndb_leveldb.o ndb_command.o ndb_job.o ndb_ttl.o
$(KV_BIN): $(KV_OBJ)
	$(LD) $(LDFLAGS) -o $@ $^ $(LDLIBS)

#summary
ALL_BIN=$(KV_BIN)
bin: $(ALL_BIN)

test: bin
	for number in $(ALL_BIN) ; do \
		echo $$number ; \
		./$$number ; \
	done

.PHONY: test
