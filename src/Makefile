LD=$(CC)

CFLAGS=-std=c99 -pedantic -Wall -O0 -g -ggdb -D_POSIX_C_SOURCE -D_GNU_SOURCE -DNC_DEBUG_LOG
CFLAGS+= -I../deps/lua/src -I./util/

LDFLAGS=-g -ggdb -rdynamic

LDLIBS= -lm -ldl
LDLIBS+= ../deps/lua/src/liblua.a

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

#kv-server
KV_BIN=kv-server
KV_OBJ=$(UTIL_OBJ) kv.o
$(KV_BIN): $(KV_OBJ)
	$(LD) $(LDFLAGS) -o $@ $^ $(LDLIBS)

#test-conf
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
		./$$number ; \
	done

.PHONY: test
