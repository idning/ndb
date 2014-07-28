all: dep
	cd src && $(MAKE) $@

dep:
	cd deps/lua && make linux

clean:
	cd src && $(MAKE) $@

.PHONY: all clean
