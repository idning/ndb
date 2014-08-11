all: dep
	cd src && $(MAKE) $@

dep:
	cd deps && $(MAKE)

test:
	cd test && $(MAKE) test


clean:
	cd deps && $(MAKE) $@
	cd src && $(MAKE) $@
	find . -name core | xargs rm -f
	rm -f tags cscope.*
	rm -rf db


.PHONY: all clean test
