all: snappy leveldb lua hiredis
	
leveldb: leveldb-1.15.0.tar.gz
	tar vzxf leveldb-1.15.0.tar.gz 
	mv leveldb-1.15.0 leveldb
	cd leveldb && make -j8

snappy: snappy-1.1.2.tar.gz
	tar vzxf snappy-1.1.2.tar.gz
	mv snappy-1.1.2 snappy
	cd snappy && ./configure && make -j8

.PHONY: lua hiredis

hiredis:
	cd hiredis && make 

lua:
	cd lua && make linux

clean:
	cd lua && make clean
	rm -rf snappy
	rm -rf leveldb
