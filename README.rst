ndb
===

- redis protocol
- store with leveldb
- config with lua
- got 50k/s on GET/SET with redis-benchmark no pipeline.        (less then 10,000,000 records -- data in mem)
- got 100K SET, 300K GET /s on small data with pipeline=1000    (more then )

performance
===========

- data: 2G/20G/200G
- pipeline: 1/1000
- media: ssd/hdd

