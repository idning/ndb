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

todo
====

1. not use mbuf.
2. we need a nc_dict.c for cmd table
3. update leveldb api, support compaction_speed, default_comparator.
    - leveldb_open return err: Invalid argument: thecmp does not match existing comparator : simple-cmp
    - if we use thecmp, this db can not open by other apps
4. #compact
5. #expire
6. #scan
7. master-slave
8. backup (dump / copy file, if we have scan, we do not need this)
9. #info
10. #FLUSHDB

type:

- #string
- hash
- list

we do not want to support:

- set
- zset
- script

