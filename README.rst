ndb
===

- redis protocol
- store with leveldb
- config with lua
- got 50k/s on GET/SET on small data with redis-benchmark no pipeline.

todo
====

1. not use mbuf.
2. we need a nc_dict.c for cmd table
3. update leveldb api, support compaction_speed, default_comparator.
4. compact
5. expire

type:

- #string
- hash
- list

we do not support:

- set
- zset
- script
