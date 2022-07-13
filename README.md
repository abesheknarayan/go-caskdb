# go-caskdb

[![Go Report Card](https://goreportcard.com/badge/github.com/abesheknarayan/go-caskdb)](https://goreportcard.com/report/github.com/abesheknarayan/go-caskdb)

Go-CaskDB is a disk-based, embedded, persistent, key-value store written in Golang. It started off as a key value store based on the [Riak's bitcask paper](https://riak.com/assets/bitcask-intro.pdf) and also inspired from [py-CaskDB](https://github.com/avinassh/py-caskdb). Now, its a key value store using LSM-Trees and SSTables with merge compaction strategy similar to that of Google's LevelDB. It is more focused on the educational capabilities than using it in production. The file format is platform, machine, and programming language independent.

Refer [Link](https://github.com/abesheknarayan/go-caskdb/tree/main/docs/README_AT_UR_OWN_RISK.md) for more on how different components of this database is designed.

## Benchmarks
Refer [Link](https://github.com/abesheknarayan/go-caskdb/tree/main/docs/Benchmarks.md) for the benchmarking of this db.


## Tasks
- [x] Get, Set KV using disk as store
- [x] Loading data from disk onto memory 
- [x] Testing (Unit + Integration)
- [x] Proper logging
- [x] Split db file into several small files 
- [x] Implement merging compaction strategy 
- [ ] Key Deletion with Tombstone file
- [ ] Crash Safety with WAL
- [ ] Benchmarking
- [ ] Cache (Block + Table)
- [ ] Bloom filter for fast non-existent key reads
- [ ] Data Compression
- [ ] RB-tree to support range scans
- [ ] Distributed using Paxos or consistent hashing


