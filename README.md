# go-caskdb

[![Go Report Card](https://goreportcard.com/badge/github.com/abesheknarayan/go-caskdb)](https://goreportcard.com/report/github.com/abesheknarayan/go-caskdb)

Go-CaskDB is a disk-based, embedded, persistent, key-value store based on the [Riak's bitcask paper](https://riak.com/assets/bitcask-intro.pdf) , written in Golang inspired from [py-CaskDB](https://github.com/avinassh/py-caskdb). It is more focused on the educational capabilities than using it in production. The file format is platform, machine, and programming language independent.

## Tasks
- [x] Get, Set KV using disk as store
- [x] Loading data from disk onto memory 
- [x] Testing (Unit + Integration)
- [x] Proper logging
- [x] Split db file into several small files 
- [ ] Implement merging compaction using go-routines
- [ ] Key Deletion with Tombstone file
- [ ] Crash Safety with WAL
- [ ] RB-tree to support range scans
- [ ] Benchmarking
- [ ] Cache (LRU? EXPLORE!)
- [ ] Distributed using Paxos or consistent hashing


