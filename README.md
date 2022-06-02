# go-caskdb

[![Go Report Card](https://goreportcard.com/badge/github.com/abesheknarayan/go-caskdb)](https://goreportcard.com/report/github.com/abesheknarayan/go-caskdb)

Go-CaskDB is a disk-based, embedded, persistent, key-value store based on the [Riak's bitcask paper](https://riak.com/assets/bitcask-intro.pdf) , written in Golang inspired from [py-CaskDB](https://github.com/avinassh/py-caskdb). It is more focused on the educational capabilities than using it in production. The file format is platform, machine, and programming language independent.

## Tasks
- [x] Get, Set KV using disk as store
- [x] Loading data from disk onto memory 
- [x] testing
- [x] Proper logging
- [ ] Support for generic key and values (right now only for strings)
- [ ] need better way of handling bytes (very bad rn)
- [ ] Benchmarking
- [ ] Crash Safety
- [ ] Key Deletion
- [ ] RB-tree to support range scans
- [ ] Split db file into several small files (implement merging compaction using go-routines)
- [ ] Cache
- [ ] Garbage Collector for removing old deleted keys
- [ ] Distributed using Paxos or consistent hashing


