Mostly contains stuff where I debate with myself on which design / lib / ... is better to use


## DB Internals

### Why is having an in-disk data structure like hash table / SSTables with LSM-Trees better than just storing stuff sequentially  and loading a in-memory hash table everytime we start the app ?
- Having every key indexed in an in-memory hash table has limitations of memory size, lets say we have a db of size 10 GB running on a machine with 8GB RAM, it is not really possible to build the in-memory hash index due to memory limitations. So when we actually do the same thing with disks, we could just load a small portion onto RAM (if necessary at all)
- Problems with large dbs on startups : everytime we start the app (in our case as its embedded) it might take a long time to load all the keys onto the in-memory hash table


### Okay, we settled down with in-disk > in-memory. But what should we use? Hash tables or LSM Trees?
- Basically LSM trees have an in-memory tree (memtable [ in sorted order ] ) and once the memtable reaches a certain limit it flushes the contents on to the disk called segment. Each segment will be of constant size (mostly around page size of 4KB). Over time, as many segments arise, compaction is used to compress the segments into a newer one
- Hash tables are similar to in-memory hash tables nothing specifically changes with disk. Hash reads are of random order not sequential. But disks are better for sequential reads than random. 


### Going with LSM with segment size of 4KB
- how to do memtable? binary heap?
- how to check its size? it has to be less than 4kb
- what file format to use for segments
- how to do compaction in the background?
- do we need to store something external about the db itself? db for a db? details like how many segments

- Metadata file for db (this needn't be in binary format) : should contain info like number of segments and stuff
- Hash Index acts as a cache here where it will contain N number of key-value pairs at max (N can be configurable by user) in LRU manner 
- Load memtable at db start for the latest segment


## Logging
- Uber zap seems to be amazingly fast. [Reference](https://www.sobyte.net/post/2022-03/uber-zap-advanced-usage/)
- But zap is json-like (not very good looking for devevelopment purpose)
- Removing zap and going on with logrus (may come back later for production level logs)


## Things that I found resourceful
- [Blog #1](https://silhding.github.io/2021/08/20/A-Closer-Look-to-a-Key-Value-Storage-Engine/)
- [My own notes](https://abesheknotes.netlify.app/docs/designing-data-intensive-applications/3-storage-and-retrieval/)