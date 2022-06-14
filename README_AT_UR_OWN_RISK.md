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


### Merge Compaction (!!!!! Most important)
- Referring [3]. 
- Merging is a NP-Hard problem.
- Problem is formulated as merging N sets into 1. Each set is $A\subscript{i}
- Cost is Sum ( | A[i] | + 2* | A[v] | + | A[root] | ) where v is middle non-leaf nodes in binary tree
- Considering 4 greedy heuristic algorithms from the paper
    - BALANCETREE 
    - SMALLESTINPUT 
    - SMALLESTOUTPUT 
    - LARGESTMATCH

    #### BALANCE TREE
    - LogN + 1 - Approximation ==> lower bound is atleast Logn + 1 times OPT, where OPT is optimal time

    #### SMALLEST INPUT
    - Just take smaller sets in each iteration to defer the bigger sets

    #### SMALLEST OUTPUT
    - In each interation, choose 2 sets whose output is minimum 

    ### LARGEST MATCH
    - Each iteration, take 2 sets who have the largest intersection
    - Worst case complexity can be arbitrarily bad
  
- Going with SMALLEST INPUT merging method as it provides LogN Approximation and way easier to implement compared to others.
- Major part of manifest has to be changed to maintain the list of segment names, created timestamp and their cardinality (compute while inserting them itself)
- While performing merge compaction of N segments, where N is configurable, following things are to be considered
    - Their merged segment size should'nt cross certain size limit
    - ~~Timestamp of newest merged segment = max (timestamps of all segments considered for merging)~~
    - Timestamps doesn't work as the difference to fill another 4kb memtable is very very small. Going with just incremental segmentId for this
    - For a new segment, segment id would be max(all segment id's) + 1
    - For a merged segment, segment id = max(all merged segments)
    - Cardinality is simple
- When to perform merge compaction on the background? when total number of segments become more than N
- Segments should be named according to segment id
- Is it better to hold the *os.File of db manifest file as long as program is running? Reason being we have to read and write from segment file so many times and opening and closing it everytime might be in-effficient 
- Referring [BigTable]


### Caching
- Can have 2 kinds of caches - Block and Table
- Block cache is a LRU cache which contains the recently seeked keys and has an upper memory limit (configurable) 
- Table cache stores the recently seeked file descriptors of Segment files (configurable)

## Logging
- Uber zap seems to be amazingly fast. [Reference](https://www.sobyte.net/post/2022-03/uber-zap-advanced-usage/)
- But zap is json-like (not very good looking for devevelopment purpose)
- Removing zap and going on with logrus (may come back later for production level logs)

## Data race here, data race there, data race everywhere :)
- Should think about properly preventing data races
- Right now just used sync.Mutex but there are other options as sync.RWMutex --> should check if this would replace normal Mutex and enhance performance




## Things that I found resourceful
- [1] [Blog #1](https://silhding.github.io/2021/08/20/A-Closer-Look-to-a-Key-Value-Storage-Engine/)
- [2] [My own notes](https://abesheknotes.netlify.app/docs/designing-data-intensive-applications/3-storage-and-retrieval/)
- [3] [Optimal Merge Compaction](https://www.researchgate.net/publication/283780735_Fast_Compaction_Algorithms_for_NoSQL_Databases)
- [4] [Merge Compaction in BigTable](https://arxiv.org/pdf/1407.3008.pdf)
- [5] [An In-depth Discussion on the LSM Compaction Mechanism](https://www.alibabacloud.com/blog/an-in-depth-discussion-on-the-lsm-compaction-mechanism_596780)
- [6] [Facebook's RocksDB](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview)