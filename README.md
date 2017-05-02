# HeftyDB

*"You put your data in it."*

HeftyDB is a persistent, sorted, key-value library for the JVM. It was designed with the following goals in mind:

1. Be as fast and memory efficient as is feasible on the JVM for random reads, random writes, and range scans.
2. Provide a stable base on which to build new and interesting storage systems.
3. Provide detailed metrics about what is going on at every level of the stack.
4. Have clean, understandable code that others can learn from.

*Note: HeftyDB was built primarily for fun and learning. While the code is generally production quality and has extensive test coverage, you probably shouldn't use it in production unless you know what you are doing. So, yea, don't use it in production and lose a bunch of important data, or something.*

## Features

### Simple API
Supports gets and puts by key as well as ascending and descending iteration from any key.

### Log Structured Merge Trees
All write operations are sequential, and are limited by the sequential IO performance of the underlying disk. Tables
are written with a full B+tree index for memory efficiency, even at very large table sizes.

### Snapshotted multi-version concurrency control
Reads and range scans never block writes.

### Off-heap data structures
Operations in the critical read and write paths are implemented using off-heap memory wherever possible to reduce GC
pressure and memory overhead.

### Multi-threaded compactions
Make use of multiple CPU cores for table writes and table compactions

### Pluggable compaction strategies
Provide custom compaction behavior tailored to specific workloads.

## Design Details
https://github.com/jordw/heftydb/wiki/Design-Overview

## Example Usage

```java
try {
    //Open a HeftyDB in a directory
    DB testDB = HeftyDB.open(new Config.Builder().directory(directory).build());

    //Write a key
    Snapshot snapshot = testDB.put(someByteBufferKey, someByteBufferValue);

    //Read a key at a particular snapshot
    Record record = testDB.get(someByteBufferKey, snapshot);

    //Delete a key
    Snapshot deleteSnapshot = testDB.delete(someByteBufferKey);

    //Get an ascending iterator of keys greater than or equal
    //to the provided key at the provided snapshot
    CloseableIterator<Record> ascendingIterator = testDB.ascendingIterator(someByteBufferKey, snapshot);
    
    while (ascendingIterator.hasNext()) {
        Record next = ascendingIterator.next();
    }

    //Get a descending iterator of keys less than or equal
    //to the provided key at the provided snapshot
    CloseableIterator<Record> descendingIterator = testDB.descendingIterator(someByteBufferKey, snapshot);
    
    while (descendingIterator.hasNext()) {
        Record next = descendingIterator.next();
    }

    //Compact the database
    Future<?> compactionFuture = testDB.compact();
    compactionFuture.get();

    //Close the database
    testDB.close();
} catch (IOException e){
    Logger.error(e);
}
```




