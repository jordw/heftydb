#HeftyDB

*"You put your data in it."*

HeftyDB is a sorted key-value library for the JVM. It was designed with the following goals in mind:

1. Be as fast and memory efficient as is feasible on the JVM.
2. Provide a stable base on which to build new and interesting distributed storage systems. 
3. Provide detailed metrics about what is going on at every level of the stack, from the top-level API down to disk IO.
3. Have clean, understandable code that others with no database background can easily understand.

##Features

###Simple API
Supports gets and puts by key as well as ascending and descending iteration from any key.

###Log Structured Merge Trees
All write operations are sequential, and are limited by the sequential IO performance of the underlying disk.

###Snapshotted multi-version concurrency control
Reads and range scans never block writes

###Off-heap data structures
Operations in the critical read and write paths are implemented using off-heap memory wherever possible to reduce GC pressure and memory overhead.

###Pluggable Compaction Strategies
It's easy to provide custom compaction behavior suitable to specific work loads.

##Example Usage

```java
try {
    DB testDB = HeftyDB.open(new Config.Builder().directory(directory).build());

    Snapshot snapshot = db.put(someByteBufferKey, someByteBufferValue);
    Record record = db.get(someByteBufferKey, snapshot);
    Iterator<Record> ascendingIterator = testDB.ascendingIterator(someByteBufferKey, snapshot);

    while (ascendingIterator.hasNext()){
        Record next = ascendingIterator.next();
    }

    testDB.compact();
    testDB.close();

} catch (IOException e){
    Logger.error(e);
}
```




