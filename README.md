<img src="http://i.imgur.com/qPcZ7qp.jpg" />

*"You put your data in it."*

HeftyDB is a sorted key-value library for the JVM. It was designed with the following goals in mind:

1. Be as fast and memory efficient as is feasible on the JVM.
2. Provide a stable base on which to build new and interesting distributed storage systems. 
3. Provide detailed metrics about what is going on at every level of the stack.
4. Have clean, understandable code that others with no database background can easily understand.

##Features

###Simple API
Supports gets and puts by key as well as ascending and descending iteration from any key.

###Log Structured Merge Trees
All write operations are sequential, and are limited by the sequential IO performance of the underlying disk. Tables
are written with a full B+tree index for memory efficiency, even at very large table sizes.

###Snapshotted multi-version concurrency control
Reads and range scans never block writes.

###Off-heap data structures
Operations in the critical read and write paths are implemented using off-heap memory wherever possible to reduce GC pressure and memory overhead.

###Multi-threaded compactions
Make use of multiple CPU cores for table writes and table compactions

###Pluggable compaction strategies
Provide custom compaction behavior tailored to specific workloads.

##Example Usage

```java
try {
    //Open a HeftyDB in a directory
    DB testDB = HeftyDB.open(new Config.Builder().directory(directory).build());

    //Write a key
    Snapshot snapshot = testDB.put(someByteBufferKey, someByteBufferValue);

    //Read a key at a particular snapshot
    Record record = testDB.get(someByteBufferKey, snapshot);

    //Get an ascending iterator of keys greater than or equal
    //to the provided key at the provided snapshot
    Iterator<Record> ascendingIterator = testDB.ascendingIterator(someByteBufferKey, snapshot);
    while (ascendingIterator.hasNext()){
        Record next = ascendingIterator.next();
    }

    //Get a descending iterator of keys less than or equal
    //to the provided key at the provided snapshot
    Iterator<Record> descendingIterator = testDB.descendingIterator(someByteBufferKey, snapshot);
    while (descendingIterator.hasNext()){
        Record next = descendingIterator.next();
    }

    //Compact the database
    testDB.compact();

    //Close the database
    testDB.close();
} catch (IOException e){
    Logger.error(e);
}
```

##Benchmarks

### "Moderate" Endurance Test

Concurrent reads, writes, and range scans with 16 threads dedicated to each task. Each thread does a small number of
operations and then yields. This test is designed to run under a profiler with out being too slow,
but is a decent workload.

~25% CPU on a 2013 Haswell Retina MBP

<pre>
-- Timers ----------------------------------------------------------------------
heftydb.read
             count = 2429774
         mean rate = 27003.13 calls/second
     1-minute rate = 26357.71 calls/second
     5-minute rate = 24674.39 calls/second
    15-minute rate = 24172.81 calls/second
               min = 0.00 milliseconds
               max = 0.50 milliseconds
              mean = 0.03 milliseconds
            stddev = 0.04 milliseconds
            median = 0.02 milliseconds
              75% <= 0.03 milliseconds
              95% <= 0.08 milliseconds
              98% <= 0.13 milliseconds
              99% <= 0.18 milliseconds
            99.9% <= 0.49 milliseconds
heftydb.scan
             count = 1297430
         mean rate = 14418.29 calls/second
     1-minute rate = 14269.46 calls/second
     5-minute rate = 13804.30 calls/second
    15-minute rate = 13669.96 calls/second
               min = 0.00 milliseconds
               max = 0.04 milliseconds
              mean = 0.00 milliseconds
            stddev = 0.00 milliseconds
            median = 0.00 milliseconds
              75% <= 0.00 milliseconds
              95% <= 0.00 milliseconds
              98% <= 0.00 milliseconds
              99% <= 0.00 milliseconds
            99.9% <= 0.04 milliseconds
heftydb.write
             count = 1245520
         mean rate = 13840.83 calls/second
     1-minute rate = 13506.19 calls/second
     5-minute rate = 12554.54 calls/second
    15-minute rate = 12278.59 calls/second
               min = 0.00 milliseconds
               max = 4.53 milliseconds
              mean = 0.06 milliseconds
            stddev = 0.24 milliseconds
            median = 0.01 milliseconds
              75% <= 0.02 milliseconds
              95% <= 0.36 milliseconds
              98% <= 0.64 milliseconds
              99% <= 0.77 milliseconds
            99.9% <= 4.51 milliseconds

</pre>

More coming soon.




