/*
 * Copyright (c) 2013. Jordan Williams
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jordanwilliams.heftydb.table.memory;

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.read.LatestTupleIterator;
import com.jordanwilliams.heftydb.table.MutableTable;
import com.jordanwilliams.heftydb.table.Table;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryTable implements MutableTable {

    private final long id;
    private final ConcurrentNavigableMap<Key, Tuple> records = new ConcurrentSkipListMap<Key, Tuple>();
    private final AtomicLong maxSnapshotId = new AtomicLong();
    private final AtomicInteger recordCount = new AtomicInteger();
    private final AtomicInteger size = new AtomicInteger();

    public MemoryTable(long id) {
        this.id = id;
    }

    @Override
    public void put(Tuple tuple) {
        records.put(tuple.key(), tuple);
        recordCount.incrementAndGet();
        size.addAndGet(tuple.size());
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public boolean mightContain(Key key) {
        Key floorKey = records.floorKey(key);
        return floorKey != null && floorKey.data().equals(key.data());
    }

    @Override
    public Tuple get(Key key) {
        Map.Entry<Key, Tuple> closestEntry = records.floorEntry(key);
        return closestEntry == null ? null : closestEntry.getValue();
    }

    @Override
    public Iterator<Tuple> ascendingIterator(long snapshotId) {
        return new LatestTupleIterator(snapshotId, records.values().iterator());
    }

    @Override
    public Iterator<Tuple> descendingIterator(long snapshotId) {
        return new LatestTupleIterator(snapshotId, records.descendingMap().values().iterator());
    }

    @Override
    public Iterator<Tuple> ascendingIterator(Key key, long snapshotId) {
        return new LatestTupleIterator(snapshotId, records.tailMap(key, true).values().iterator());
    }

    @Override
    public Iterator<Tuple> descendingIterator(Key key, long snapshotId) {
        return new LatestTupleIterator(snapshotId, records.headMap(key, true).descendingMap().values().iterator());
    }

    @Override
    public long tupleCount() {
        return recordCount.get();
    }

    @Override
    public long size() {
        return size.get();
    }

    @Override
    public int level() {
        return 0;
    }

    @Override
    public long maxSnapshotId() {
        return maxSnapshotId.get();
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public Iterator<Tuple> iterator() {
        return records.values().iterator();
    }

    @Override
    public int compareTo(Table o) {
        return Long.compare(id, o.id());
    }

    private void advanceMaxSnapshotId(long newSnapshotId) {
        while (true) {
            long currentMaxSnapshotId = maxSnapshotId.get();

            if (newSnapshotId < currentMaxSnapshotId) {
                break;
            }

            if (maxSnapshotId.compareAndSet(currentMaxSnapshotId, newSnapshotId)) {
                break;
            }
        }
    }
}
