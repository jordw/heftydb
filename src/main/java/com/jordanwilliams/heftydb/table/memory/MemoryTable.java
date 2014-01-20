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

import com.jordanwilliams.heftydb.read.VersionedRecordIterator;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.table.MutableTable;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryTable implements MutableTable {

    private final long id;
    private final ConcurrentNavigableMap<Key, Record> records = new ConcurrentSkipListMap<Key, Record>();
    private final AtomicInteger recordCount = new AtomicInteger();
    private final AtomicInteger sizeBytes = new AtomicInteger();

    public MemoryTable(long id) {
        this.id = id;
    }

    @Override
    public void put(Record record) {
        records.put(record.key(), record);
        recordCount.incrementAndGet();
        sizeBytes.addAndGet(record.size());
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
    public Record get(Key key) {
        Map.Entry<Key, Record> closestEntry = records.floorEntry(key);
        return closestEntry == null ? null : closestEntry.getValue();
    }

    @Override
    public Iterator<Record> iterator(IterationDirection direction, long snapshotId) {
        Iterator<Record> sourceIterator = direction.equals(IterationDirection.ASCENDING) ? records.values().iterator() : records.descendingMap().values().iterator();
        return new VersionedRecordIterator(snapshotId, sourceIterator);
    }

    @Override
    public Iterator<Record> iteratorFrom(Key key, IterationDirection direction, long snapshotId) {
        Iterator<Record> sourceIterator = direction.equals(IterationDirection.ASCENDING) ? records.tailMap(key, true).values().iterator() : records.headMap(key, true).descendingMap().values().iterator();

        return new VersionedRecordIterator(snapshotId, sourceIterator);
    }

    @Override
    public long recordCount() {
        return recordCount.get();
    }

    @Override
    public long sizeBytes() {
        return sizeBytes.get();
    }

    @Override
    public int level() {
        return 0;
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public Iterator<Record> iterator() {
        return records.values().iterator();
    }
}
