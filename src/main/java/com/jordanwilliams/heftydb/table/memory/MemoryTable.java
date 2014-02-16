/*
 * Copyright (c) 2014. Jordan Williams
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
import com.jordanwilliams.heftydb.table.MutableTable;
import com.jordanwilliams.heftydb.table.Table;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryTable implements MutableTable {

    private final long id;
    private final SynchronizedTupleMap records = new SynchronizedTupleMap();
    private final AtomicLong maxSnapshotId = new AtomicLong();
    private final AtomicInteger recordCount = new AtomicInteger();
    private final AtomicInteger size = new AtomicInteger();

    public MemoryTable(long id) {
        this.id = id;
    }

    @Override
    public void put(Tuple tuple) {
        records.put(tuple.key(), tuple.value());
        recordCount.incrementAndGet();
        size.addAndGet(tuple.size());
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public boolean mightContain(Key key) {
        return true;
    }

    @Override
    public Tuple get(Key key) {
        return records.get(key);
    }

    @Override
    public Iterator<Tuple> ascendingIterator(long snapshotId) {
        return records.ascendingIterator(snapshotId);
    }

    @Override
    public Iterator<Tuple> descendingIterator(long snapshotId) {
        return records.descendingIterator(snapshotId);
    }

    @Override
    public Iterator<Tuple> ascendingIterator(Key key, long snapshotId) {
        return records.ascendingIterator(key, snapshotId);
    }

    @Override
    public Iterator<Tuple> descendingIterator(Key key, long snapshotId) {
        return records.descendingIterator(key, snapshotId);
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
        return records.iterator();
    }

    @Override
    public int compareTo(Table o) {
        return Long.compare(id, o.id());
    }
}
