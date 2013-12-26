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

    private static class VersionedKey implements Comparable<VersionedKey> {

        private final Key key;
        private final long snapshotId;

        private VersionedKey(Key key, long snapshotId) {
            this.key = key;
            this.snapshotId = snapshotId;
        }

        @Override
        public int compareTo(VersionedKey o) {
            int compared = key.compareTo(o.key);

            if (compared != 0) {
                return compared;
            }

            if (snapshotId == o.snapshotId) {
                return 0;
            }

            return snapshotId > o.snapshotId ? 1 : -1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VersionedKey that = (VersionedKey) o;

            if (snapshotId != that.snapshotId) return false;
            if (key != null ? !key.equals(that.key) : that.key != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (int) (snapshotId ^ (snapshotId >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "VersionedKey{" +
                    "key=" + key +
                    ", snapshotId=" + snapshotId +
                    '}';
        }
    }

    private final long id;
    private final ConcurrentNavigableMap<VersionedKey, Record> records = new ConcurrentSkipListMap<VersionedKey, Record>();
    private final AtomicInteger recordCount = new AtomicInteger();
    private final AtomicInteger sizeBytes = new AtomicInteger();

    public MemoryTable(long id) {
        this.id = id;
    }

    @Override
    public void put(Record record) {
        records.put(new VersionedKey(record.key(), record.snapshotId()), record);
        recordCount.incrementAndGet();
        sizeBytes.addAndGet(record.key().size() + record.value().size());
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public boolean mightContain(Key key) {
        VersionedKey floorKey = records.floorKey(new VersionedKey(key, Long.MAX_VALUE));
        return floorKey != null && floorKey.key.equals(key);
    }

    @Override
    public Record get(Key key, long snapshotId) {
        Map.Entry<VersionedKey, Record> closestEntry = records.floorEntry(new VersionedKey(key, snapshotId));
        return closestEntry == null ? null : closestEntry.getValue();
    }

    @Override
    public Iterator<Record> iterator(IterationDirection direction, long snapshotId) {
        Iterator<Record> sourceIterator = direction.equals(IterationDirection.ASCENDING) ? records.values().iterator() : records.descendingMap().values().iterator();

        return new VersionedRecordIterator(snapshotId, sourceIterator);
    }

    @Override
    public Iterator<Record> iteratorFrom(Key key, IterationDirection direction, long snapshotId) {
        VersionedKey versionedKey = new VersionedKey(key, snapshotId);
        Iterator<Record> sourceIterator = direction.equals(IterationDirection.ASCENDING) ? records.tailMap(versionedKey, true).values().iterator() : records.headMap(versionedKey).descendingMap().values().iterator();

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
