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

package com.jordanwilliams.heftydb.table.file;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.jordanwilliams.heftydb.offheap.ByteMap;
import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.offheap.Offheap;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.table.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class RecordBlock implements Iterable<Record>, Offheap {

    public static class Cache {

        private final com.google.common.cache.Cache<String, RecordBlock> cache;

        public Cache(long maxsize) {
            cache = CacheBuilder.newBuilder().concurrencyLevel(64).weigher(new Weigher<String, RecordBlock>() {
                @Override
                public int weigh(String key, RecordBlock value) {
                    return key.length() + value.memory().size();
                }
            }).removalListener(new RemovalListener<String, RecordBlock>() {
                @Override
                public void onRemoval(RemovalNotification<String, RecordBlock> removalNotification) {
                    removalNotification.getValue().memory().release();
                }
            }).maximumWeight(maxsize).build();
        }

        public Cache() {
            this(1024000);
        }

        public RecordBlock get(long tableId, long offset) {
            return cache.getIfPresent(key(tableId, offset));
        }

        public void put(long tableId, long offset, RecordBlock recordBlock) {
            cache.put(key(tableId, offset), recordBlock);
        }

        private String key(long tableId, long offset) {
            return new StringBuilder().append(tableId).append(offset).toString();
        }

        public void clear(){
            cache.invalidateAll();
        }
    }

    public static class Builder {

        private final ByteMap.Builder byteMapBuilder = new ByteMap.Builder();
        private int size;

        public void addRecord(Record record) {
            byteMapBuilder.add(new Key(record.key().data(), record.key().snapshotId()), record.value());
            size += record.size();
        }

        public int size() {
            return size;
        }

        public RecordBlock build() {
            return new RecordBlock(byteMapBuilder.build());
        }
    }

    private class BlockIterator implements Iterator<Record> {

        private final boolean ascending;
        private int currentRecordIndex;

        public BlockIterator(int startIndex, Table.IterationDirection iterationDirection) {
            this.currentRecordIndex = startIndex;
            this.ascending = iterationDirection.equals(Table.IterationDirection.ASCENDING);
        }

        @Override
        public boolean hasNext() {
            return ascending ? currentRecordIndex < byteMap.entryCount() : currentRecordIndex > -1;
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Record record = deserializeRecord(currentRecordIndex);
            currentRecordIndex += ascending ? 1 : -1;
            return record;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private final ByteMap byteMap;

    public RecordBlock(ByteMap byteMap) {
        this.byteMap = byteMap;
    }

    public Record get(Key key) {
        int closestIndex = byteMap.floorIndex(key);

        if (closestIndex < 0 || closestIndex >= byteMap.entryCount()) {
            return null;
        }

        Record closestRecord = deserializeRecord(closestIndex);
        return closestRecord.key().data().equals(key.data()) ? closestRecord : null;
    }

    public Record startRecord() {
        return deserializeRecord(0);
    }

    public Iterator<Record> iterator(Table.IterationDirection iterationDirection) {
        int startIndex = iterationDirection.equals(Table.IterationDirection.ASCENDING) ? 0 : byteMap.entryCount() - 1;
        return new BlockIterator(startIndex, iterationDirection);
    }

    public Iterator<Record> iteratorFrom(Key key, Table.IterationDirection iterationDirection) {
        boolean ascending = iterationDirection.equals(Table.IterationDirection.ASCENDING);
        Key versionedKey = new Key(key.data(), ascending ? 0 : Long.MAX_VALUE);
        int startIndex = ascending ? byteMap.ceilingIndex(versionedKey) : byteMap.floorIndex(versionedKey);
        return new BlockIterator(startIndex, iterationDirection);
    }

    @Override
    public Iterator<Record> iterator() {
        return new BlockIterator(0, Table.IterationDirection.ASCENDING);
    }

    @Override
    public Memory memory() {
        return byteMap.memory();
    }

    @Override
    public String toString() {
        List<Record> records = new ArrayList<Record>();
        for (Record record : this) {
            records.add(record);
        }

        return "RecordBlock{records=" + records + "}";
    }

    private Record deserializeRecord(int recordIndex) {
        ByteMap.Entry entry = byteMap.get(recordIndex);
        return new Record(entry.key(), entry.value());
    }
}
