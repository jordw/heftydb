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
import com.jordanwilliams.heftydb.record.Value;
import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IndexBlock implements Iterable<IndexRecord>, Offheap {

    public static class Cache {

        private final com.google.common.cache.Cache<String, IndexBlock> cache;

        public Cache(long maxsize) {
            cache = CacheBuilder.newBuilder().concurrencyLevel(64).weigher(new Weigher<String, IndexBlock>() {
                @Override
                public int weigh(String key, IndexBlock value) {
                    return key.length() + value.memory().size();
                }
            }).removalListener(new RemovalListener<String, IndexBlock>() {
                @Override
                public void onRemoval(RemovalNotification<String, IndexBlock> removalNotification) {
                    removalNotification.getValue().memory().release();
                }
            }).maximumWeight(maxsize).build();
        }

        public Cache() {
            this(1024000);
        }

        public IndexBlock get(long tableId, long offset) {
            return cache.getIfPresent(key(tableId, offset));
        }

        public void put(long tableId, long offset, IndexBlock indexBlock) {
            cache.put(key(tableId, offset), indexBlock);
        }

        private String key(long tableId, long offset) {
            return new StringBuilder().append(tableId).append(offset).toString();
        }

        public void clear() {
            cache.invalidateAll();
        }
    }

    public static class Builder {

        private final ByteMap.Builder byteMapBuilder = new ByteMap.Builder();

        private int size;

        public void addRecord(IndexRecord indexRecord) {
            byteMapBuilder.add(indexRecord.startKey(), new Value(indexRecordValue(indexRecord)));
            size += indexRecord.size();
        }

        public int size() {
            return size;
        }

        public IndexBlock build() {
            return new IndexBlock(byteMapBuilder.build());
        }

        private ByteBuffer indexRecordValue(IndexRecord indexRecord) {
            ByteBuffer contentsBuffer = ByteBuffer.allocate(indexRecord.contentsSize());
            contentsBuffer.putLong(indexRecord.blockOffset());
            contentsBuffer.putInt(indexRecord.blockSize());
            contentsBuffer.put(indexRecord.isLeaf() ? (byte) 1 : (byte) 0);
            contentsBuffer.rewind();
            return contentsBuffer;
        }
    }

    private class RecordIterator implements Iterator<IndexRecord> {

        private final Iterator<ByteMap.Entry> entryIterator;

        private RecordIterator(Iterator<ByteMap.Entry> entryIterator) {
            this.entryIterator = entryIterator;
        }

        @Override
        public boolean hasNext() {
            return entryIterator.hasNext();
        }

        @Override
        public IndexRecord next() {
            ByteMap.Entry nextEntry = entryIterator.next();
            return deserializeRecord(nextEntry);
        }

        @Override
        public void remove() {
            entryIterator.remove();
        }
    }

    private final ByteMap byteMap;

    public IndexBlock(ByteMap byteMap) {
        this.byteMap = byteMap;
    }

    public IndexRecord startRecord() {
        return deserializeRecord(0);
    }

    public IndexRecord get(Key key) {
        int closestIndex = byteMap.floorIndex(key);

        if (closestIndex < 0) {
            closestIndex = 0;
        }

        if (closestIndex >= byteMap.entryCount()) {
            closestIndex = byteMap.entryCount() - 1;
        }

        return deserializeRecord(closestIndex);
    }

    @Override
    public Memory memory() {
        return byteMap.memory();
    }

    public Iterator<IndexRecord> ascendingIterator() {
        return new RecordIterator(byteMap.ascendingIterator());
    }

    public Iterator<IndexRecord> ascendingIterator(Key key) {
        return new RecordIterator(byteMap.ascendingIterator(key));
    }

    public Iterator<IndexRecord> descendingIterator() {
        return new RecordIterator(byteMap.descendingIterator());
    }

    public Iterator<IndexRecord> descendingIterator(Key key) {
        return new RecordIterator(byteMap.descendingIterator(key));
    }

    @Override
    public Iterator<IndexRecord> iterator() {
        return new RecordIterator(byteMap.ascendingIterator());
    }

    @Override
    public String toString() {
        List<IndexRecord> records = new ArrayList<IndexRecord>();

        for (IndexRecord indexRecord : this) {
            records.add(indexRecord);
        }

        return "IndexBlock{records=" + records + "}";
    }

    private IndexRecord deserializeRecord(int recordIndex) {
        ByteMap.Entry entry = byteMap.get(recordIndex);
        return deserializeRecord(entry);
    }

    private IndexRecord deserializeRecord(ByteMap.Entry entry) {
        ByteBuffer entryValueBuffer = entry.value().data();
        long blockOffset = entryValueBuffer.getLong(0);
        int blockSize = entryValueBuffer.getInt(Sizes.LONG_SIZE);
        boolean isLeaf = entryValueBuffer.get(Sizes.LONG_SIZE + Sizes.INT_SIZE) == (byte) 1;
        return new IndexRecord(entry.key(), blockOffset, blockSize, isLeaf);
    }
}
