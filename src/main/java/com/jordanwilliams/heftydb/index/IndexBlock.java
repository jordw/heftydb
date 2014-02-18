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

package com.jordanwilliams.heftydb.index;

import com.codahale.metrics.Gauge;
import com.google.common.cache.Weigher;
import com.jordanwilliams.heftydb.cache.BlockCache;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.offheap.ByteMap;
import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.offheap.Offheap;
import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IndexBlock implements Iterable<IndexRecord>, Offheap {

    public static class Cache {

        private final BlockCache<IndexBlock> cache;

        public Cache(long maxSize, Metrics metrics) {
            cache = new BlockCache<IndexBlock>(maxSize, new Weigher<BlockCache.Entry,
                    IndexBlock>() {
                @Override
                public int weigh(BlockCache.Entry entry, IndexBlock value) {
                    return value.memory().size();
                }
            });

            metrics.gauge("cache.indexBlock.entrySize", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return cache.totalEntrySize();
                }
            });

            metrics.gauge("cache.indexBlock.utilizationRate", new Gauge<Double>() {
                @Override
                public Double getValue() {
                    return cache.utilizationRate();
                }
            });
        }

        public IndexBlock get(long tableId, long offset) {
            return cache.get(tableId, offset);
        }

        public void put(long tableId, long offset, IndexBlock tupleBlock) {
            cache.put(tableId, offset, tupleBlock);
        }

        public void invalidate(long tableId){
            cache.invalidate(tableId);
        }

        public void clear() {
            cache.clear();
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
            return deserialize(nextEntry);
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
        return deserialize(0);
    }

    public IndexRecord get(Key key) {
        int closestIndex = byteMap.floorIndex(key);

        if (closestIndex < 0) {
            return null;
        }

        if (closestIndex >= byteMap.entryCount()) {
            closestIndex = byteMap.entryCount() - 1;
        }

        return deserialize(closestIndex);
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

    private IndexRecord deserialize(int index) {
        ByteMap.Entry entry = byteMap.get(index);
        return deserialize(entry);
    }

    private IndexRecord deserialize(ByteMap.Entry entry) {
        ByteBuffer entryValueBuffer = entry.value().data();
        long blockOffset = entryValueBuffer.getLong(0);
        int blockSize = entryValueBuffer.getInt(Sizes.LONG_SIZE);
        boolean isLeaf = entryValueBuffer.get(Sizes.LONG_SIZE + Sizes.INT_SIZE) == (byte) 1;
        return new IndexRecord(entry.key(), blockOffset, blockSize, isLeaf);
    }
}
