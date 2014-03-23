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
import com.googlecode.concurrentlinkedhashmap.Weigher;
import com.jordanwilliams.heftydb.cache.TableBlockCache;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.offheap.SortedByteMap;
import com.jordanwilliams.heftydb.offheap.MemoryPointer;
import com.jordanwilliams.heftydb.offheap.Offheap;
import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A SortedByteMap wrapper that represents a block of IndexRecords. An IndexBlock is a page in the B+tree Index for a
 * database file.
 */
public class IndexBlock implements Iterable<IndexRecord>, Offheap {

    public static class Cache {

        private final TableBlockCache<IndexBlock> cache;

        public Cache(long maxSize, Metrics metrics) {
            cache = new TableBlockCache<IndexBlock>(maxSize, new Weigher<IndexBlock>() {
                @Override
                public int weightOf(IndexBlock indexRecord) {
                    return indexRecord.memory().size();
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

        public void invalidate(long tableId) {
            cache.invalidate(tableId);
        }

        public void clear() {
            cache.clear();
        }
    }

    public static class Builder {

        private final SortedByteMap.Builder byteMapBuilder = new SortedByteMap.Builder();

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

        private final Iterator<SortedByteMap.Entry> entryIterator;

        private RecordIterator(Iterator<SortedByteMap.Entry> entryIterator) {
            this.entryIterator = entryIterator;
        }

        @Override
        public boolean hasNext() {
            return entryIterator.hasNext();
        }

        @Override
        public IndexRecord next() {
            SortedByteMap.Entry nextEntry = entryIterator.next();
            return deserialize(nextEntry);
        }

        @Override
        public void remove() {
            entryIterator.remove();
        }
    }

    private final SortedByteMap sortedByteMap;

    public IndexBlock(SortedByteMap sortedByteMap) {
        this.sortedByteMap = sortedByteMap;
    }

    public IndexRecord startRecord() {
        return deserialize(0);
    }

    public IndexRecord get(Key key) {
        int closestIndex = sortedByteMap.floorIndex(key);

        if (closestIndex < 0) {
            return null;
        }

        if (closestIndex >= sortedByteMap.entryCount()) {
            closestIndex = sortedByteMap.entryCount() - 1;
        }

        return deserialize(closestIndex);
    }

    @Override
    public MemoryPointer memory() {
        return sortedByteMap.memory();
    }

    public Iterator<IndexRecord> ascendingIterator() {
        return new RecordIterator(sortedByteMap.ascendingIterator());
    }

    public Iterator<IndexRecord> ascendingIterator(Key key) {
        return new RecordIterator(sortedByteMap.ascendingIterator(key));
    }

    public Iterator<IndexRecord> descendingIterator() {
        return new RecordIterator(sortedByteMap.descendingIterator());
    }

    public Iterator<IndexRecord> descendingIterator(Key key) {
        return new RecordIterator(sortedByteMap.descendingIterator(key));
    }

    @Override
    public Iterator<IndexRecord> iterator() {
        return new RecordIterator(sortedByteMap.ascendingIterator());
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
        SortedByteMap.Entry entry = sortedByteMap.get(index);
        return deserialize(entry);
    }

    private IndexRecord deserialize(SortedByteMap.Entry entry) {
        ByteBuffer entryValueBuffer = entry.value().data();
        long blockOffset = entryValueBuffer.getLong(0);
        int blockSize = entryValueBuffer.getInt(Sizes.LONG_SIZE);
        boolean isLeaf = entryValueBuffer.get(Sizes.LONG_SIZE + Sizes.INT_SIZE) == (byte) 1;
        return new IndexRecord(entry.key(), blockOffset, blockSize, isLeaf);
    }
}
