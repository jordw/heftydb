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

package com.jordanwilliams.heftydb.table.file;

import com.codahale.metrics.Gauge;
import com.googlecode.concurrentlinkedhashmap.Weigher;
import com.jordanwilliams.heftydb.cache.TableBlockCache;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.offheap.SortedByteMap;
import com.jordanwilliams.heftydb.offheap.MemoryPointer;
import com.jordanwilliams.heftydb.offheap.Offheap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A SortedByteMap wrapper
 */
public class TupleBlock implements Iterable<Tuple>, Offheap {

    public static class Cache {

        private final TableBlockCache<TupleBlock> cache;

        public Cache(long maxSize, Metrics metrics) {
            cache = new TableBlockCache<TupleBlock>(maxSize, new Weigher<TupleBlock>() {
                @Override
                public int weightOf(TupleBlock tuple) {
                    return tuple.memory().size();
                }
            });

            metrics.gauge("cache.tupleBlock.entrySize", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return cache.totalEntrySize();
                }
            });

            metrics.gauge("cache.tupleBlock.utilizationRate", new Gauge<Double>() {
                @Override
                public Double getValue() {
                    return cache.utilizationRate();
                }
            });
        }

        public TupleBlock get(long tableId, long offset) {
            return cache.get(tableId, offset);
        }

        public void put(long tableId, long offset, TupleBlock tupleBlock) {
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

        public void addRecord(Tuple tuple) {
            byteMapBuilder.add(new Key(tuple.key().data(), tuple.key().snapshotId()), tuple.value());
            size += tuple.size();
        }

        public int size() {
            return size;
        }

        public TupleBlock build() {
            return new TupleBlock(byteMapBuilder.build());
        }
    }

    private class TupleIterator implements Iterator<Tuple> {

        private final Iterator<SortedByteMap.Entry> entryIterator;

        private TupleIterator(Iterator<SortedByteMap.Entry> entryIterator) {
            this.entryIterator = entryIterator;
        }

        @Override
        public boolean hasNext() {
            return entryIterator.hasNext();
        }

        @Override
        public Tuple next() {
            SortedByteMap.Entry nextEntry = entryIterator.next();
            return new Tuple(nextEntry.key(), nextEntry.value());
        }

        @Override
        public void remove() {
            entryIterator.remove();
        }
    }

    private final SortedByteMap sortedByteMap;

    public TupleBlock(SortedByteMap sortedByteMap) {
        this.sortedByteMap = sortedByteMap;
    }

    public Tuple get(Key key) {
        int closestIndex = sortedByteMap.floorIndex(key);

        if (closestIndex < 0 || closestIndex >= sortedByteMap.entryCount()) {
            return null;
        }

        Tuple closestTuple = deserialize(closestIndex);
        return closestTuple.key().data().equals(key.data()) ? closestTuple : null;
    }

    public Tuple first() {
        return deserialize(0);
    }

    public Iterator<Tuple> ascendingIterator() {
        return new TupleIterator(sortedByteMap.ascendingIterator());
    }

    public Iterator<Tuple> ascendingIterator(Key key) {
        return new TupleIterator(sortedByteMap.ascendingIterator(key));
    }

    public Iterator<Tuple> descendingIterator() {
        return new TupleIterator(sortedByteMap.descendingIterator());
    }

    public Iterator<Tuple> descendingIterator(Key key) {
        return new TupleIterator(sortedByteMap.descendingIterator(key));
    }

    @Override
    public Iterator<Tuple> iterator() {
        return new TupleIterator(sortedByteMap.ascendingIterator());
    }

    @Override
    public MemoryPointer memory() {
        return sortedByteMap.memory();
    }

    @Override
    public String toString() {
        List<Tuple> tuples = new ArrayList<Tuple>();
        for (Tuple tuple : this) {
            tuples.add(tuple);
        }

        return "TupleBlock{tuples=" + tuples + "}";
    }

    private Tuple deserialize(int index) {
        SortedByteMap.Entry entry = sortedByteMap.get(index);
        return new Tuple(entry.key(), entry.value());
    }
}
