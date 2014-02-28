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

package com.jordanwilliams.heftydb.cache;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weigher;
import com.jordanwilliams.heftydb.offheap.Offheap;

import java.util.concurrent.atomic.AtomicLong;

public class BlockCache<T extends Offheap> {

    public static class Entry {

        private final Long tableId;
        private final Long offset;

        private Entry(Long tableId, Long offset) {
            this.tableId = tableId;
            this.offset = offset;
        }

        public Long tableId() {
            return tableId;
        }

        public Long offset() {
            return offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (offset != null ? !offset.equals(entry.offset) : entry.offset != null) return false;
            if (tableId != null ? !tableId.equals(entry.tableId) : entry.tableId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = tableId != null ? tableId.hashCode() : 0;
            result = 31 * result + (offset != null ? offset.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "tableId=" + tableId +
                    ", offset=" + offset +
                    '}';
        }
    }

    private static final int CONCURRENCY_LEVEL = 64;

    private final ConcurrentLinkedHashMap<Entry, T> cache;
    private final long maxSize;
    private final AtomicLong totalSize = new AtomicLong();

    public BlockCache(long maxSize, Weigher<T> weigher) {
        cache = new ConcurrentLinkedHashMap.Builder<Entry, T>().concurrencyLevel(CONCURRENCY_LEVEL).weigher(weigher)
                .listener(new EvictionListener<Entry, T>() {
                    @Override
                    public void onEviction(Entry key, T value) {
                        totalSize.addAndGet(value.memory().size() * -1);
                        value.memory().release();
                    }
                }).maximumWeightedCapacity(maxSize).build();
        this.maxSize = maxSize;
    }

    public T get(long tableId, long offset) {
        T block = cache.get((new Entry(tableId, offset)));

        if (block == null) {
            return null;
        }

        if (!block.memory().retain()) {
            return null;
        }

        return block;
    }

    public void put(long tableId, long offset, T block) {
        Entry entry = new Entry(tableId, offset);

        if (!block.memory().retain()){
            return;
        }

        T existingBlock = cache.put(entry, block);

        if (existingBlock != null) {
            existingBlock.memory().release();
        }

        totalSize.addAndGet(block.memory().size());
    }

    public long totalEntrySize() {
        return totalSize.get();
    }

    public double utilizationRate() {
        return totalSize.doubleValue() / (double) maxSize;
    }

    public void invalidate(long tableId) {
        for (Entry entry : cache.keySet()) {
            if (entry.tableId() == tableId) {
                T existingBlock = cache.remove(entry);

                if (existingBlock != null) {
                    existingBlock.memory().release();
                }
            }
        }
    }

    public void clear() {
        cache.clear();
    }
}
