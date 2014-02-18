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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
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

    private final com.google.common.cache.Cache<Entry, T> cache;
    private final long maxSize;
    private final AtomicLong totalSize = new AtomicLong();

    public BlockCache(long maxSize, Weigher<Entry, T> weigher) {
        cache = CacheBuilder.newBuilder().concurrencyLevel(CONCURRENCY_LEVEL).weigher(weigher).removalListener(new RemovalListener<Entry, T>() {
            @Override
            public void onRemoval(RemovalNotification<Entry, T> removalNotification) {
                T value = removalNotification.getValue();
                totalSize.addAndGet(value.memory().size() * -1);
                value.memory().release();
            }
        }).maximumWeight(maxSize).build();
        this.maxSize = maxSize;
    }

    public T get(long tableId, long offset) {
        return cache.getIfPresent(new Entry(tableId, offset));
    }

    public void put(long tableId, long offset, T block) {
        cache.put(new Entry(tableId, offset), block);
        totalSize.addAndGet(block.memory().size());
    }

    public long totalEntrySize() {
        return totalSize.get();
    }

    public double utilizationRate() {
        return totalSize.doubleValue() / (double) maxSize;
    }

    public void invalidate(long tableId) {
        for (Entry entry : cache.asMap().keySet()) {
            if (entry.tableId() == tableId) {
                cache.invalidate(entry);
            }
        }
    }

    public void clear() {
        cache.invalidateAll();
    }
}
