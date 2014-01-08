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

package com.jordanwilliams.heftydb.offheap;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EntryWeigher;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Value;

import java.nio.ByteBuffer;

public class ByteCache {

    private static final int CACHE_CONCURRENCY_LEVEL = 64;

    private final ConcurrentLinkedHashMap<Key, Memory> cache;

    public ByteCache(long capacityBytes) {
        this.cache = new ConcurrentLinkedHashMap.Builder().concurrencyLevel(CACHE_CONCURRENCY_LEVEL)
                .maximumWeightedCapacity(capacityBytes).weigher(new EntryWeigher<ByteBuffer, Memory>() {
            @Override
            public int weightOf(ByteBuffer key, Memory value) {
                return key.capacity() + (int) value.size();
            }
        }).listener(new EvictionListener<ByteBuffer, Memory>() {
            @Override
            public void onEviction(ByteBuffer key, Memory value) {
                if (value != null) {
                    value.release();
                }
            }
        }).build();
    }

    public ByteBuffer get(Key key) {
        Memory serializedValue = cache.get(key);
        if (serializedValue == null || !serializedValue.retain()) {
            return null;
        }

        try {
            return serializedValue.directBuffer();
        } finally {
            serializedValue.release();
        }
    }

    public void put(Key key, Value value) {
        Memory serializedValue = Memory.allocate(value.size());
        serializedValue.directBuffer().put(value.data());
        Memory oldValue = cache.put(key, serializedValue);

        if (oldValue != null) {
            oldValue.release();
        }
    }

    public void remove(Key key) {
        Memory oldValue = cache.remove(key);
        if (oldValue != null) {
            oldValue.release();
        }
    }

    public boolean containsKey(Key key) {
        return cache.containsKey(key);
    }
}
