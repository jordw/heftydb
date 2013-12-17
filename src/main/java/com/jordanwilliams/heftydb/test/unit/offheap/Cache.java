package com.jordanwilliams.heftydb.test.unit.offheap;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EntryWeigher;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.jordanwilliams.heftydb.util.Serializer;

public class Cache<K, V> {

    public interface KeyWeigher<K> {
        public int weigh(K key);
    }

    private static final int CACHE_CONCURRENCY_LEVEL = 64;

    private final Serializer.OffHeapSerializer<V> serializer;
    private final ConcurrentLinkedHashMap<K, Memory> cache;

    public Cache(Serializer.OffHeapSerializer<V> serializer, long capacityBytes, final KeyWeigher<K> keyWeigher) {
        this.serializer = serializer;
        this.cache = new ConcurrentLinkedHashMap.Builder().concurrencyLevel(CACHE_CONCURRENCY_LEVEL).maximumWeightedCapacity(capacityBytes).weigher(new EntryWeigher<K, Memory>() {
            @Override
            public int weightOf(K key, Memory value) {
                return (int) (keyWeigher.weigh(key) + value.size());
            }
        }).listener(new EvictionListener<K, Memory>() {
            @Override
            public void onEviction(K key, Memory value) {
                if (value != null) {
                    value.release();
                }
            }
        }).build();
    }

    public V get(K key) {
        Memory serializedValue = cache.get(key);
        if (serializedValue == null || !serializedValue.retain()) {
            return null;
        }

        try {
            return serializer.deserialize(serializedValue);
        } finally {
            serializedValue.release();
        }
    }

    public void put(K key, V value) {
        Memory serializedValue = serializer.serialize(value);
        Memory oldValue = cache.put(key, serializedValue);

        if (oldValue != null) {
            oldValue.release();
        }
    }

    public V putIfAbsent(K key, V value) {
        Memory serializedValue = serializer.serialize(value);
        if (serializedValue == null) {
            return null;
        }

        Memory oldSerializedValue = cache.putIfAbsent(key, serializedValue);
        if (oldSerializedValue != null) {
            serializedValue.release();
        }

        return oldSerializedValue == null ? null : serializer.deserialize(oldSerializedValue);
    }

    public boolean replace(K key, V oldValue, V newValue) {
        Memory oldSerializedValue = cache.get(key);
        if (oldSerializedValue == null) {
            return false;
        }

        Memory serializedValue = serializer.serialize(newValue);
        if (serializedValue == null) {
            return false;
        }

        V oldCacheValue;

        if (!oldSerializedValue.retain()) {
            return false;
        }

        try {
            oldCacheValue = serializer.deserialize(oldSerializedValue);
        } finally {
            oldSerializedValue.release();
        }

        boolean replaced = oldCacheValue.equals(oldValue) && cache.replace(key, oldSerializedValue, serializedValue);

        if (replaced) {
            oldSerializedValue.release();
        } else {
            serializedValue.release();
        }

        return replaced;
    }

    public void remove(K key) {
        Memory oldValue = cache.remove(key);
        if (oldValue != null) {
            oldValue.release();
        }
    }

    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }
}
