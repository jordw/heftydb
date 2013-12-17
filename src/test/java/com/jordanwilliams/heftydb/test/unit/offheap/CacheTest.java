package com.jordanwilliams.heftydb.test.unit.offheap;


import com.jordanwilliams.heftydb.util.ByteBuffers;
import com.jordanwilliams.heftydb.util.Serializer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class CacheTest {

    private static final ByteBuffer TEST_KEY = ByteBuffers.fromString("test");
    private static final ByteBuffer TEST_VALUE = ByteBuffers.fromString("test1");
    private static final ByteBuffer TEST_VALUE2 = ByteBuffers.fromString("test2");

    private final Cache.KeyWeigher<ByteBuffer> weigher = new Cache.KeyWeigher<ByteBuffer>() {
        @Override
        public int weigh(ByteBuffer key) {
            return key.array().length;
        }
    };

    private final Serializer.OffHeapSerializer<ByteBuffer> serializer = new Serializer.OffHeapSerializer<ByteBuffer>() {
        @Override
        public Memory serialize(ByteBuffer data) {
            Memory memory = Memory.allocate(serializedSize(data));
            memory.setBytes(0, data);
            return memory;
        }

        @Override
        public ByteBuffer deserialize(Memory in) {
            ByteBuffer buffer = ByteBuffer.allocate((int) in.size());
            in.getBytes(0, buffer);
            buffer.rewind();
            return buffer;
        }

        @Override
        public long serializedSize(ByteBuffer data) {
            return data.array().length;
        }
    };

    @Test
    public void getPutTest() {
        Cache<ByteBuffer, ByteBuffer> testCache = new Cache<ByteBuffer, ByteBuffer>(serializer, 1024, weigher);
        testCache.put(TEST_KEY, TEST_VALUE);
        Assert.assertEquals("Cache values match", TEST_VALUE, testCache.get(TEST_KEY));
    }

    @Test
    public void putIfAbsentTest() {
        Cache<ByteBuffer, ByteBuffer> testCache = new Cache<ByteBuffer, ByteBuffer>(serializer, 1024, weigher);
        Assert.assertNull("Value does not exist", testCache.putIfAbsent(TEST_KEY, TEST_VALUE));
    }

    @Test
    public void putIfAbsentPresentTest() {
        Cache<ByteBuffer, ByteBuffer> testCache = new Cache<ByteBuffer, ByteBuffer>(serializer, 1024, weigher);
        testCache.putIfAbsent(TEST_KEY, TEST_VALUE);
        Assert.assertEquals("Value already exists", TEST_VALUE, testCache.putIfAbsent(TEST_KEY, TEST_VALUE));
    }

    @Test
    public void replaceSuccessTest() {
        Cache<ByteBuffer, ByteBuffer> testCache = new Cache<ByteBuffer, ByteBuffer>(serializer, 1024, weigher);
        testCache.put(TEST_KEY, TEST_VALUE);
        Assert.assertTrue("Replace success", testCache.replace(TEST_KEY, TEST_VALUE, TEST_VALUE2));
    }

    @Test
    public void replaceFailureTest() {
        Cache<ByteBuffer, ByteBuffer> testCache = new Cache<ByteBuffer, ByteBuffer>(serializer, 1024, weigher);
        testCache.put(TEST_KEY, TEST_VALUE);
        testCache.put(TEST_KEY, TEST_VALUE2);
        Assert.assertFalse("Replace failure", testCache.replace(TEST_KEY, TEST_VALUE, TEST_VALUE2));
    }
}
