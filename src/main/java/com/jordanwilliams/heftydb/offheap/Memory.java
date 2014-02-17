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

package com.jordanwilliams.heftydb.offheap;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.jordanwilliams.heftydb.offheap.allocator.Allocator;
import com.jordanwilliams.heftydb.offheap.allocator.UnsafeAllocator;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Memory {

    private static final Unsafe unsafe = UnsafeAllocator.unsafe;
    private static final Allocator allocator = Allocator.allocator;
    private static final Class<?> directByteBufferClass;
    private static final long addressOffset;
    private static final long capacityOffset;
    private static final long limitOffset;

    private static final MetricRegistry metrics = new MetricRegistry();
    private static final JmxReporter jmxReporter;

    static {
        try {
            Class<?> bufferClass = Class.forName("java.nio.Buffer");
            Field address = bufferClass.getDeclaredField("address");
            Field capacity = bufferClass.getDeclaredField("capacity");
            Field limit = bufferClass.getDeclaredField("limit");

            addressOffset = unsafe.objectFieldOffset(address);
            capacityOffset = unsafe.objectFieldOffset(capacity);
            limitOffset = unsafe.objectFieldOffset(limit);
            directByteBufferClass = Class.forName("java.nio.DirectByteBuffer");

            //Metrics
            metrics.counter("offHeapMemory");
            jmxReporter = JmxReporter.forRegistry(metrics).inDomain("HeftyDB").build();
            jmxReporter.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private final AtomicInteger retainCount = new AtomicInteger(1);
    private final int size;
    private final ByteBuffer directBuffer;

    private long baseAddress;

    private Memory(int size) {
        this.baseAddress = allocator.allocate(size);
        this.size = size;
        this.directBuffer = rawDirectBuffer(baseAddress, size);
        directBuffer.rewind();

        metrics.counter("offHeapMemory").inc(size);
    }

    public ByteBuffer directBuffer() {
        return directBuffer;
    }

    public boolean isFree() {
        return baseAddress == 0;
    }

    public void free() {
        allocator.release(baseAddress);
        baseAddress = 0;
        metrics.counter("offHeapMemory").dec(size);
    }

    public int size() {
        return size;
    }

    public boolean retain() {
        while (true) {
            int retainValue = retainCount.get();

            if (retainValue <= 0) {
                return false;
            }

            if (retainCount.compareAndSet(retainValue, retainValue + 1)) {
                return true;
            }
        }
    }

    public void release() {
        if (retainCount.decrementAndGet() == 0) {
            free();
        }
    }

    @Override
    public String toString() {
        return toHexString(directBuffer);
    }

    public static Memory allocate(int size) {
        if (size < 0) {
            throw new IllegalArgumentException();
        }

        return new Memory(size);
    }

    public static Memory allocateAndZero(int size) {
        Memory memory = allocate(size);
        zeroMemory(memory);
        return memory;
    }

    public static Memory allocate(int size, int align) {
        return allocate(pageAlignedSize(size, align));
    }

    public static Memory allocateAndZero(int size, int align) {
        Memory memory = allocate(size, align);
        zeroMemory(memory);
        return memory;
    }

    private static ByteBuffer rawDirectBuffer(long address, int size) {
        try {
            ByteBuffer newBuffer = (ByteBuffer) unsafe.allocateInstance(directByteBufferClass);
            unsafe.putLong(newBuffer, addressOffset, address);
            unsafe.putInt(newBuffer, capacityOffset, size);
            unsafe.putInt(newBuffer, limitOffset, size);
            return newBuffer;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static int pageAlignedSize(int memorySize, int pageSize) {
        int pageCount = memorySize / pageSize;

        if (memorySize % pageSize != 0) {
            pageCount++;
        }

        return pageCount * pageSize;
    }

    private static void zeroMemory(Memory memory) {
        unsafe.setMemory(memory.baseAddress, memory.size, (byte) 0);
    }

    private static char[] hexArray = "0123456789ABCDEF".toCharArray();

    private static String toHexString(ByteBuffer byteBuffer) {
        char[] hexChars = new char[byteBuffer.capacity() * 2];
        for (int i = 0; i < byteBuffer.capacity(); i++) {
            int v = byteBuffer.get(i) & 0xFF;
            hexChars[i * 2] = hexArray[v >>> 4];
            hexChars[i * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
}
