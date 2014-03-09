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

import com.codahale.metrics.Counter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.jordanwilliams.heftydb.offheap.allocator.Allocator;
import com.jordanwilliams.heftydb.offheap.allocator.UnsafeAllocator;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MemoryAllocator {

    private static final Unsafe unsafe = JVMUnsafe.unsafe;
    private static final Allocator allocator = new UnsafeAllocator();
    private static final Class<?> directByteBufferClass;
    private static final long addressOffset;
    private static final long capacityOffset;
    private static final long limitOffset;

    private static final MetricRegistry metrics = new MetricRegistry();
    private static final JmxReporter jmxReporter;
    private static final Counter offHeapMemoryCounter;

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
            offHeapMemoryCounter = metrics.counter("offHeapMemory");
            jmxReporter = JmxReporter.forRegistry(metrics).inDomain("HeftyDB").build();
            jmxReporter.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static MemoryPointer wrap(long address, int size) {
        return new MemoryPointer(address, size, rawDirectBuffer(address, size));
    }

    public static MemoryPointer allocate(int size) {
        if (size < 0) {
            throw new IllegalArgumentException();
        }

        long address = allocator.allocate(size);
        offHeapMemoryCounter.inc(size);
        return new MemoryPointer(address, size, rawDirectBuffer(address, size));
    }

    public static MemoryPointer allocateAndZero(int size) {
        MemoryPointer pointer = allocate(size);
        zeroMemory(pointer);
        return pointer;
    }

    public static MemoryPointer allocate(int size, int align) {
        return allocate(pageAlignedSize(size, align));
    }

    public static MemoryPointer allocateAndZero(int size, int align) {
        MemoryPointer pointer = allocate(size, align);
        zeroMemory(pointer);
        return pointer;
    }

    public static void deallocate(long address, int size) {
        allocator.deallocate(address);
        offHeapMemoryCounter.dec(size);
    }

    private static void zeroMemory(MemoryPointer pointer) {
        unsafe.setMemory(pointer.address(), pointer.size(), (byte) 0);
    }

    private static int pageAlignedSize(int memorySize, int pageSize) {
        int pageCount = memorySize / pageSize;

        if (memorySize % pageSize != 0) {
            pageCount++;
        }

        return pageCount * pageSize;
    }

    private static ByteBuffer rawDirectBuffer(long address, int size) {
        try {
            ByteBuffer newBuffer = (ByteBuffer) unsafe.allocateInstance(directByteBufferClass);
            unsafe.putLong(newBuffer, addressOffset, address);
            unsafe.putInt(newBuffer, capacityOffset, size);
            unsafe.putInt(newBuffer, limitOffset, size);
            newBuffer.order(ByteOrder.nativeOrder());
            return newBuffer;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
