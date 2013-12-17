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

import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Memory {

    private static final Unsafe unsafe = Allocator.unsafe;
    private static final long BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);

    private final AtomicInteger retainCount = new AtomicInteger(1);
    private final long size;
    private long baseAddress;

    private Memory(long bytes) {
        size = bytes;
        baseAddress = Allocator.allocate(size);
    }

    public static Memory allocate(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException();
        }
        return new Memory(bytes);
    }

    public byte getByte(long offset) {
        checkOffset(offset);
        return unsafe.getByte(baseAddress + offset);
    }

    public void getBytes(long offset, ByteBuffer buffer) {
        if (buffer == null) {
            throw new NullPointerException();
        }

        checkOffset(offset);
        long endOffset = offset + buffer.capacity();
        checkOffset(endOffset - 1);

        unsafe.copyMemory(null, baseAddress + offset, buffer.array(), BYTE_ARRAY_BASE_OFFSET, buffer.capacity());
    }

    public void setByte(long offset, byte b) {
        checkOffset(offset);
        unsafe.putByte(baseAddress + offset, b);
    }

    public void setBytes(long memoryOffset, ByteBuffer buffer) {
        if (buffer == null) {
            throw new NullPointerException();
        }

        checkOffset(memoryOffset);
        long endOffset = memoryOffset + buffer.capacity();
        checkOffset(endOffset - 1);

        unsafe.copyMemory(buffer.array(), BYTE_ARRAY_BASE_OFFSET, null, baseAddress + memoryOffset, buffer.capacity());
    }

    public int getInt(long offset) {
        checkOffset(offset);
        return unsafe.getInt(baseAddress + offset);
    }

    public void setInt(long offset, int l) {
        checkOffset(offset);
        unsafe.putInt(baseAddress + offset, l);
    }

    public long getLong(long offset) {
        checkOffset(offset);
        return unsafe.getLong(baseAddress + offset);
    }

    public void setLong(long offset, long l) {
        checkOffset(offset);
        unsafe.putLong(baseAddress + offset, l);
    }

    public boolean isFree() {
        return baseAddress == 0;
    }

    public void free() {
        Allocator.free(baseAddress);
        baseAddress = 0;
    }

    public long size() {
        return size;
    }

    public boolean retain() {
        while (true) {
            int value = retainCount.get();

            if (value <= 0) {
                return false;
            }

            if (retainCount.compareAndSet(value, value + 1)) {
                return true;
            }
        }
    }

    public void release() {
        if (retainCount.decrementAndGet() == 0) {
            free();
        }
    }

    private void checkOffset(long offset) {
        assert baseAddress != 0 : "Memory was already freed";
        assert offset >= 0 && offset < size : "Illegal address: " + offset + ", size: " + size;
    }
}
