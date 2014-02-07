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

import com.jordanwilliams.heftydb.util.Sizes;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Memory {

    private static final Allocator allocator = Allocator.allocator;
    private static final Constructor directBufferConstructor;

    static {
        try {
            Constructor[] constructors = Class.forName("java.nio.DirectByteBuffer").getDeclaredConstructors();
            directBufferConstructor = constructors[0];
            directBufferConstructor.setAccessible(true);
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

        //Zero out memory
        for (int i = 0; i < directBuffer.capacity() / Sizes.LONG_SIZE; i++) {
            directBuffer.putLong(0L);
        }

        directBuffer.rewind();
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

    public static Memory allocate(int size, int align) {
        return allocate(pageAlignedSize(size, align));
    }

    private static ByteBuffer rawDirectBuffer(long address, int size) {
        try {
            return (ByteBuffer) directBufferConstructor.newInstance(address, size);
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
