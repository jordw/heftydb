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

    private Memory(int sizeBytes){
        this.baseAddress = allocator.allocate(sizeBytes);
        this.size = sizeBytes;
        this.directBuffer = rawDirectBuffer(baseAddress, sizeBytes);
    }

    public static Memory allocate(int sizeBytes) {
        if (sizeBytes < 0) {
            throw new IllegalArgumentException();
        }

        return new Memory(sizeBytes);
    }

    public ByteBuffer directBuffer(){
        return directBuffer;
    }

    public boolean isFree(){
        return baseAddress == 0;
    }

    public void free() {
        allocator.free(baseAddress);
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

    private static ByteBuffer rawDirectBuffer(long address, int sizeBytes) {
        try {
            return (ByteBuffer) directBufferConstructor.newInstance(address, sizeBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
