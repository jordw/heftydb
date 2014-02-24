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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryPointer {

    private final AtomicInteger retainCount = new AtomicInteger(1);
    private final int size;
    private final ByteBuffer directBuffer;

    private long address;

    MemoryPointer(long address, int size, ByteBuffer directBuffer) {
        this.address = address;
        this.size = size;
        this.directBuffer = directBuffer;
        directBuffer.rewind();
    }

    public long address() {
        return address;
    }

    public ByteBuffer directBuffer() {
        return directBuffer;
    }

    public boolean isFree() {
        return address == 0;
    }

    public void free() {
        MemoryAllocator.deallocate(address, size);
        address = 0;
    }

    public int size() {
        return directBuffer.limit();
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
