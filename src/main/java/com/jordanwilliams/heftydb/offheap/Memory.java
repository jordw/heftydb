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

import com.jordanwilliams.heftydb.util.ByteBuffers;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.Buffer;
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

    private Memory(long baseAddress, long size) {
        this.baseAddress = baseAddress;
        this.size = size;
    }

    public byte getByte(long offset) {
        checkOffset(offset);
        return unsafe.getByte(baseAddress + offset);
    }

    public void getBytes(long memoryOffset, ByteBuffer buffer, int bufferPosition, int length) {
        if (buffer == null) {
            throw new NullPointerException();
        }

        checkOffset(memoryOffset);
        long endOffset = memoryOffset + length;
        checkOffset(endOffset - 1);


        if (buffer instanceof DirectBuffer) {
            unsafe.copyMemory(baseAddress + memoryOffset, ((DirectBuffer) buffer).address(), length);
        } else {
            unsafe.copyMemory(null, baseAddress + memoryOffset, buffer.array(), BYTE_ARRAY_BASE_OFFSET + bufferPosition, length);
        }
    }

    public void getBytes(long memoryOffset, ByteBuffer buffer) {
        getBytes(memoryOffset, buffer, 0, buffer.capacity());
    }

    public void getBytes(long memoryOffset, Memory memory, long fromOffset, long length){
        checkOffset(memoryOffset);
        long endOffset = memoryOffset + length;
        checkOffset(endOffset - 1);

        unsafe.copyMemory(baseAddress + memoryOffset, memory.baseAddress + fromOffset, length);
    }

    public void getBytes(long memoryOffset, Memory memory){
        getBytes(memoryOffset, memory, 0, memory.size);
    }

    public void putByte(long offset, byte b) {
        checkOffset(offset);
        unsafe.putByte(baseAddress + offset, b);
    }

    public void putBytes(long memoryOffset, ByteBuffer buffer, int bufferPosition, int length) {
        if (buffer == null) {
            throw new NullPointerException();
        }

        checkOffset(memoryOffset);
        long endOffset = memoryOffset + length;
        checkOffset(endOffset - 1);

        if (buffer instanceof DirectBuffer) {
            unsafe.copyMemory(((DirectBuffer) buffer).address() + bufferPosition, baseAddress + memoryOffset, length);
        } else {
            unsafe.copyMemory(buffer.array(), BYTE_ARRAY_BASE_OFFSET + bufferPosition, null, baseAddress + memoryOffset, length);
        }
    }

    public void putBytes(long memoryOffset, ByteBuffer buffer) {
        putBytes(memoryOffset, buffer, 0, buffer.capacity());
    }

    public void putBytes(long memoryOffset, Memory memory, long fromOffset, long length){
        checkOffset(memoryOffset);
        long endOffset = memoryOffset + length;
        checkOffset(endOffset - 1);

        unsafe.copyMemory(memory.baseAddress + fromOffset, baseAddress + memoryOffset, length);
    }

    public void putBytes(long memoryOffset, Memory memory){
        putBytes(memoryOffset, memory, 0, memory.size);
    }

    public int getInt(long offset) {
        checkOffset(offset);
        return unsafe.getInt(baseAddress + offset);
    }

    public void putInt(long offset, int l) {
        checkOffset(offset);
        unsafe.putInt(baseAddress + offset, l);
    }

    public long getLong(long offset) {
        checkOffset(offset);
        return unsafe.getLong(baseAddress + offset);
    }

    public void putLong(long offset, long l) {
        checkOffset(offset);
        unsafe.putLong(baseAddress + offset, l);
    }

    public boolean isFree() {
        return baseAddress == 0;
    }

    public void free() {
        assert baseAddress != 0 : "Memory was already freed";
        Allocator.free(baseAddress);
        baseAddress = 0;
    }

    public long size() {
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

    public ByteBuffer toDirectBuffer(long offset, int length) {
        if (size >= Integer.MAX_VALUE) {
            throw new IndexOutOfBoundsException();
        }

        return ByteBuffers.rawDirectBuffer(baseAddress + offset, length);
    }

    public ByteBuffer toDirectBuffer(){
        return toDirectBuffer(0, (int) size);
    }

    public Memory copy(long memoryOffset, long length) {
        checkOffset(memoryOffset);
        long endOffset = memoryOffset + length;
        checkOffset(endOffset - 1);

        Memory memory = Memory.allocate(length);

        for (long i = 0; i < length; i++) {
            memory.putByte(i, memory.getByte(i));
        }

        return memory;
    }

    public Memory copy() {
        return copy(0, size);
    }

    public int compareAsBytes(ByteBuffer compare, long memoryOffset, int length) {
        long compareCount = Math.min(length, compare.remaining());
        long compareOffset = memoryOffset + compareCount;
        int remaining = length;

        for (long i = memoryOffset; i < compareOffset; i++) {
            byte thisVal = this.getByte(i);
            byte thatVal = compare.get();
            remaining--;

            if (thisVal == thatVal) {
                continue;
            }

            if (thisVal < thatVal) {
                return -1;
            }

            return 1;
        }

        return remaining - compare.remaining();
    }

    private void checkOffset(long offset) {
        assert baseAddress != 0 : "Memory was already freed";
        assert offset >= 0 && offset < size : "Illegal address: " + offset + ", size: " + size;
    }

    public static Memory allocate(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException();
        }
        return new Memory(bytes);
    }

    public static Memory fromDirectBuffer(ByteBuffer buffer) {
        if (!(buffer instanceof DirectBuffer)) {
            throw new IllegalArgumentException();
        }

        //Capacity
        int capacity = buffer.capacity();

        //Base Address
        DirectBuffer directBuffer = (DirectBuffer) buffer;
        long address = directBuffer.address();

        return new Memory(address, capacity);
    }

    public static Memory copyFromByteBuffer(ByteBuffer buffer) {
        Buffer fromBuffer = buffer.duplicate();
        fromBuffer.rewind();
        Memory memory = Memory.allocate(buffer.capacity());
        memory.putBytes(0, buffer);
        return memory;
    }
}
