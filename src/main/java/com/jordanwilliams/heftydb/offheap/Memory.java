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
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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


        if (buffer instanceof DirectBuffer){
            unsafe.copyMemory(baseAddress + memoryOffset, ((DirectBuffer) buffer).address(), length);
        } else {
            unsafe.copyMemory(null, baseAddress + memoryOffset, buffer.array(), BYTE_ARRAY_BASE_OFFSET + bufferPosition, length);
        }
    }

    public void getBytes(long memoryOffset, ByteBuffer buffer) {
        getBytes(memoryOffset, buffer, 0, buffer.capacity());
    }

    public void setByte(long offset, byte b) {
        checkOffset(offset);
        unsafe.putByte(baseAddress + offset, b);
    }

    public void setBytes(long memoryOffset, ByteBuffer buffer, int bufferPosition, int length) {
        if (buffer == null) {
            throw new NullPointerException();
        }

        checkOffset(memoryOffset);
        long endOffset = memoryOffset + length;
        checkOffset(endOffset - 1);

        if (buffer instanceof DirectBuffer){
            unsafe.copyMemory(((DirectBuffer) buffer).address() + bufferPosition, baseAddress + memoryOffset, length);
        } else {
            unsafe.copyMemory(buffer.array(), BYTE_ARRAY_BASE_OFFSET + bufferPosition, null, baseAddress + memoryOffset, length);
        }
    }

    public void setBytes(long memoryOffset, ByteBuffer buffer){
        setBytes(memoryOffset, buffer, 0, buffer.capacity());
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
            int retainValue = retainCount.get();

            if (retainValue <= 0) {
                return false;
            }

            if (retainCount.compareAndSet(retainValue, retainValue + 1)) {
                return true;
            }
        }
    }

    public ByteBuffer toDirectBuffer(){
        if (size >= Integer.MAX_VALUE){
            throw new IndexOutOfBoundsException();
        }

        try {
            Field bufferAddressField = Buffer.class.getDeclaredField("address");
            bufferAddressField.setAccessible(true);
            Field bufferCapacityField = Buffer.class.getDeclaredField("capacity");
            bufferCapacityField.setAccessible(true);

            ByteBuffer directBuffer = ByteBuffer.allocateDirect(0).order(ByteOrder.nativeOrder());
            bufferAddressField.setLong(directBuffer, baseAddress);
            bufferCapacityField.setInt(directBuffer, (int) size);

            return directBuffer;
        } catch (Exception e){
            return null;
        }
    }

    public Memory copy(long memoryOffset, long length){
            checkOffset(memoryOffset);
            long endOffset = memoryOffset + length;
            checkOffset(endOffset - 1);

            Memory memory = Memory.allocate(length);

            for (long i = 0; i < length; i++){
                memory.setByte(i, memory.getByte(i));
            }

            return memory;
    }

    public Memory copy(){
        return copy(0, size);
    }

    public void copyInto(ByteBuffer buffer, long memoryOffset, int length){
        checkOffset(memoryOffset);
        long endOffset = memoryOffset + length;
        checkOffset(endOffset - 1);

        for (long i = memoryOffset; i < memoryOffset + length; i++){
            buffer.put(unsafe.getByte(baseAddress + i));
        }
    }

    public void copyInto(ByteBuffer buffer){
        copyInto(buffer, 0, (int) size);
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

    public static Memory allocate(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException();
        }
        return new Memory(bytes);
    }

    public static Memory fromByteBuffer(ByteBuffer buffer){
        Buffer fromBuffer = buffer.duplicate();
        fromBuffer.rewind();
        Memory memory = Memory.allocate(buffer.capacity());
        memory.setBytes(0, buffer);
        return memory;
    }
}
