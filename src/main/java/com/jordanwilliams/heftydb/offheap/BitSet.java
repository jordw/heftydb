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

public class BitSet {

    private final static int ADDRESS_BITS_PER_WORD = 6;
    private final Memory memory;

    public BitSet(long numberOfBits, long paddingBytes){
        memory = Memory.allocate(((memoryOffset(numberOfBits - 1) + 1) * Sizes.LONG_SIZE) + paddingBytes);
    }

    public BitSet(long numberOfBits){
        this(numberOfBits, 0);
    }

    public BitSet(Memory memory){
        this.memory = memory;
    }

    public void set(long index, boolean value) {
        int offset = memoryOffset(index);

        if (value){
            //Set
            memory.setLong(offset, memory.getLong(offset) | (1L << index));
        } else {
            //Clear
            memory.setLong(offset, memory.getLong(offset) & ~(1L << index));
        }
    }

    public boolean get(long index){
        int offset = memoryOffset(index);
        return ((memory.getLong(offset) & (1L << index)) != 0);
    }

    public long size(){
        return memory.size();
    }

    public Memory memory(){
        return memory;
    }

    @Override
    public void finalize(){
        memory.release();
    }

    private static int memoryOffset(long bitIndex) {
        return (int) ((bitIndex >> ADDRESS_BITS_PER_WORD) * Sizes.LONG_SIZE);
    }
}
