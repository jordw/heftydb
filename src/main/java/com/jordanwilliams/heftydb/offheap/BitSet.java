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

public class BitSet implements Offheap {

    public static class Builder {

        private final Memory memory;

        public Builder(long bitCount) {
            memory = Memory.allocate(((memoryOffset(bitCount - 1) + 1) * Sizes.LONG_SIZE));
        }

        public void set(long bitIndex, boolean value) {
            long offset = memoryOffset(bitIndex);

            if (value) {
                //Set
                memory.putLong(offset, memory.getLong(offset) | (1L << bitIndex));
            } else {
                //Clear
                memory.putLong(offset, memory.getLong(offset) & ~(1L << bitIndex));
            }
        }

        public BitSet build() {
            return new BitSet(memory, 0);
        }

        public long sizeBytes() {
            return memory.size();
        }

        private long memoryOffset(long bitIndex) {
            return ((bitIndex >> ADDRESS_BITS_PER_WORD) * Sizes.LONG_SIZE);
        }
    }

    private static final int ADDRESS_BITS_PER_WORD = 6;

    private final Memory memory;
    private final long startingOffset;

    public BitSet(Memory memory, long startingOffset) {
        this.memory = memory;
        this.startingOffset = startingOffset;
    }

    public boolean get(long index) {
        long offset = memoryOffset(index);
        return ((memory.getLong(offset) & (1L << index)) != 0);
    }

    @Override
    public Memory memory() {
        return memory;
    }

    @Override
    public long sizeBytes() {
        return memory.size() - startingOffset;
    }

    @Override
    public void releaseMemory() {
        memory.release();
    }

    private long memoryOffset(long bitIndex) {
        return startingOffset + ((bitIndex >> ADDRESS_BITS_PER_WORD) * Sizes.LONG_SIZE);
    }
}
