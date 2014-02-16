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

import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;

public class BitSet implements Offheap {

    public static class Builder {

        private final Memory memory;
        private final ByteBuffer directBuffer;
        private final int usableBytes;

        public Builder(long bitCount, int paddingBytes) {
            this.usableBytes = memoryOffset(bitCount) + Sizes.LONG_SIZE;
            this.memory = Memory.allocateAndZero(usableBytes + paddingBytes);
            this.directBuffer = memory.directBuffer();
        }

        public void set(long bitIndex, boolean value) {
            int offset = memoryOffset(bitIndex);

            if (value) {
                //Set
                directBuffer.putLong(offset, directBuffer.getLong(offset) | (1L << bitIndex));
            } else {
                //Clear
                directBuffer.putLong(offset, directBuffer.getLong(offset) & ~(1L << bitIndex));
            }
        }

        public BitSet build() {
            return new BitSet(memory, usableBytes);
        }

        public int usableBytes() {
            return usableBytes;
        }

        public long bitCount() {
            return usableBytes * 8;
        }
    }

    private static final int ADDRESS_BITS_PER_WORD = 6;

    private final Memory memory;
    private final ByteBuffer directBuffer;
    private final int usableBytes;

    public BitSet(Memory memory, int usableBytes) {
        this.memory = memory;
        this.directBuffer = memory.directBuffer();
        this.usableBytes = usableBytes;
    }

    public boolean get(long index) {
        int offset = memoryOffset(index);
        long currentValue = directBuffer.getLong(offset);
        return (currentValue & (1L << index)) != 0;
    }

    public int usableBytes() {
        return usableBytes;
    }

    public long bitCount() {
        return usableBytes * 8;
    }

    @Override
    public Memory memory() {
        return memory;
    }

    private static int memoryOffset(long bitIndex) {
        return (int) (bitIndex >> ADDRESS_BITS_PER_WORD) * Sizes.LONG_SIZE;
    }
}
