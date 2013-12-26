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

import com.google.common.hash.Hashing;
import com.jordanwilliams.heftydb.util.Serializer;
import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;

public class BloomFilter {

    public static final Serializer.ByteBufferSerializer<BloomFilter> SERIALIZER = new Serializer.ByteBufferSerializer<BloomFilter>() {
        @Override
        public ByteBuffer serialize(BloomFilter data) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(serializedSize(data));
            Memory bitSetMemory = data.bitSet.memory();

            buffer.putLong(bitSetMemory.size());
            bitSetMemory.copyInto(buffer);
            buffer.putInt(data.hashFunctionCount);

            return buffer;
        }

        @Override
        public BloomFilter deserialize(ByteBuffer in) {
            ByteBuffer buffer = in.duplicate();
            buffer.rewind();

            long bitSetSize = buffer.getLong();
            Memory bitSetMemory = Memory.allocate(bitSetSize);
            bitSetMemory.putBytes(0, buffer, buffer.position(), (int) bitSetSize);
            buffer.position(buffer.position() + (int) bitSetSize);
            int hashFunctionCount = buffer.getInt();

            return new BloomFilter(new BitSet(bitSetMemory), hashFunctionCount);
        }

        @Override
        public int serializedSize(BloomFilter data) {
            return Sizes.LONG_SIZE + //Bit set length
                    (int) data.bitSet.size() +  //Bit set
                    Sizes.INT_SIZE; //Hash function count
        }
    };

    private final BitSet bitSet;
    private final int hashFunctionCount;

    public BloomFilter(long approxElementCount, double falsePositiveProbability) {
        long bitCount = bitCount(approxElementCount, falsePositiveProbability);
        this.bitSet = new BitSet(bitCount);
        this.hashFunctionCount = hashFunctionCount(approxElementCount, bitCount);
    }

    private BloomFilter(BitSet bitSet, int hashFunctionCount) {
        this.bitSet = bitSet;
        this.hashFunctionCount = hashFunctionCount;
    }

    public void put(byte[] data) {
        long hash64 = Hashing.murmur3_128().hashBytes(data).asLong();
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= hashFunctionCount; i++) {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            bitSet.set(nextHash % bitSet.size(), true);
        }
    }

    public boolean mightContain(byte[] data) {
        long hash64 = Hashing.murmur3_128().hashBytes(data).asLong();
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= hashFunctionCount; i++) {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            if (!bitSet.get(nextHash % bitSet.size())) {
                return false;
            }
        }

        return true;
    }

    static int hashFunctionCount(long approxElementCount, long bitCount) {
        return Math.max(1, (int) Math.round(bitCount / approxElementCount * Math.log(2)));
    }

    static long bitCount(long approxElementCount, double falsePositiveProbability) {
        if (falsePositiveProbability == 0) {
            falsePositiveProbability = Double.MIN_VALUE;
        }
        return (long) (-approxElementCount * Math.log(falsePositiveProbability) / (Math.log(2) * Math.log(2)));
    }
}
