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
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;

public class BloomFilter implements Offheap {

    public static class Builder {

        private final BitSet.Builder bitSetBuilder;
        private final int hashFunctionCount;

        public Builder(long approxElementCount, double falsePositiveProbability) {
            long bitCount = bitCount(approxElementCount, falsePositiveProbability);
            this.bitSetBuilder = new BitSet.Builder(bitCount, Sizes.INT_SIZE);
            this.hashFunctionCount = hashFunctionCount(approxElementCount, bitCount);
        }

        public void put(Key key) {
            long hash64 = Hashing.murmur3_128().hashBytes(key.data().array()).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            for (int i = 1; i <= hashFunctionCount; i++) {
                int nextHash = hash1 + i * hash2;

                if (nextHash < 0) {
                    nextHash = ~nextHash;
                }

                bitSetBuilder.set(nextHash % bitSetBuilder.usableBytes(), true);
            }
        }

        public BloomFilter build() {
            Memory bloomFilterMemory = serializeBloomFilter(bitSetBuilder.build(), hashFunctionCount);
            return new BloomFilter(bloomFilterMemory);
        }

        private static int hashFunctionCount(long approxElementCount, long bitCount) {
            return Math.max(1, (int) Math.round(bitCount / approxElementCount * Math.log(2)));
        }

        private static long bitCount(long approxElementCount, double falsePositiveProbability) {
            if (falsePositiveProbability == 0) {
                falsePositiveProbability = Double.MIN_VALUE;
            }
            return (long) (-approxElementCount * Math.log(falsePositiveProbability) / (Math.log(2) * Math.log(2)));
        }

        private static Memory serializeBloomFilter(BitSet bitSet, int hashFunctionCount) {
            Memory bloomFilterMemory = bitSet.memory();
            bloomFilterMemory.directBuffer().putInt(bitSet.usableBytes(), hashFunctionCount);
            return bloomFilterMemory;
        }
    }

    private final Memory memory;
    private final BitSet bitSet;
    private final int hashFunctionCount;

    public BloomFilter(Memory memory) {
        this.memory = memory;
        this.bitSet = new BitSet(memory, memory.size() - Sizes.INT_SIZE);
        ByteBuffer directBuffer = memory.directBuffer();
        this.hashFunctionCount = directBuffer.getInt(directBuffer.capacity() - Sizes.INT_SIZE);
    }

    public boolean mightContain(Key key) {
        long hash64 = Hashing.murmur3_128().hashBytes(key.data().array()).asLong();
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= hashFunctionCount; i++) {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            if (!bitSet.get(nextHash % bitSet.usableBytes())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Memory memory() {
        return memory;
    }
}
