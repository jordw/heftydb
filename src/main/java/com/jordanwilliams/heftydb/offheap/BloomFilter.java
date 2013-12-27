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
import com.jordanwilliams.heftydb.util.Sizes;

public class BloomFilter implements Offheap {

    public static class Builder {

        private final BitSet.Builder bitSetBuilder;
        private final int hashFunctionCount;

        public Builder(long approxElementCount, double falsePositiveProbability){
            long bitCount = bitCount(approxElementCount, falsePositiveProbability);
            this.bitSetBuilder = new BitSet.Builder(bitCount);
            this.hashFunctionCount = hashFunctionCount(approxElementCount, bitCount);
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

                bitSetBuilder.set(nextHash % bitSetBuilder.sizeBytes(), true);
            }
        }

        public BloomFilter build(){
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

        private static Memory serializeBloomFilter(BitSet bitSet, int hashFunctionCount){
            Memory bloomFilterMemory = Memory.allocate(bitSet.sizeBytes() + Sizes.INT_SIZE);
            bloomFilterMemory.putInt(0, hashFunctionCount);
            bloomFilterMemory.putBytes(Sizes.INT_SIZE, bitSet.memory());
            return bloomFilterMemory;
        }
    }

    private final Memory memory;
    private final BitSet bitSet;
    private final int hashFunctionCount;

    private BloomFilter(Memory memory) {
        this.memory = memory;
        this.bitSet = new BitSet(memory, Sizes.INT_SIZE);
        this.hashFunctionCount = memory.getInt(0);
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
            if (!bitSet.get(nextHash % bitSet.sizeBytes())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Memory memory() {
        return memory;
    }

    @Override
    public long sizeBytes() {
        return memory.size();
    }
}
