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

package com.jordanwilliams.heftydb.test.generator;

import org.apache.commons.lang3.RandomStringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class KeyValueGenerator {

    private final Random rand = new Random(System.nanoTime());
    private final Map<Integer, ByteBuffer> testKeys = new HashMap<Integer, ByteBuffer>();
    private final Map<Integer, ByteBuffer> testValues = new HashMap<Integer, ByteBuffer>();

    private ByteBuffer[] preComputedKeys;
    private int preComputedKeySize;
    private int preComputedKeysUsed = 0;

    public void precomputeKeys(int preComputedKeySize, int preComputedKeyCount) {
        this.preComputedKeys = new ByteBuffer[preComputedKeyCount];
        this.preComputedKeySize = preComputedKeySize;

        for (int i = 0; i < preComputedKeyCount; i++) {
            preComputedKeys[i] = ByteBuffer.wrap(RandomStringUtils.randomAlphanumeric(preComputedKeySize).getBytes());
        }
    }

    public ByteBuffer testKey(int size, int reuseWeight) {
        int next = rand.nextInt(100);

        if (next <= reuseWeight && reuseWeight != 0) {
            if (!testKeys.containsKey(next)) {
                testKeys.put(next, randomKey(size));
            }

            return testKeys.get(next);
        } else {
            return randomKey(size);
        }
    }

    public ByteBuffer testKey(int reuseWeight) {
        return testKey(16, reuseWeight);
    }

    public ByteBuffer testValue(int size) {
        if (!testValues.containsKey(size)) {
            testValues.put(size, randomValue(size));
        }

        return testValues.get(size);
    }

    private ByteBuffer randomKey(int size) {
        //See if we can use a precomputed random key
        if (size == preComputedKeySize && preComputedKeysUsed < preComputedKeys.length) {
            ByteBuffer preComputed = preComputedKeys[preComputedKeysUsed];
            preComputedKeysUsed++;
            return preComputed;
        }

        return ByteBuffer.wrap(RandomStringUtils.randomAlphanumeric(size).getBytes());
    }

    private ByteBuffer randomValue(int size) {
        String randomString = RandomStringUtils.randomAlphanumeric(size);
        return ByteBuffer.wrap(randomString.getBytes(Charset.defaultCharset()));
    }
}
