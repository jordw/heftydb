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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class KeyValueGenerator {

    private static final int MAX_CACHED_KEYS = 1024;

    private final Random rand = new Random(System.nanoTime());
    private final List<ByteBuffer> testKeys = new ArrayList<ByteBuffer>();
    private final Map<Integer, ByteBuffer> testValues = new HashMap<Integer, ByteBuffer>();

    public synchronized ByteBuffer testKey(int size, int reuseWeight) {
        int next = rand.nextInt(100);

        if (next <= reuseWeight && reuseWeight != 0 && testKeys.size() > 0) {
            ByteBuffer existing = testKeys.get(rand.nextInt(testKeys.size()));
            ByteBuffer reusedKey = existing.duplicate();
            reusedKey.rewind();
            return reusedKey;
        } else {
            ByteBuffer random = randomKey(size);

            if (testKeys.size() < MAX_CACHED_KEYS) {
                testKeys.add(random);
            }

            return random.duplicate();
        }
    }

    public ByteBuffer testKey(int reuseWeight) {
        return testKey(16, reuseWeight);
    }

    public synchronized ByteBuffer testValue(int size) {
        if (!testValues.containsKey(size)) {
            testValues.put(size, randomValue(size));
        }

        ByteBuffer testValue = testValues.get(size).duplicate();
        testValue.rewind();
        return testValue;
    }

    private ByteBuffer randomKey(int size) {
        return ByteBuffer.wrap(RandomStringUtils.randomAlphanumeric(size).getBytes());
    }

    private ByteBuffer randomValue(int size) {
        String randomString = RandomStringUtils.randomAlphanumeric(size);
        return ByteBuffer.wrap(randomString.getBytes(Charset.defaultCharset()));
    }
}
