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

package com.jordanwilliams.heftydb.test.unit.offheap;

import com.jordanwilliams.heftydb.offheap.BloomFilter;
import com.jordanwilliams.heftydb.test.generator.KeyValueGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class BloomFilterTest {

    @Test
    public void readWriteTest() {
        Set<ByteBuffer> testData = testKeys();
        BloomFilter testFilter = new BloomFilter(1000, 0.01);

        for (ByteBuffer key : testData) {
            testFilter.put(key.array());
        }

        for (ByteBuffer key : testData) {
            Assert.assertTrue("Key is in filter", testFilter.mightContain(key.array()));
        }
    }

    @Test
    public void serializationTest() {
        Set<ByteBuffer> testData = testKeys();
        BloomFilter testFilter = new BloomFilter(1000, 0.01);

        for (ByteBuffer key : testData) {
            testFilter.put(key.array());
        }

        for (ByteBuffer key : testData) {
            Assert.assertTrue("Key is in filter", testFilter.mightContain(key.array()));
        }

        ByteBuffer serialized = BloomFilter.SERIALIZER.serialize(testFilter);
        BloomFilter deserializedFilter = BloomFilter.SERIALIZER.deserialize(serialized);

        for (ByteBuffer key : testData) {
            Assert.assertTrue("Key is in filter", deserializedFilter.mightContain(key.array()));
        }
    }

    private static Set<ByteBuffer> testKeys() {
        KeyValueGenerator generator = new KeyValueGenerator();
        Set<ByteBuffer> testDataSet = new HashSet<ByteBuffer>();

        for (int i = 0; i < 1000; i++) {
            testDataSet.add(generator.testKey(100, 0));
        }

        return testDataSet;
    }
}
