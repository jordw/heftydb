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

package com.jordanwilliams.heftydb.test.performance.table.file;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.file.TableBloomFilter;
import com.jordanwilliams.heftydb.table.file.TableBloomFilterWriter;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.test.generator.KeyValueGenerator;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;
import com.jordanwilliams.heftydb.util.ByteBuffers;

public class TableBloomFilterPerformance {

    private static final int RECORD_COUNT = 5 * 1000000;

    public static void main(String[] args) throws Exception {
        TestFileHelper.createTestDirectory();
        KeyValueGenerator keyValueGenerator = new KeyValueGenerator();
        Value value = new Value(keyValueGenerator.testValue(100));

        System.out.println("Writing bloom filter");

        Paths paths = ConfigGenerator.testPaths();
        TableBloomFilterWriter filterWriter = TableBloomFilterWriter.open(1, paths, RECORD_COUNT);
        BloomFilter<Key> guavaFilter = BloomFilter.create(new Funnel<Key>() {
            @Override
            public void funnel(Key key, PrimitiveSink primitiveSink) {
                primitiveSink.putBytes(key.data().array());
            }
        }, RECORD_COUNT, 0.01);

        for (int i = 0; i < RECORD_COUNT; i++) {
            value.data().rewind();
            filterWriter.write(new Key(ByteBuffers.fromString(i + ""), i));
            guavaFilter.put(new Key(ByteBuffers.fromString(i + ""), i));
        }

        filterWriter.finish();

        System.out.println("Reading bloom filter");

        TableBloomFilter tableBloomFilter = TableBloomFilter.read(1, paths);

        double hits = 0;
        double misses = 0;

        double ghits = 0;
        double gmisses = 0;

        for (int i = RECORD_COUNT * 2; i > RECORD_COUNT; i--) {
            if (tableBloomFilter.mightContain(new Key(ByteBuffers.fromString(i + ""), i))){
                hits++;
            } else {
                misses++;
            }

            if (guavaFilter.mightContain(new Key(ByteBuffers.fromString(i + ""), i))){
                ghits++;
            } else {
                gmisses++;
            }
        }

        System.out.println("False positive rate: " + hits / (hits + misses));
        System.out.println("Guava positive rate: " + ghits / (ghits + gmisses));

        TestFileHelper.cleanUpTestFiles();
    }

}
