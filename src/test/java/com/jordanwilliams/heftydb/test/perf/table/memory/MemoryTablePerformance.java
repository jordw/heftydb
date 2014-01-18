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

package com.jordanwilliams.heftydb.test.perf.table.memory;

import com.jordanwilliams.heftydb.metrics.StopWatch;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.record.Value;
import com.jordanwilliams.heftydb.table.memory.MemoryTable;
import com.jordanwilliams.heftydb.test.generator.KeyValueGenerator;
import com.jordanwilliams.heftydb.test.util.TestFileUtils;
import com.jordanwilliams.heftydb.util.ByteBuffers;

import java.util.Random;

public class MemoryTablePerformance {

    private static final int RECORD_COUNT = 128000;

    public static void main(String[] args) throws Exception {
        TestFileUtils.createTestDirectory();
        KeyValueGenerator keyValueGenerator = new KeyValueGenerator();
        Value value = new Value(keyValueGenerator.testValue(100));

        StopWatch watch = StopWatch.start();
        MemoryTable memTable = new MemoryTable(1);

        for (int i = 0; i < RECORD_COUNT; i++) {
            memTable.put(new Record(new Key(ByteBuffers.fromString(i + "")), value, i));
        }

        System.out.println("Writes " + RECORD_COUNT / watch.elapsedSeconds());
        TestFileUtils.cleanUpTestFiles();

        Random random = new Random(System.nanoTime());
        watch = StopWatch.start();
        int iterations = 10000000;

        for (int i = 0; i < iterations; i++) {
            memTable.get(new Key(ByteBuffers.fromString(random.nextInt(RECORD_COUNT) + "")), Long.MAX_VALUE);
        }

        System.out.println("Reads " + iterations / watch.elapsedSeconds());
        TestFileUtils.cleanUpTestFiles();
    }
}
