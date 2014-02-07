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

package com.jordanwilliams.heftydb.test.performance.table.memory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.table.memory.MemoryTable;
import com.jordanwilliams.heftydb.test.generator.KeyValueGenerator;
import com.jordanwilliams.heftydb.test.helper.PerformanceHelper;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;
import com.jordanwilliams.heftydb.util.ByteBuffers;

import java.util.Random;

public class MemoryTablePerformance {

    private static final int RECORD_COUNT = 1000000;

    public static void main(String[] args) throws Exception {
        MetricRegistry metrics = new MetricRegistry();
        ConsoleReporter reporter = PerformanceHelper.consoleReporter(metrics);
        Timer writeTimer = metrics.timer("writes");
        Timer readTimer = metrics.timer("reads");

        TestFileHelper.createTestDirectory();
        KeyValueGenerator keyValueGenerator = new KeyValueGenerator();
        Value value = new Value(keyValueGenerator.testValue(100));

        MemoryTable memTable = new MemoryTable(1);

        for (int i = 0; i < RECORD_COUNT; i++) {
            Timer.Context watch = writeTimer.time();
            memTable.put(new Tuple(new Key(ByteBuffers.fromString(i + ""), i), value));
            watch.stop();
        }

        TestFileHelper.cleanUpTestFiles();

        Random random = new Random(System.nanoTime());
        int iterations = 10000000;

        for (int i = 0; i < iterations; i++) {
            Timer.Context watch = readTimer.time();
            memTable.get(new Key(ByteBuffers.fromString(random.nextInt(RECORD_COUNT) + ""), Long.MAX_VALUE));
            watch.stop();
        }

        reporter.report();

        TestFileHelper.cleanUpTestFiles();
    }
}
