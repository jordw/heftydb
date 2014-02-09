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

package com.jordanwilliams.heftydb.test.performance.db;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.db.HeftyDB;
import com.jordanwilliams.heftydb.state.Config;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.test.generator.KeyValueGenerator;
import com.jordanwilliams.heftydb.test.helper.PerformanceHelper;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;
import com.jordanwilliams.heftydb.util.ByteBuffers;

import java.util.Random;

public class ReadWritePerformance {

    private static final int RECORD_COUNT = 1 * 1000000;

    public static void main(String[] args) throws Exception {
        MetricRegistry metrics = new MetricRegistry();
        ConsoleReporter reporter = PerformanceHelper.consoleReporter(metrics);
        Timer writeTimer = metrics.timer("writes");
        Timer readTimer = metrics.timer("reads");
        Timer readCompactedTimer = metrics.timer("compactedReads");

        TestFileHelper.createTestDirectory();
        KeyValueGenerator keyValueGenerator = new KeyValueGenerator();
        Value value = new Value(keyValueGenerator.testValue(100));
        Random random = new Random(System.nanoTime());

        Config config = ConfigGenerator.perfConfig();

        //Write
        HeftyDB db = HeftyDB.open(config);

        for (int i = 0; i < RECORD_COUNT; i++) {
            value.data().rewind();
            Timer.Context watch = writeTimer.time();
            db.put(ByteBuffers.fromString(i + ""), value.data());
            watch.stop();
        }

        db.close();

        //Read
        db = HeftyDB.open(config);

        for (int i = 0; i < RECORD_COUNT; i++) {
            String key = random.nextInt(RECORD_COUNT) + "";
            Timer.Context watch = readTimer.time();
            db.get(ByteBuffers.fromString(key));
            watch.stop();
        }

        db.compact();

        //Read Compacted
        for (int i = 0; i < RECORD_COUNT; i++) {
            String key = random.nextInt(RECORD_COUNT) + "";
            Timer.Context watch = readCompactedTimer.time();
            db.get(ByteBuffers.fromString(key));
            watch.stop();
        }

        db.close();

        reporter.report();

        TestFileHelper.cleanUpTestFiles();
    }
}
