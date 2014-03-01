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

package com.jordanwilliams.heftydb.test.performance.thirdparty;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.test.generator.KeyValueGenerator;
import com.jordanwilliams.heftydb.test.helper.PerformanceHelper;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;

import java.util.Random;

public class TuplPerformance {

    private static final int RECORD_COUNT = 1 * 1000000;

    public static void main(String[] args) throws Exception {
        TestFileHelper.createTestDirectory();
        TestFileHelper.cleanUpTestFiles();
        KeyValueGenerator keyValueGenerator = new KeyValueGenerator();
        Value value = new Value(keyValueGenerator.testValue(100));
        Random random = new Random(System.nanoTime());

        DatabaseConfig config = new DatabaseConfig()
                .baseFilePath("/tmp/heftytest/test")
                .maxCacheSize(512000000)
                .pageSize(4096)
                .durabilityMode(DurabilityMode.NO_SYNC);

        Database db = Database.open(config);
        Index testIndex = db.openIndex("test");

        MetricRegistry metrics = new MetricRegistry();
        ConsoleReporter reporter = PerformanceHelper.consoleReporter(metrics);
        Timer writeTimer = metrics.timer("writes");

        Cursor fill = testIndex.newCursor(Transaction.BOGUS);
        try {
            for (int i = 0; i < RECORD_COUNT; i++) {
                value.data().rewind();
                Timer.Context watch = writeTimer.time();
                fill.findNearby((i + "").getBytes());
                fill.store(value.data().array());
                watch.stop();
            }
        } finally {
            fill.reset();
        }

        db.checkpoint();
        reporter.report();

        metrics = new MetricRegistry();
        reporter = PerformanceHelper.consoleReporter(metrics);
        Timer readTimer = metrics.timer("reads");

        //Read
        for (int i = 0; i < RECORD_COUNT * 5; i++) {
            String key = random.nextInt(RECORD_COUNT) + "";
            Timer.Context watch = readTimer.time();
            testIndex.load(null, key.getBytes());
            watch.stop();
        }

        reporter.report();

        db.close();

        TestFileHelper.cleanUpTestFiles();
    }
}
