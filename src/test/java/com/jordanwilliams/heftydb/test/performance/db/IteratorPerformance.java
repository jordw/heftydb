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

import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.db.HeftyDB;
import com.jordanwilliams.heftydb.db.Record;
import com.jordanwilliams.heftydb.db.Snapshot;
import com.jordanwilliams.heftydb.metrics.HistogramMetric;
import com.jordanwilliams.heftydb.metrics.StopWatch;
import com.jordanwilliams.heftydb.state.Config;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.test.generator.KeyValueGenerator;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;
import com.jordanwilliams.heftydb.util.ByteBuffers;

import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class IteratorPerformance {

    private static final int RECORD_COUNT = 5 * 1000000;

    public static void main(String[] args) throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        TestFileHelper.createTestDirectory();
        KeyValueGenerator keyValueGenerator = new KeyValueGenerator();
        Value value = new Value(keyValueGenerator.testValue(100));

        Config config = ConfigGenerator.perfConfig();

        //Write
        final HeftyDB db = HeftyDB.open(config);

        StopWatch watch = StopWatch.start();
        HistogramMetric writeLatency = new HistogramMetric("writeLatency", "ms");

        for (int i = 0; i < RECORD_COUNT; i++) {
            value.data().rewind();
            StopWatch writeWatch = StopWatch.start();
            db.put(ByteBuffers.fromString(i + ""), value.data());
            writeLatency.record(writeWatch.elapsedMillis());
        }

        System.out.println(writeLatency.summary());
        System.out.println(RECORD_COUNT / watch.elapsedSeconds() + " write/sec");

        //Scan
        watch = StopWatch.start();
        HistogramMetric scanLatency = new HistogramMetric("scanLatency", "ms");

        Iterator<Record> iterator = db.ascendingIterator(Snapshot.MAX);

        while (iterator.hasNext()){
            StopWatch readWatch = StopWatch.start();
            iterator.next();
            scanLatency.record(readWatch.elapsedMillis());
        }

        db.close();
        System.out.println(scanLatency.summary());
        System.out.println(RECORD_COUNT / watch.elapsedSeconds() + " reads/sec");

        TestFileHelper.cleanUpTestFiles();
    }

}
