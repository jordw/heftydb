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

package com.jordanwilliams.heftydb.test.performance.write;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.jordanwilliams.heftydb.write.TableWriter;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.db.DBState;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.test.generator.KeyValueGenerator;
import com.jordanwilliams.heftydb.test.helper.PerformanceHelper;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;
import com.jordanwilliams.heftydb.util.ByteBuffers;

public class RecordWriterPerformance {

    private static final int RECORD_COUNT = 5 * 1000000;

    public static void main(String[] args) throws Exception {
        MetricRegistry metrics = new MetricRegistry();
        ConsoleReporter reporter = PerformanceHelper.consoleReporter(metrics);
        Timer timer = metrics.timer("writes");

        TestFileHelper.createTestDirectory();
        KeyValueGenerator keyValueGenerator = new KeyValueGenerator();
        Value value = new Value(keyValueGenerator.testValue(100));

        DBState state = ConfigGenerator.perfState();
        TableWriter tableWriter = new TableWriter(state.config(), state.paths(), state.tables(), state.snapshots(),
                state.caches(), new Metrics(state.config()));

        for (int i = 0; i < RECORD_COUNT; i++) {
            value.data().rewind();
            Timer.Context watch = timer.time();
            tableWriter.write(ByteBuffers.fromString(i + ""), value.data(), false);
            watch.stop();
        }

        reporter.report();
        tableWriter.close();

        TestFileHelper.cleanUpTestFiles();
    }
}
