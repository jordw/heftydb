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

package com.jordanwilliams.heftydb.test.performance.table.file;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.db.Config;
import com.jordanwilliams.heftydb.index.IndexBlock;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.file.FileTable;
import com.jordanwilliams.heftydb.table.file.FileTableWriter;
import com.jordanwilliams.heftydb.table.file.TupleBlock;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.test.generator.KeyValueGenerator;
import com.jordanwilliams.heftydb.test.helper.PerformanceHelper;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;
import com.jordanwilliams.heftydb.util.ByteBuffers;

import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Random;

public class FileTablePerformance {

    private static final int RECORD_COUNT = 20 * 1000000;

    public static void main(String[] args) throws Exception {
        MetricRegistry metrics = new MetricRegistry();
        ConsoleReporter reporter = PerformanceHelper.consoleReporter(metrics);
        Timer writeTimer = metrics.timer("writes");
        Timer readTimer = metrics.timer("reads");
        Config config = ConfigGenerator.defaultConfig();

        TestFileHelper.createTestDirectory();
        KeyValueGenerator keyValueGenerator = new KeyValueGenerator();
        Value value = new Value(keyValueGenerator.testValue(100));

        System.out.println("Writing file table");

        Paths paths = ConfigGenerator.testPaths();
        FileTableWriter fileTableWriter = FileTableWriter.open(1, paths, RECORD_COUNT, 32768, 8192, 1);
        for (int i = 0; i < RECORD_COUNT; i++) {
            value.data().rewind();
            Timer.Context watch = writeTimer.time();
            fileTableWriter.write(new Tuple(new Key(ByteBuffers.fromString(i + ""), i), value));
            watch.stop();
        }

        fileTableWriter.finish();

        Files.move(paths.tempPath(1), paths.tablePath(1), StandardCopyOption.ATOMIC_MOVE);

        System.out.println("Reading file table");

        FileTable fileTable = FileTable.open(1, paths, new TupleBlock.Cache(128000000),
                new IndexBlock.Cache(16384000), new Metrics(config));

        Random random = new Random(System.nanoTime());
        int iterations = 10 * 1000000;

        for (int i = 0; i < iterations; i++) {
            Timer.Context watch = readTimer.time();
            fileTable.get(new Key(ByteBuffers.fromString(random.nextInt(RECORD_COUNT) + ""), Long.MAX_VALUE));
            watch.stop();
        }

        reporter.report();

        TestFileHelper.cleanUpTestFiles();
    }
}
