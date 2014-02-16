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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.table.file.TupleBlock;
import com.jordanwilliams.heftydb.test.generator.TupleGenerator;
import com.jordanwilliams.heftydb.test.helper.PerformanceHelper;

import java.util.List;
import java.util.Random;

public class RecordBlockPerformance {

    public static void main(String[] args) {
        MetricRegistry metrics = new MetricRegistry();
        ConsoleReporter reporter = PerformanceHelper.consoleReporter(metrics);
        Timer timer = metrics.timer("reads");
        TupleGenerator generator = new TupleGenerator();
        List<Tuple> tuples = generator.testRecords(1, 64000, 20, 16, 100);

        TupleBlock.Builder blockBuilder = new TupleBlock.Builder();
        for (Tuple tuple : tuples) {
            blockBuilder.addRecord(tuple);
        }

        TupleBlock block = blockBuilder.build();

        Random random = new Random(System.nanoTime());
        int iterations = 10000000;

        for (int i = 0; i < iterations; i++) {
            Timer.Context watch = timer.time();
            block.get(tuples.get(random.nextInt(tuples.size())).key());
            watch.stop();
        }

        reporter.report();
    }
}
