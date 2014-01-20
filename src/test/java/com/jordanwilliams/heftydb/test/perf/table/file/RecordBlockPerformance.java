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

package com.jordanwilliams.heftydb.test.perf.table.file;

import com.jordanwilliams.heftydb.metrics.StopWatch;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.table.file.RecordBlock;
import com.jordanwilliams.heftydb.test.generator.RecordGenerator;

import java.util.List;
import java.util.Random;

public class RecordBlockPerformance {

    public static void main(String[] args) {
        RecordGenerator generator = new RecordGenerator();
        List<Record> records = generator.testRecords(1, 64000, 20, 16, 100);

        RecordBlock.Builder blockBuilder = new RecordBlock.Builder();
        for (Record record : records) {
            blockBuilder.addRecord(record);
        }

        RecordBlock block = blockBuilder.build();

        Random random = new Random(System.nanoTime());
        StopWatch watch = StopWatch.start();
        int iterations = 2000000;

        for (int i = 0; i < iterations; i++) {
            block.get(records.get(random.nextInt(records.size())).key());
        }

        System.out.println(iterations / watch.elapsedSeconds());
    }
}
