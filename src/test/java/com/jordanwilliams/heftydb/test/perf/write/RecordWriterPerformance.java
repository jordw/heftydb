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

package com.jordanwilliams.heftydb.test.perf.write;

import com.jordanwilliams.heftydb.metrics.StopWatch;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.state.State;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.test.generator.KeyValueGenerator;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;
import com.jordanwilliams.heftydb.util.ByteBuffers;
import com.jordanwilliams.heftydb.write.TableWriter;

public class RecordWriterPerformance {

    private static final int RECORD_COUNT = 1 * 1000000;

    public static void main(String[] args) throws Exception {
        TestFileHelper.createTestDirectory();
        KeyValueGenerator keyValueGenerator = new KeyValueGenerator();
        Value value = new Value(keyValueGenerator.testValue(100));

        State state = ConfigGenerator.testState();
        TableWriter tableWriter = new TableWriter(state);

        StopWatch watch = StopWatch.start();

        for (int i = 0; i < RECORD_COUNT; i++) {
            value.data().rewind();
            tableWriter.write(ByteBuffers.fromString(i + ""), value.data());
        }

        tableWriter.close();

        System.out.println(RECORD_COUNT / watch.elapsedSeconds());
        TestFileHelper.cleanUpTestFiles();
    }
}
