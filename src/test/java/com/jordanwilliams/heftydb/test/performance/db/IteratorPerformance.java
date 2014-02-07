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

        for (int i = 0; i < RECORD_COUNT; i++) {
            value.data().rewind();
            db.put(ByteBuffers.fromString(i + ""), value.data());
        }

        //Scan
        Iterator<Record> iterator = db.ascendingIterator(Snapshot.MAX);

        while (iterator.hasNext()){
            iterator.next();
        }

        db.close();

        TestFileHelper.cleanUpTestFiles();
    }

}
