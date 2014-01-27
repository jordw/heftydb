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

package com.jordanwilliams.heftydb.test.unit.log;

import com.jordanwilliams.heftydb.log.WriteLog;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class WriteLogTest extends RecordTest {

    public WriteLogTest(List<Record> testRecords) throws Exception {
        super(testRecords);
    }

    @Test
    public void readWriteTest() throws IOException {
        Paths paths = ConfigGenerator.testPaths();
        WriteLog log = WriteLog.open(1, paths);

        for (Record record : records) {
            log.append(record);
        }

        Iterator<Record> logIterator = log.iterator();
        Iterator<Record> recordIterator = records.iterator();

        while (logIterator.hasNext()){
            Assert.assertEquals("Records match", recordIterator.next(), logIterator.next());
        }
    }

}
