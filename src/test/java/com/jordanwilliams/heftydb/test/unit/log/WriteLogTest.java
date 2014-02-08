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

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.log.WriteLog;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.test.base.ParameterizedRecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class WriteLogTest extends ParameterizedRecordTest {

    public WriteLogTest(List<Tuple> testTuples) throws Exception {
        super(testTuples);
    }

    @Test
    public void readWriteTest() throws IOException {
        Paths paths = ConfigGenerator.testPaths();
        WriteLog log = WriteLog.open(1, paths);

        for (Tuple tuple : tuples) {
            log.append(tuple, false);
        }

        Iterator<Tuple> logIterator = log.iterator();
        Iterator<Tuple> recordIterator = tuples.iterator();

        while (logIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.next(), logIterator.next());
        }
    }

}
