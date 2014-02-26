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

package com.jordanwilliams.heftydb.test.unit.write;

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.test.base.ParameterizedTupleTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.write.CommitLog;
import com.jordanwilliams.heftydb.write.CommitLogWriter;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class CommitLogTest extends ParameterizedTupleTest {

    public CommitLogTest(List<Tuple> testTuples) throws Exception {
        super(testTuples);
    }

    @Test
    public void readWriteTest() throws IOException {
        Paths paths = ConfigGenerator.testPaths();
        CommitLogWriter log = CommitLogWriter.open(1, paths);

        for (Tuple tuple : tuples) {
            log.append(tuple, false);
        }

        log.close();

        CommitLog commitLog = CommitLog.open(1, paths);
        Iterator<Tuple> logIterator = commitLog.iterator();
        Iterator<Tuple> recordIterator = tuples.iterator();

        while (logIterator.hasNext()) {
            Tuple next = recordIterator.next();
            Tuple logNext = logIterator.next();
            //Assert.assertEquals("Records match", recordIterator.next(), logIterator.next());
            int x = 1;
        }

        commitLog.close();
    }

}
