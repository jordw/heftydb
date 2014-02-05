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

package com.jordanwilliams.heftydb.test.unit.state;

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.log.WriteLog;
import com.jordanwilliams.heftydb.state.Config;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.state.State;
import com.jordanwilliams.heftydb.state.StateInitializer;
import com.jordanwilliams.heftydb.table.file.FileTableWriter;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class StateInitializerTest extends RecordTest {

    @Test
    public void defaultStateTest() throws Exception {
        Config config = ConfigGenerator.testConfig();
        State state = new StateInitializer(config).initialize();
        Assert.assertTrue("No tables", state.tables().all().isEmpty());
        Assert.assertEquals("No Max Snapshot Id", 0, state.snapshots().currentId());
    }

    @Test
    public void existingStateTest() throws Exception {
        Paths paths = ConfigGenerator.testPaths();
        FileTableWriter tableWriter = FileTableWriter.open(1, paths, tuples.size(), 1024, 1024, 1);
        for (Tuple tuple : tuples) {
            tableWriter.write(tuple);
        }
        tableWriter.finish();

        Config config = ConfigGenerator.testConfig();
        State state = new StateInitializer(config).initialize();
        Assert.assertEquals("Should be 1 table", 1, state.tables().all().size());
        Assert.assertEquals("Should be 100 as the max snapshot id", 100, state.snapshots().currentId());
    }

    @Test
    public void existingStateLogTest() throws Exception {
        Paths paths = ConfigGenerator.testPaths();
        FileTableWriter tableWriter = FileTableWriter.open(1, paths, tuples.size(), 1024, 1024, 1);
        for (Tuple tuple : tuples) {
            tableWriter.write(tuple);
        }
        tableWriter.finish();

        WriteLog log = WriteLog.open(2, paths);
        List<Tuple> moreTestTuples = generateMoreTestRecords(101);
        for (Tuple tuple : moreTestTuples) {
            log.append(tuple);
        }

        Config config = ConfigGenerator.testConfig();
        State state = new StateInitializer(config).initialize();
        Assert.assertEquals("Should be 2 tables", 2, state.tables().all().size());
        Assert.assertEquals("Should be 100 as the max snapshot id", 200, state.snapshots().currentId());
    }
}
