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

import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.Config;
import com.jordanwilliams.heftydb.state.State;
import com.jordanwilliams.heftydb.state.StateInitializer;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class StateInitializerTest extends RecordTest {

    public StateInitializerTest(List<Record> testRecords) {
        super(testRecords);
    }

    @Test
    public void defaultStateTest() throws Exception {
        Config config = ConfigGenerator.testConfig();
        State state = new StateInitializer(config).initialize();
        Assert.assertTrue("No tables", state.tables().all().isEmpty());
    }

    @Test
    public void existingStateTest(){

    }
}