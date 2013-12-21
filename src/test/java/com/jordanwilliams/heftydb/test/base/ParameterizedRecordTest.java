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

package com.jordanwilliams.heftydb.test.base;

import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.test.generator.RecordGenerator;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public abstract class ParameterizedRecordTest extends RecordTest {

    @Parameterized.Parameters
    public static Collection<Object[]> generateTestRecords() {
        RecordGenerator recordGenerator = new RecordGenerator();
        List<Object[]> testParams = new ArrayList<Object[]>();

        for (int i = 0; i < 10; i++){
            Object[] params = new Object[1];

            List<Record> testRecords = recordGenerator.testRecords(1, 100, i * 10, 16, 100);
            params[0] = testRecords;

            testParams.add(params);
        }

        return testParams;
    }

    protected final List<Record> records;

    public ParameterizedRecordTest(List<Record> testRecords){
        this.records = testRecords;
    }
}
