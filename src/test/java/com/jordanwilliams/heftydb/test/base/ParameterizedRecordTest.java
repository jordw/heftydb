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
import com.jordanwilliams.heftydb.test.util.TestFileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

@RunWith(Parameterized.class)
public abstract class ParameterizedRecordTest {

    @BeforeClass
    public static void beforeClass() throws IOException {
        TestFileUtils.createTestDirectory();
        TestFileUtils.cleanUpTestFiles();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        TestFileUtils.cleanUpTestFiles();
    }

    @Before
    public void beforeTest() throws IOException {
        recordGenerator = new RecordGenerator();
        TestFileUtils.createTestDirectory();
    }

    @After
    public void afterTest() throws IOException {
        TestFileUtils.cleanUpTestFiles();
    }

    @Parameterized.Parameters
    public static Collection<Object[]> generateTestRecords() {
        RecordGenerator recordGenerator = new RecordGenerator();
        List<Object[]> testParams = new ArrayList<Object[]>();
        Random random = new Random(System.nanoTime());

        for (int i = 0; i < 100; i++) {
            Object[] params = new Object[1];

            List<Record> testRecords = recordGenerator.testRecords(1, 100, i, random.nextInt(100) + 1, 100);
            params[0] = testRecords;

            testParams.add(params);
        }

        return testParams;
    }

    protected final List<Record> records;
    protected RecordGenerator recordGenerator;

    public ParameterizedRecordTest(List<Record> testRecords) {
        this.records = testRecords;
    }
}
