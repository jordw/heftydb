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

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.test.generator.TupleGenerator;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;
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
public abstract class ParameterizedTupleTest {

    @BeforeClass
    public static void beforeClass() throws IOException {
        TestFileHelper.createTestDirectory();
        TestFileHelper.cleanUpTestFiles();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        TestFileHelper.cleanUpTestFiles();
    }

    @Before
    public void beforeTest() throws IOException {
        tupleGenerator = new TupleGenerator();
        TestFileHelper.createTestDirectory();
    }

    @After
    public void afterTest() throws IOException {
        TestFileHelper.cleanUpTestFiles();
    }

    @Parameterized.Parameters
    public static Collection<Object[]> generateTestRecords() {
        TupleGenerator tupleGenerator = new TupleGenerator();
        List<Object[]> testParams = new ArrayList<Object[]>();
        final Random random = new Random(System.nanoTime());

        for (int i = 0; i < 100; i++) {
            Object[] params = new Object[1];

            List<Tuple> tuples = tupleGenerator.testRecords(1, 1000, i, new TupleGenerator.Function<Integer>() {
                        @Override
                        public Integer apply() {
                            return random.nextInt(255) + 1;
                        }
                    }, new TupleGenerator.Function<Integer>() {
                        @Override
                        public Integer apply() {
                            return random.nextInt(255);
                        }
                    }
            );
            params[0] = tuples;

            testParams.add(params);
        }

        return testParams;
    }

    protected final List<Tuple> tuples;
    protected TupleGenerator tupleGenerator;

    public ParameterizedTupleTest(List<Tuple> testTuples) {
        this.tuples = testTuples;
    }
}
