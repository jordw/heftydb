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

package com.jordanwilliams.heftydb.test.base;

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.db.Config;
import com.jordanwilliams.heftydb.db.DB;
import com.jordanwilliams.heftydb.db.HeftyDB;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
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
public abstract class ParameterizedIntegrationTest {

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
        TestFileHelper.createTestDirectory();
    }

    @After
    public void afterTest() throws IOException {
        TestFileHelper.cleanUpTestFiles();
    }

    @Parameterized.Parameters
    public static Collection<Object[]> generateTestData() throws Exception {
        TupleGenerator tupleGenerator = new TupleGenerator();
        final Random random = new Random(System.nanoTime());
        List<Object[]> testParams = new ArrayList<Object[]>();

        for (int i = 0; i < 100; i++) {
            Object[] params = new Object[2];

            Config config = ConfigGenerator.testConfig();
            List<Tuple> tuples = tupleGenerator.testRecords(1, 1000, 20, new TupleGenerator.Function<Integer>() {
                        @Override
                        public Integer apply() {
                            return random.nextInt(255) + 1;
                        }
                    }, new TupleGenerator.Function<Integer>() {
                        @Override
                        public Integer apply() {
                            return random.nextInt(255) + 1;
                        }
                    }
            );
            params[0] = tuples;
            params[1] = config;

            testParams.add(params);
        }

        return testParams;
    }

    protected DB db;
    protected final List<Tuple> tuples;
    protected final Config config;

    public ParameterizedIntegrationTest(List<Tuple> tuples, Config config) throws IOException {
        this.db = HeftyDB.open(config);
        this.tuples = tuples;
        this.config = config;
    }

    protected void writeRecords() throws IOException {
        for (Tuple tuple : tuples) {
            db.put(tuple.key().data(), tuple.value().data());
        }

        db.close();
    }
}
