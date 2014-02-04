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

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.test.generator.TupleGenerator;
import com.jordanwilliams.heftydb.test.util.TestFileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public abstract class RecordTest {

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
        tupleGenerator = new TupleGenerator();
        this.tuples = tupleGenerator.testRecords(1, 100, 20, random.nextInt(100) + 1, 100);
        TestFileUtils.createTestDirectory();
    }

    @After
    public void afterTest() throws IOException {
        TestFileUtils.cleanUpTestFiles();
    }

    protected final Random random = new Random(System.nanoTime());
    protected TupleGenerator tupleGenerator;
    protected List<Tuple> tuples;

    protected List<Tuple> generateMoreTestRecords(int startingSnapshotId){
        return tupleGenerator.testRecords(startingSnapshotId, 100, 20, random.nextInt(100) + 1, 100);
    }
}
