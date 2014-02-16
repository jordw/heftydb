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

package com.jordanwilliams.heftydb.test.performance.table.file;

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.db.Config;
import com.jordanwilliams.heftydb.index.Index;
import com.jordanwilliams.heftydb.index.IndexBlock;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.file.FileTableWriter;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.test.generator.TupleGenerator;
import com.jordanwilliams.heftydb.test.helper.TestFileHelper;

import java.util.List;
import java.util.Random;

public class IndexPerformance {

    public static void main(String[] args) throws Exception {
        TestFileHelper.createTestDirectory();
        TupleGenerator generator = new TupleGenerator();
        List<Tuple> tuples = generator.testRecords(1, 500000, 20, 16, 100);

        Paths paths = ConfigGenerator.testPaths();
        Config config = ConfigGenerator.testConfig();
        FileTableWriter fileTableWriter = FileTableWriter.open(1, paths, 500000, 32000, 32000, 1);
        for (Tuple tuple : tuples) {
            fileTableWriter.write(tuple);
        }

        fileTableWriter.finish();

        Index index = Index.open(1, paths, new IndexBlock.Cache(4096000), new Metrics(config));

        Random random = new Random(System.nanoTime());
        int iterations = 1000000;

        for (int i = 0; i < iterations; i++) {
            index.get(tuples.get(random.nextInt(tuples.size())).key());
        }

        TestFileHelper.cleanUpTestFiles();
    }
}
