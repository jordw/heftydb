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

package com.jordanwilliams.heftydb.test.unit.index;

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.index.Index;
import com.jordanwilliams.heftydb.index.IndexBlock;
import com.jordanwilliams.heftydb.index.IndexRecord;
import com.jordanwilliams.heftydb.index.IndexWriter;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.test.base.ParameterizedRecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexTest extends ParameterizedRecordTest {

    private final Index index;
    private final List<IndexRecord> indexRecords = new ArrayList<IndexRecord>();

    public IndexTest(List<Tuple> testTuples) throws Exception {
        super(testTuples);
        this.index = createIndex();
    }

    @Test
    public void getTest() throws IOException {
        int count = 0;

        for (Tuple tuple : tuples) {
            IndexRecord indexRecord = index.get(tuple.key());
            Assert.assertEquals("Index blocks are found", count, indexRecord.blockOffset());
            count++;
        }

        index.close();
    }

    private Index createIndex() throws IOException {
        Paths paths = ConfigGenerator.testPaths();
        IndexWriter indexWriter = IndexWriter.open(1, paths, 512);

        int count = 0;

        for (Tuple tuple : tuples) {
            IndexRecord indexRecord = new IndexRecord(tuple.key(), count, 128);
            indexRecords.add(indexRecord);
            indexWriter.write(indexRecord);
            count++;
        }

        indexWriter.finish();

        return Index.open(1, paths, new IndexBlock.Cache());
    }
}
