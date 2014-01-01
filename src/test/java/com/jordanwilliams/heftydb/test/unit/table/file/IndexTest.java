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

package com.jordanwilliams.heftydb.test.unit.table.file;

import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.DataFiles;
import com.jordanwilliams.heftydb.table.file.Index;
import com.jordanwilliams.heftydb.table.file.IndexBlock;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.write.IndexWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexTest extends RecordTest {

    public IndexTest(List<Record> testRecords) {
        super(testRecords);
    }

    @Test
    public void readWriteTest() throws IOException {
        DataFiles dataFiles = ConfigGenerator.defaultDataFiles();
        IndexWriter indexWriter = IndexWriter.open(1, dataFiles, 512);

        List<Key> keys = new ArrayList<Key>();
        int count = 0;

        for (Record record : records) {
            keys.add(record.key());
            indexWriter.write(new IndexBlock.Record(record.key(), count));
            count++;
        }

        indexWriter.finish();

        Index index = Index.open(1, dataFiles);

        for (Record record : records) {
            List<Long> blockOffsets = index.blockOffsets(record.key());
            Assert.assertFalse("Index blocks are found", blockOffsets.isEmpty());
        }

        index.close();
    }
}
