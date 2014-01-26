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

import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.file.Index;
import com.jordanwilliams.heftydb.table.file.IndexBlock;
import com.jordanwilliams.heftydb.table.file.IndexRecord;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.write.IndexWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class IndexTest extends RecordTest {

    private final Index index;
    private final List<IndexRecord> indexRecords = new ArrayList<IndexRecord>();

    public IndexTest(List<Record> testRecords) throws Exception {
        super(testRecords);
        this.index = createIndex();
    }

    @Test
    public void readWriteTest() throws IOException {
        int count = 0;

        for (Record record : records) {
            IndexRecord indexRecord = index.get(record.key());
            Assert.assertEquals("Index blocks are found", count, indexRecord.blockOffset());
            count++;
        }

        index.close();
    }

    @Test
    public void ascendingIteratorTest(){
        Iterator<IndexRecord> indexRecordIterator = index.ascendingIterator();
        Iterator<IndexRecord> expectedIterator = indexRecords.iterator();

        while (indexRecordIterator.hasNext()){
            Assert.assertEquals("Index records match", expectedIterator.next(), indexRecordIterator.next());
        }
    }

    @Test
    public void descendingIteratorTest(){
        Iterator<IndexRecord> indexRecordIterator = index.descendingIterator();
        ListIterator<IndexRecord> expectedIterator = indexRecords.listIterator(indexRecords.size());

        while (indexRecordIterator.hasNext()){
            Assert.assertEquals("Index records match", expectedIterator.previous(), indexRecordIterator.next());
        }
    }

    private Index createIndex() throws IOException {
        Paths paths = ConfigGenerator.testPaths();
        IndexWriter indexWriter = IndexWriter.open(1, paths, 512);

        int count = 0;

        for (Record record : records) {
            IndexRecord indexRecord = new IndexRecord(record.key(), count, 128);
            indexRecords.add(indexRecord);
            indexWriter.write(indexRecord);
            count++;
        }

        indexWriter.finish();

        return Index.open(1, paths, new IndexBlock.Cache());
    }
}
