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
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.file.FileTable;
import com.jordanwilliams.heftydb.table.file.IndexBlock;
import com.jordanwilliams.heftydb.table.file.RecordBlock;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.write.FileTableWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

public class FileTableTest extends RecordTest {

    private final FileTable fileTable;
    private final Random random = new Random(System.nanoTime());

    public FileTableTest(List<Record> testRecords) throws IOException {
        super(testRecords);
        this.fileTable = openFileTable();
    }

    @Test
    public void readWriteTest() throws IOException {
        for (Record record : records) {
            Record read = fileTable.get(record.key());
            Assert.assertEquals("Records match", record, read);
        }
    }

    @Test
    public void allIteratorTest() throws IOException {
        Iterator<Record> tableRecordIterator = fileTable.iterator();
        Iterator<Record> recordIterator = records.iterator();

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.next(), tableRecordIterator.next());
        }
    }

    @Test
    public void ascendingIteratorTest() throws IOException {
        Iterator<Record> tableRecordIterator = fileTable.ascendingIterator(Long.MAX_VALUE);
        Iterator<Record> recordIterator = recordGenerator.latestRecords(records, Long.MAX_VALUE).iterator();

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.next(), tableRecordIterator.next());
        }
    }

    @Test
    public void ascendingRangeIteratorTest() throws IOException {
        List<Record> latestRecords = recordGenerator.latestRecords(records, Long.MAX_VALUE);
        int medianKeyIndex = random.nextInt(latestRecords.size());
        Key medianKey = latestRecords.get(medianKeyIndex).key();
        Iterator<Record> tableRecordIterator = fileTable.ascendingIterator(medianKey, Long.MAX_VALUE);
        Iterator<Record> recordIterator = latestRecords.listIterator(medianKeyIndex);

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.next(), tableRecordIterator.next());
        }
    }

    @Test
    public void descendingIteratorTest() throws IOException {
        Iterator<Record> tableRecordIterator = fileTable.descendingIterator(Long.MAX_VALUE);
        List<Record> latestRecords = recordGenerator.latestRecords(records, Long.MAX_VALUE);
        ListIterator<Record> recordIterator = latestRecords.listIterator(latestRecords.size());

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.previous(), tableRecordIterator.next());
        }
    }

    @Test
    public void descendingRangeIteratorTest() throws IOException {
        List<Record> latestRecords = recordGenerator.latestRecords(records, Long.MAX_VALUE);
        int medianKeyIndex = random.nextInt(latestRecords.size());
        Key medianKey = latestRecords.get(medianKeyIndex).key();

        Iterator<Record> tableRecordIterator = fileTable.descendingIterator(medianKey, Long.MAX_VALUE);
        ListIterator<Record> recordIterator = latestRecords.listIterator(medianKeyIndex + 1);

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.previous(), tableRecordIterator.next());
        }
    }

    private FileTable openFileTable() throws IOException {
        Paths paths = ConfigGenerator.testPaths();
        FileTableWriter tableWriter = FileTableWriter.open(1, paths, 100, 512, 512, 1);

        for (Record record : records) {
            tableWriter.write(record);
        }

        tableWriter.finish();
        return FileTable.open(1, paths, new RecordBlock.Cache(), new IndexBlock.Cache());
    }
}
