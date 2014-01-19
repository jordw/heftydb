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
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.table.file.FileTable;
import com.jordanwilliams.heftydb.table.file.IndexBlock;
import com.jordanwilliams.heftydb.table.file.RecordBlock;
import com.jordanwilliams.heftydb.test.base.RecordTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import com.jordanwilliams.heftydb.write.FileTableWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class FileTableTest extends RecordTest {

    private final FileTable fileTable;

    public FileTableTest(List<Record> testRecords) throws IOException {
        super(testRecords);
        this.fileTable = openFileTable();
    }

    @Test
    public void readWriteTest() throws IOException {
        for (Record record : records) {
            Record read = fileTable.get(record.key(), record.snapshotId());
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
        Iterator<Record> tableRecordIterator = fileTable.iterator(Table.IterationDirection.ASCENDING, Long.MAX_VALUE);
        Iterator<Record> recordIterator = latestRecords(records).iterator();

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.next(), tableRecordIterator.next());
        }
    }

    @Test
    public void ascendingRangeIteratorTest() throws IOException {
        List<Record> latestRecords = latestRecords(records);
        int medianKeyIndex = latestRecords.size() / 2;
        Key medianKey = latestRecords.get(medianKeyIndex).key();
        Iterator<Record> tableRecordIterator = fileTable.iteratorFrom(medianKey, Table.IterationDirection.ASCENDING, Long.MAX_VALUE);
        Iterator<Record> recordIterator = latestRecords.listIterator(medianKeyIndex);

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.next(), tableRecordIterator.next());
        }
    }

    @Test
    public void descendingIteratorTest() throws IOException {
        Iterator<Record> tableRecordIterator = fileTable.iterator(Table.IterationDirection.DESCENDING, Long.MAX_VALUE);
        List<Record> latestRecords = latestRecords(records);
        ListIterator<Record> recordIterator = latestRecords.listIterator(latestRecords.size());

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

    private List<Record> latestRecords(List<Record> records) {
        SortedMap<Key, Record> latestRecordMap = new TreeMap<Key, Record>();

        for (Record record : records) {
            Record known = latestRecordMap.get(record.key());

            if (known == null || known.snapshotId() < record.snapshotId()) {
                latestRecordMap.put(record.key(), record);
            }
        }

        List<Record> latestRecords = new ArrayList<Record>();

        for (Record record : latestRecordMap.values()) {
            latestRecords.add(record);
        }

        return latestRecords;
    }
}
