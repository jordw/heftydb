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

package com.jordanwilliams.heftydb.test.unit.table.memory;

import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.table.memory.MemoryTable;
import com.jordanwilliams.heftydb.test.base.ParameterizedRecordTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

public class MemoryTableTest extends ParameterizedRecordTest {

    private final MemoryTable memoryTable;
    private final Random random = new Random(System.nanoTime());

    public MemoryTableTest(List<Record> testRecords) throws IOException {
        super(testRecords);
        this.memoryTable = createMemoryTable();
    }

    @Test
    public void readWriteTest() throws IOException {
        for (Record record : records) {
            Record read = memoryTable.get(record.key());
            Assert.assertEquals("Records match", record, read);
        }
    }

    @Test
    public void allIteratorTest() throws IOException {
        Iterator<Record> tableRecordIterator = memoryTable.iterator();
        Iterator<Record> recordIterator = records.iterator();

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.next(), tableRecordIterator.next());
        }
    }

    @Test
    public void ascendingIteratorTest() throws IOException {
        Iterator<Record> tableRecordIterator = memoryTable.ascendingIterator(Long.MAX_VALUE);
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
        Iterator<Record> tableRecordIterator = memoryTable.ascendingIterator(medianKey, Long.MAX_VALUE);
        Iterator<Record> recordIterator = latestRecords.listIterator(medianKeyIndex);

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.next(), tableRecordIterator.next());
        }
    }

    @Test
    public void descendingIteratorTest() throws IOException {
        Iterator<Record> tableRecordIterator = memoryTable.descendingIterator(Long.MAX_VALUE);
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

        Iterator<Record> tableRecordIterator = memoryTable.descendingIterator(medianKey, Long.MAX_VALUE);
        ListIterator<Record> recordIterator = latestRecords.listIterator(medianKeyIndex + 1);

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.previous(), tableRecordIterator.next());
        }
    }

    private MemoryTable createMemoryTable() {
        MemoryTable memoryTable = new MemoryTable(1);

        for (Record record : records) {
            memoryTable.put(record);
        }

        return memoryTable;
    }
}
