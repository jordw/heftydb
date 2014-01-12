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

package com.jordanwilliams.heftydb.test.unit.table.file;

import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.record.Value;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.table.file.RecordBlock;
import com.jordanwilliams.heftydb.util.ByteBuffers;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class RecordBlockTest {

    private static final Key TEST_KEY_1 = new Key(ByteBuffers.fromString("An awesome test key"));
    private static final Key TEST_KEY_2 = new Key(ByteBuffers.fromString("Bad as I want to be"));
    private static final Key TEST_KEY_3 = new Key(ByteBuffers.fromString("Dog I am a test key"));
    private static final RecordBlock TEST_BLOCK;
    private static final List<Record> TEST_RECORDS = new ArrayList<Record>();

    static {
        TEST_RECORDS.add(new Record(TEST_KEY_1, Value.TOMBSTONE_VALUE, 1));
        TEST_RECORDS.add(new Record(TEST_KEY_1, Value.TOMBSTONE_VALUE, 2));
        TEST_RECORDS.add(new Record(TEST_KEY_2, Value.TOMBSTONE_VALUE, 3));
        TEST_RECORDS.add(new Record(TEST_KEY_3, Value.TOMBSTONE_VALUE, 4));
        TEST_RECORDS.add(new Record(TEST_KEY_3, Value.TOMBSTONE_VALUE, 5));

        RecordBlock.Builder builder = new RecordBlock.Builder();
        for (Record record : TEST_RECORDS){
            builder.addRecord(record);
        }

        TEST_BLOCK = builder.build();
    }

    @Test
    public void findRecordExistsTest() {
        Record record = TEST_BLOCK.get(TEST_KEY_1, Long.MAX_VALUE);
        Assert.assertEquals("Record matches", 2, record.snapshotId());
    }

    @Test
    public void findRecordExistsEndTest() {
        Record record = TEST_BLOCK.get(TEST_KEY_3, Long.MAX_VALUE);
        Assert.assertEquals("Record matches", 5, record.snapshotId());
    }

    @Test
    public void findRecordMissingTest() {
        Record record = TEST_BLOCK.get(new Key(ByteBuffers.fromString("Doesn't exist")), Long.MAX_VALUE);
        Assert.assertNull("Record is null", record);
    }

    @Test
    public void recordIteratorTest() {
        Iterator<Record> blockRecords = TEST_BLOCK.iterator();
        Iterator<Record> expectedRecords = TEST_RECORDS.iterator();

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.next(), blockRecords.next());
        }
    }

    @Test
    public void descendingIteratorTest() {
        Iterator<Record> blockRecords = TEST_BLOCK.iterator(Table.IterationDirection.DESCENDING);
        ListIterator<Record> expectedRecords = TEST_RECORDS.listIterator(5);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.previous(), blockRecords.next());
        }
    }

    @Test
    public void rangeIteratorTest() {
        Iterator<Record> blockRecords = TEST_BLOCK.iteratorFrom(TEST_KEY_2, Table.IterationDirection.ASCENDING);
        ListIterator<Record> expectedRecords = TEST_RECORDS.listIterator(2);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.next(), blockRecords.next());
        }
    }

    @Test
    public void rangeIteratorInexactTest() {
        Iterator<Record> blockRecords = TEST_BLOCK.iteratorFrom(new Key(ByteBuffers.fromString("Box")),
                Table.IterationDirection.ASCENDING);
        ListIterator<Record> expectedRecords = TEST_RECORDS.listIterator(3);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.next(), blockRecords.next());
        }
    }

    @Test
    public void descendingRangeIteratorTest() {
        Iterator<Record> blockRecords = TEST_BLOCK.iteratorFrom(TEST_KEY_2, Table.IterationDirection.DESCENDING);
        ListIterator<Record> expectedRecords = TEST_RECORDS.listIterator(2);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.previous(), blockRecords.next());
        }
    }

    @Test
    public void descendingRangeIteratorInexactTest() {
        Iterator<Record> blockRecords = TEST_BLOCK.iteratorFrom(new Key(ByteBuffers.fromString("Box")),
                Table.IterationDirection.DESCENDING);
        ListIterator<Record> expectedRecords = TEST_RECORDS.listIterator(3);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.previous(), blockRecords.next());
        }
    }
}
