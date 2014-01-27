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
import com.jordanwilliams.heftydb.table.file.RecordBlock;
import com.jordanwilliams.heftydb.util.ByteBuffers;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class RecordBlockTest {

    private static final ByteBuffer TEST_KEY_1 = ByteBuffers.fromString("An awesome test key");
    private static final ByteBuffer TEST_KEY_2 = ByteBuffers.fromString("Bad as I want to be");
    private static final ByteBuffer TEST_KEY_3 = ByteBuffers.fromString("Dog I am a test key");

    private final RecordBlock recordBlock;
    private final List<Record> records = new ArrayList<Record>();

    public RecordBlockTest() {
        records.add(new Record(new Key(TEST_KEY_1, 1), Value.TOMBSTONE_VALUE));
        records.add(new Record(new Key(TEST_KEY_1, 2), Value.TOMBSTONE_VALUE));
        records.add(new Record(new Key(TEST_KEY_2, 3), Value.TOMBSTONE_VALUE));
        records.add(new Record(new Key(TEST_KEY_3, 4), Value.TOMBSTONE_VALUE));
        records.add(new Record(new Key(TEST_KEY_3, 5), Value.TOMBSTONE_VALUE));

        RecordBlock.Builder builder = new RecordBlock.Builder();
        for (Record record : records) {
            builder.addRecord(record);
        }

        recordBlock = builder.build();
    }

    @Test
    public void findRecordExistsTest() {
        Record record = recordBlock.get(new Key(TEST_KEY_1, Long.MAX_VALUE));
        Assert.assertEquals("Record matches", 2, record.key().snapshotId());
    }

    @Test
    public void findRecordExistsEndTest() {
        Record record = recordBlock.get(new Key(TEST_KEY_3, Long.MAX_VALUE));
        Assert.assertEquals("Record matches", 5, record.key().snapshotId());
    }

    @Test
    public void findRecordMissingTest() {
        Record record = recordBlock.get(new Key(ByteBuffers.fromString("Doesn't exist"), 0));
        Assert.assertNull("Record is null", record);
    }

    @Test
    public void recordIteratorTest() {
        Iterator<Record> blockRecords = recordBlock.iterator();
        Iterator<Record> expectedRecords = records.iterator();

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.next(), blockRecords.next());
        }
    }

    @Test
    public void descendingIteratorTest() {
        Iterator<Record> blockRecords = recordBlock.descendingIterator();
        ListIterator<Record> expectedRecords = records.listIterator(5);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.previous(), blockRecords.next());
        }
    }

    @Test
    public void rangeIteratorTest() {
        Iterator<Record> blockRecords = recordBlock.ascendingIterator(new Key(TEST_KEY_2, 1));
        ListIterator<Record> expectedRecords = records.listIterator(2);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.next(), blockRecords.next());
        }
    }

    @Test
    public void rangeIteratorInexactTest() {
        Iterator<Record> blockRecords = recordBlock.ascendingIterator(new Key(ByteBuffers.fromString("Box"), 0));
        ListIterator<Record> expectedRecords = records.listIterator(3);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.next(), blockRecords.next());
        }
    }

    @Test
    public void descendingRangeIteratorTest() {
        Iterator<Record> blockRecords = recordBlock.descendingIterator(new Key(TEST_KEY_2, 1));
        ListIterator<Record> expectedRecords = records.listIterator(3);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.previous(), blockRecords.next());
        }
    }

    @Test
    public void descendingRangeIteratorInexactTest() {
        Iterator<Record> blockRecords = recordBlock.descendingIterator(new Key(ByteBuffers.fromString("Box"), 0));
        ListIterator<Record> expectedRecords = records.listIterator(3);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.previous(), blockRecords.next());
        }
    }
}
