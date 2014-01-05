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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RecordBlockTest {

    private static final Key TEST_KEY_1 = new Key(ByteBuffers.fromString("An awesome test key"));
    private static final Key TEST_KEY_2 = new Key(ByteBuffers.fromString("Bad as I want to be"));
    private static final Key TEST_KEY_3 = new Key(ByteBuffers.fromString("Dog I am a test key"));
    private static final RecordBlock TEST_BLOCK;

    static {
        RecordBlock.Builder builder = new RecordBlock.Builder();
        builder.addRecord(new Record(TEST_KEY_1, Value.TOMBSTONE_VALUE, 1));
        builder.addRecord(new Record(TEST_KEY_1, Value.TOMBSTONE_VALUE, 2));
        builder.addRecord(new Record(TEST_KEY_2, Value.TOMBSTONE_VALUE, 3));
        builder.addRecord(new Record(TEST_KEY_3, Value.TOMBSTONE_VALUE, 4));
        builder.addRecord(new Record(TEST_KEY_3, Value.TOMBSTONE_VALUE, 5));

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
        List<Record> recordList = new ArrayList<Record>();
        recordList.add(new Record(TEST_KEY_1, Value.TOMBSTONE_VALUE, 1));
        recordList.add(new Record(TEST_KEY_1, Value.TOMBSTONE_VALUE, 2));
        recordList.add(new Record(TEST_KEY_2, Value.TOMBSTONE_VALUE, 3));
        recordList.add(new Record(TEST_KEY_3, Value.TOMBSTONE_VALUE, 4));
        recordList.add(new Record(TEST_KEY_3, Value.TOMBSTONE_VALUE, 5));

        Iterator<Record> blockRecords = TEST_BLOCK.iterator();
        Iterator<Record> expectedRecords = recordList.iterator();

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.next(), blockRecords.next());
        }
    }
}
