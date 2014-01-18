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

package com.jordanwilliams.heftydb.test.unit.read;

import com.jordanwilliams.heftydb.read.VersionedRecordIterator;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.record.Value;
import com.jordanwilliams.heftydb.util.ByteBuffers;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class VersionedRecordIteratorTest {

    private static final Key KEY_1 = new Key(ByteBuffers.fromString("test key 1"));
    private static final Key KEY_2 = new Key(ByteBuffers.fromString("test key 2"));

    private static final List<Record> SOURCE_RECORDS = new ArrayList<Record>();
    private static final List<Record> FILTERED_RECORDS = new ArrayList<Record>();
    private static final List<Record> SNAPSHOT_RECORDS = new ArrayList<Record>();

    static {
        SOURCE_RECORDS.add(new Record(KEY_1, Value.TOMBSTONE_VALUE, 1));
        SOURCE_RECORDS.add(new Record(KEY_1, Value.TOMBSTONE_VALUE, 2));
        SOURCE_RECORDS.add(new Record(KEY_1, Value.TOMBSTONE_VALUE, 3));
        SOURCE_RECORDS.add(new Record(KEY_2, Value.TOMBSTONE_VALUE, 4));
        SOURCE_RECORDS.add(new Record(KEY_2, Value.TOMBSTONE_VALUE, 5));

        FILTERED_RECORDS.add(new Record(KEY_1, Value.TOMBSTONE_VALUE, 3));
        FILTERED_RECORDS.add(new Record(KEY_2, Value.TOMBSTONE_VALUE, 5));

        SNAPSHOT_RECORDS.add(new Record(KEY_1, Value.TOMBSTONE_VALUE, 3));
        SNAPSHOT_RECORDS.add(new Record(KEY_2, Value.TOMBSTONE_VALUE, 4));
    }

    @Test
    public void filterRecordTest() {
        Iterator<Record> filteredIterator = FILTERED_RECORDS.iterator();
        Iterator<Record> versionedIterator = new VersionedRecordIterator(6, SOURCE_RECORDS.iterator());

        while (versionedIterator.hasNext()) {
            Assert.assertEquals("Records match", versionedIterator.next(), filteredIterator.next());
        }
    }

    @Test
    public void respectSnapshotTest() {
        Iterator<Record> snapshotIterator = SNAPSHOT_RECORDS.iterator();
        Iterator<Record> versionedIterator = new VersionedRecordIterator(4, SOURCE_RECORDS.iterator());

        while (versionedIterator.hasNext()) {
            Assert.assertEquals("Records match", versionedIterator.next(), snapshotIterator.next());
        }
    }
}
