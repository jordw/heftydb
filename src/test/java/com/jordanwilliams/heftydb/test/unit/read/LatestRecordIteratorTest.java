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

import com.jordanwilliams.heftydb.read.LatestRecordIterator;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.record.Value;
import com.jordanwilliams.heftydb.util.ByteBuffers;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LatestRecordIteratorTest {

    private static final ByteBuffer KEY_1 = ByteBuffers.fromString("test key 1");
    private static final ByteBuffer KEY_2 = ByteBuffers.fromString("test key 2");

    private final List<Record> sourceRecords = new ArrayList<Record>();
    private final List<Record> filteredRecords = new ArrayList<Record>();
    private final List<Record> snapshotRecords = new ArrayList<Record>();

    public LatestRecordIteratorTest(){
        sourceRecords.add(new Record(new Key(KEY_1, 1), Value.TOMBSTONE_VALUE));
        sourceRecords.add(new Record(new Key(KEY_1, 2), Value.TOMBSTONE_VALUE));
        sourceRecords.add(new Record(new Key(KEY_1, 3), Value.TOMBSTONE_VALUE));
        sourceRecords.add(new Record(new Key(KEY_2, 4), Value.TOMBSTONE_VALUE));
        sourceRecords.add(new Record(new Key(KEY_2, 5), Value.TOMBSTONE_VALUE));

        filteredRecords.add(new Record(new Key(KEY_1, 3), Value.TOMBSTONE_VALUE));
        filteredRecords.add(new Record(new Key(KEY_2, 5), Value.TOMBSTONE_VALUE));

        snapshotRecords.add(new Record(new Key(KEY_1, 3), Value.TOMBSTONE_VALUE));
        snapshotRecords.add(new Record(new Key(KEY_2, 4), Value.TOMBSTONE_VALUE));
    }

    @Test
    public void filterRecordTest() {
        Iterator<Record> filteredIterator = filteredRecords.iterator();
        Iterator<Record> versionedIterator = new LatestRecordIterator(6, sourceRecords.iterator());

        while (versionedIterator.hasNext()) {
            Assert.assertEquals("Records match", versionedIterator.next(), filteredIterator.next());
        }
    }

    @Test
    public void respectSnapshotTest() {
        Iterator<Record> snapshotIterator = snapshotRecords.iterator();
        Iterator<Record> versionedIterator = new LatestRecordIterator(4, sourceRecords.iterator());

        while (versionedIterator.hasNext()) {
            Assert.assertEquals("Records match", versionedIterator.next(), snapshotIterator.next());
        }
    }
}
