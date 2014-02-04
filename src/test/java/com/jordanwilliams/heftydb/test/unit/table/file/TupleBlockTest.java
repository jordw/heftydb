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

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.table.file.DataBlock;
import com.jordanwilliams.heftydb.util.ByteBuffers;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class TupleBlockTest {

    private static final ByteBuffer TEST_KEY_1 = ByteBuffers.fromString("An awesome test key");
    private static final ByteBuffer TEST_KEY_2 = ByteBuffers.fromString("Bad as I want to be");
    private static final ByteBuffer TEST_KEY_3 = ByteBuffers.fromString("Dog I am a test key");

    private final DataBlock dataBlock;
    private final List<Tuple> tuples = new ArrayList<Tuple>();

    public TupleBlockTest() {
        tuples.add(new Tuple(new Key(TEST_KEY_1, 1), Value.TOMBSTONE_VALUE));
        tuples.add(new Tuple(new Key(TEST_KEY_1, 2), Value.TOMBSTONE_VALUE));
        tuples.add(new Tuple(new Key(TEST_KEY_2, 3), Value.TOMBSTONE_VALUE));
        tuples.add(new Tuple(new Key(TEST_KEY_3, 4), Value.TOMBSTONE_VALUE));
        tuples.add(new Tuple(new Key(TEST_KEY_3, 5), Value.TOMBSTONE_VALUE));

        DataBlock.Builder builder = new DataBlock.Builder();
        for (Tuple tuple : tuples) {
            builder.addRecord(tuple);
        }

        dataBlock = builder.build();
    }

    @Test
    public void findRecordExistsTest() {
        Tuple tuple = dataBlock.get(new Key(TEST_KEY_1, Long.MAX_VALUE));
        Assert.assertEquals("Tuple matches", 2, tuple.key().snapshotId());
    }

    @Test
    public void findRecordExistsEndTest() {
        Tuple tuple = dataBlock.get(new Key(TEST_KEY_3, Long.MAX_VALUE));
        Assert.assertEquals("Tuple matches", 5, tuple.key().snapshotId());
    }

    @Test
    public void findRecordMissingTest() {
        Tuple tuple = dataBlock.get(new Key(ByteBuffers.fromString("Doesn't exist"), 0));
        Assert.assertNull("Tuple is null", tuple);
    }

    @Test
    public void recordIteratorTest() {
        Iterator<Tuple> blockRecords = dataBlock.iterator();
        Iterator<Tuple> expectedRecords = tuples.iterator();

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.next(), blockRecords.next());
        }
    }

    @Test
    public void descendingIteratorTest() {
        Iterator<Tuple> blockRecords = dataBlock.descendingIterator();
        ListIterator<Tuple> expectedRecords = tuples.listIterator(5);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.previous(), blockRecords.next());
        }
    }

    @Test
    public void rangeIteratorTest() {
        Iterator<Tuple> blockRecords = dataBlock.ascendingIterator(new Key(TEST_KEY_2, 1));
        ListIterator<Tuple> expectedRecords = tuples.listIterator(2);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.next(), blockRecords.next());
        }
    }

    @Test
    public void rangeIteratorInexactTest() {
        Iterator<Tuple> blockRecords = dataBlock.ascendingIterator(new Key(ByteBuffers.fromString("Box"), 0));
        ListIterator<Tuple> expectedRecords = tuples.listIterator(3);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.next(), blockRecords.next());
        }
    }

    @Test
    public void descendingRangeIteratorTest() {
        Iterator<Tuple> blockRecords = dataBlock.descendingIterator(new Key(TEST_KEY_2, 1));
        ListIterator<Tuple> expectedRecords = tuples.listIterator(3);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.previous(), blockRecords.next());
        }
    }

    @Test
    public void descendingRangeIteratorInexactTest() {
        Iterator<Tuple> blockRecords = dataBlock.descendingIterator(new Key(ByteBuffers.fromString("Box"), 0));
        ListIterator<Tuple> expectedRecords = tuples.listIterator(3);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.previous(), blockRecords.next());
        }
    }
}
