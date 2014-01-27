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
import com.jordanwilliams.heftydb.table.file.IndexBlock;
import com.jordanwilliams.heftydb.table.file.IndexRecord;
import com.jordanwilliams.heftydb.util.ByteBuffers;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class IndexBlockTest {

    private static final ByteBuffer TEST_KEY_1 = ByteBuffers.fromString("An awesome test key");
    private static final ByteBuffer TEST_KEY_2 = ByteBuffers.fromString("Bad as I want to be");
    private static final ByteBuffer TEST_KEY_3 = ByteBuffers.fromString("Dog I am a test key");

    private final IndexBlock indexBlock;
    private final List<IndexRecord> indexRecords = new ArrayList<IndexRecord>();

    public IndexBlockTest() {
        indexRecords.add(new IndexRecord(new Key(TEST_KEY_1, 1), 1, 1));
        indexRecords.add(new IndexRecord(new Key(TEST_KEY_1, 2), 2, 1));
        indexRecords.add(new IndexRecord(new Key(TEST_KEY_2, 3), 3, 1));
        indexRecords.add(new IndexRecord(new Key(TEST_KEY_3, 4), 4, 1));
        indexRecords.add(new IndexRecord(new Key(TEST_KEY_3, 5), 5, 1));

        IndexBlock.Builder builder = new IndexBlock.Builder();
        for (IndexRecord record : indexRecords) {
            builder.addRecord(record);
        }

        indexBlock = builder.build();
    }

    @Test
    public void findRecordExactMatchTest() {
        IndexRecord indexRecord = indexBlock.get(new Key(TEST_KEY_1, 1));
        Assert.assertEquals("Offset matches", 1, indexRecord.blockOffset());
    }

    @Test
    public void findRecordExactMatchEndTest() {
        IndexRecord indexRecord = indexBlock.get(new Key(TEST_KEY_3, 4));
        Assert.assertEquals("Offset matches", 4, indexRecord.blockOffset());
    }

    @Test
    public void findRecordTest() {
        IndexRecord indexRecord = indexBlock.get(new Key(ByteBuffers.fromString("Awesome"), 1));
        Assert.assertEquals("Offset matches", 2, indexRecord.blockOffset());
    }

    @Test
    public void findRecordMidTest() {
        IndexRecord indexRecord = indexBlock.get(new Key(ByteBuffers.fromString("Box"), 1));
        Assert.assertEquals("Offset matches", 3, indexRecord.blockOffset());
    }

    @Test
    public void findRecordEndTest() {
        IndexRecord indexRecord = indexBlock.get(new Key(ByteBuffers.fromString("Toast"), 1));
        Assert.assertEquals("Offset matches", 5, indexRecord.blockOffset());
    }

    @Test
    public void descendingIteratorTest() {
        Iterator<IndexRecord> blockRecords = indexBlock.descendingIterator();
        ListIterator<IndexRecord> expectedRecords = indexRecords.listIterator(5);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.previous(), blockRecords.next());
        }
    }

    @Test
    public void rangeIteratorTest() {
        Iterator<IndexRecord> blockRecords = indexBlock.ascendingIterator(new Key(TEST_KEY_2, 1));
        ListIterator<IndexRecord> expectedRecords = indexRecords.listIterator(2);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.next(), blockRecords.next());
        }
    }

    @Test
    public void rangeIteratorInexactTest() {
        Iterator<IndexRecord> blockRecords = indexBlock.ascendingIterator(new Key(ByteBuffers.fromString("Box"), 0));
        ListIterator<IndexRecord> expectedRecords = indexRecords.listIterator(3);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.next(), blockRecords.next());
        }
    }

    @Test
    public void descendingRangeIteratorTest() {
        Iterator<IndexRecord> blockRecords = indexBlock.descendingIterator(new Key(TEST_KEY_2, 1));
        ListIterator<IndexRecord> expectedRecords = indexRecords.listIterator(3);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.previous(), blockRecords.next());
        }
    }

    @Test
    public void descendingRangeIteratorInexactTest() {
        Iterator<IndexRecord> blockRecords = indexBlock.descendingIterator(new Key(ByteBuffers.fromString("Box"), 0));
        ListIterator<IndexRecord> expectedRecords = indexRecords.listIterator(3);

        while (blockRecords.hasNext()) {
            Assert.assertEquals("Records match", expectedRecords.previous(), blockRecords.next());
        }
    }
}
