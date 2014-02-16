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

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.table.memory.MemoryTable;
import com.jordanwilliams.heftydb.test.base.ParameterizedTupleTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

public class MemoryTableTest extends ParameterizedTupleTest {

    private final MemoryTable memoryTable;
    private final Random random = new Random(System.nanoTime());

    public MemoryTableTest(List<Tuple> testTuples) throws IOException {
        super(testTuples);
        this.memoryTable = createMemoryTable();
    }

    @Test
    public void readWriteTest() throws IOException {
        for (Tuple tuple : tuples) {
            Tuple read = memoryTable.get(tuple.key());
            Assert.assertEquals("Records match", tuple, read);
        }
    }

    @Test
    public void mightContainTest() throws IOException {
        for (Tuple tuple : tuples) {
            Assert.assertTrue("Tuple might be in the table", memoryTable.mightContain(tuple.key()));
        }
    }

    @Test
    public void allIteratorTest() throws IOException {
        Iterator<Tuple> tableRecordIterator = memoryTable.iterator();
        Iterator<Tuple> recordIterator = tuples.iterator();

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.next(), tableRecordIterator.next());
        }
    }

    @Test
    public void ascendingIteratorTest() throws IOException {
        Iterator<Tuple> tableRecordIterator = memoryTable.ascendingIterator(Long.MAX_VALUE);
        Iterator<Tuple> recordIterator = tupleGenerator.latest(tuples, Long.MAX_VALUE).iterator();

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.next(), tableRecordIterator.next());
        }
    }

    @Test
    public void ascendingRangeIteratorTest() throws IOException {
        List<Tuple> latestTuples = tupleGenerator.latest(tuples, Long.MAX_VALUE);
        int medianKeyIndex = random.nextInt(latestTuples.size());
        Key medianKey = latestTuples.get(medianKeyIndex).key();
        Iterator<Tuple> tableRecordIterator = memoryTable.ascendingIterator(medianKey, Long.MAX_VALUE);
        Iterator<Tuple> recordIterator = latestTuples.listIterator(medianKeyIndex);

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.next(), tableRecordIterator.next());
        }
    }

    @Test
    public void descendingIteratorTest() throws IOException {
        Iterator<Tuple> tableRecordIterator = memoryTable.descendingIterator(Long.MAX_VALUE);
        List<Tuple> latestTuples = tupleGenerator.latest(tuples, Long.MAX_VALUE);
        ListIterator<Tuple> recordIterator = latestTuples.listIterator(latestTuples.size());

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.previous(), tableRecordIterator.next());
        }
    }

    @Test
    public void descendingRangeIteratorTest() throws IOException {
        List<Tuple> latestTuples = tupleGenerator.latest(tuples, Long.MAX_VALUE);
        int medianKeyIndex = random.nextInt(latestTuples.size());
        Key medianKey = latestTuples.get(medianKeyIndex).key();

        Iterator<Tuple> tableRecordIterator = memoryTable.descendingIterator(medianKey, Long.MAX_VALUE);
        ListIterator<Tuple> recordIterator = latestTuples.listIterator(medianKeyIndex + 1);

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.previous(), tableRecordIterator.next());
        }
    }

    private MemoryTable createMemoryTable() {
        MemoryTable memoryTable = new MemoryTable(1);

        for (Tuple tuple : tuples) {
            memoryTable.put(tuple);
        }

        return memoryTable;
    }
}
