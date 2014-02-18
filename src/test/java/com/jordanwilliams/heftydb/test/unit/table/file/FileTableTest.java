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

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.db.Config;
import com.jordanwilliams.heftydb.index.IndexBlock;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.file.FileTable;
import com.jordanwilliams.heftydb.table.file.FileTableWriter;
import com.jordanwilliams.heftydb.table.file.TupleBlock;
import com.jordanwilliams.heftydb.test.base.ParameterizedTupleTest;
import com.jordanwilliams.heftydb.test.generator.ConfigGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

public class FileTableTest extends ParameterizedTupleTest {

    private final FileTable fileTable;
    private final Random random = new Random(System.nanoTime());

    public FileTableTest(List<Tuple> testTuples) throws IOException {
        super(testTuples);
        this.fileTable = openFileTable();
    }

    @Test
    public void readWriteTest() throws IOException {
        for (Tuple tuple : tuples) {
            Tuple read = fileTable.get(tuple.key());
            Assert.assertEquals("Records match", tuple, read);
        }
    }

    @Test
    public void mightContainTest() throws IOException {
        for (Tuple tuple : tuples) {
            Assert.assertTrue("Tuple might be in the table", fileTable.mightContain(tuple.key()));
        }
    }

    @Test
    public void allIteratorTest() throws IOException {
        Iterator<Tuple> tableRecordIterator = fileTable.iterator();
        Iterator<Tuple> recordIterator = tuples.iterator();

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.next(), tableRecordIterator.next());
        }
    }

    @Test
    public void ascendingIteratorTest() throws IOException {
        Iterator<Tuple> tableRecordIterator = fileTable.ascendingIterator(Long.MAX_VALUE);
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
        Iterator<Tuple> tableRecordIterator = fileTable.ascendingIterator(medianKey, Long.MAX_VALUE);
        Iterator<Tuple> recordIterator = latestTuples.listIterator(medianKeyIndex);

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.next(), tableRecordIterator.next());
        }
    }

    @Test
    public void descendingIteratorTest() throws IOException {
        Iterator<Tuple> tableRecordIterator = fileTable.descendingIterator(Long.MAX_VALUE);
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

        Iterator<Tuple> tableRecordIterator = fileTable.descendingIterator(medianKey, Long.MAX_VALUE);
        ListIterator<Tuple> recordIterator = latestTuples.listIterator(medianKeyIndex + 1);

        while (tableRecordIterator.hasNext()) {
            Assert.assertEquals("Records match", recordIterator.previous(), tableRecordIterator.next());
        }
    }

    private FileTable openFileTable() throws IOException {
        Paths paths = ConfigGenerator.testPaths();
        Config config = ConfigGenerator.testConfig();
        FileTableWriter.Task writerTask = new FileTableWriter.Task(1, 1, paths, config, tuples.iterator(),
                tuples.size(), null, Integer.MAX_VALUE);

        writerTask.run();

        return FileTable.open(1, paths, new TupleBlock.Cache(1024000, new Metrics(config)), new IndexBlock.Cache(1024000, new Metrics(config)), new Metrics(config));
    }
}
