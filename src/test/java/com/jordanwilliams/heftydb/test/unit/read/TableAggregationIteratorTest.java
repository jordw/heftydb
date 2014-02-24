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

package com.jordanwilliams.heftydb.test.unit.read;

import com.jordanwilliams.heftydb.read.LatestTupleIterator;
import com.jordanwilliams.heftydb.read.MergingIterator;
import com.jordanwilliams.heftydb.read.TableAggregationIterator;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.state.Tables;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.table.memory.MemoryTable;
import com.jordanwilliams.heftydb.test.generator.TupleGenerator;
import com.jordanwilliams.heftydb.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class TableAggregationIteratorTest {

    private final TupleGenerator tupleGenerator = new TupleGenerator();

    @Test
    public void changeTablesTests() throws Exception {
        for (int i = 0; i < 100; i++) {
            changingTablesTest();
        }
    }

    public void changingTablesTest() throws Exception {
        Tables tables = testTables();
        List<Tuple> iteratorTuples = new ArrayList<Tuple>();
        CloseableIterator<Tuple> tableReaderTuples = ascendingTupleIterator(tables);

        while (tableReaderTuples.hasNext()) {
            iteratorTuples.add(tableReaderTuples.next());
        }

        TableAggregationIterator tableAggregationIterator = new TableAggregationIterator(ascendingTupleIterator
                (tables), Long.MAX_VALUE, tables, ascendingIteratorSource(tables));

        Iterator<Tuple> iteratorTupleIterator = iteratorTuples.iterator();

        int count = 0;
        int pivot = new Random().nextInt(iteratorTuples.size());

        while (tableAggregationIterator.hasNext()) {

            //Mutate the set of tables halfway through
            if (count == Math.max(pivot, 1)) {
                Table merged = merge(tables);

                List<Table> toRemove = new ArrayList<Table>();

                for (Table table : tables) {
                    toRemove.add(table);
                }

                tables.add(merged);

                for (Table table : toRemove) {
                    tables.remove(table);
                }
            }

            Assert.assertEquals("Tuples match", iteratorTupleIterator.next(), tableAggregationIterator.next());
            count++;
        }
    }

    private Table merge(Tables tables) {
        MemoryTable merged = new MemoryTable(255);

        for (Table table : tables) {
            Iterator<Tuple> tuples = table.ascendingIterator(Long.MAX_VALUE);

            while (tuples.hasNext()) {
                merged.put(tuples.next());
            }
        }

        return merged;
    }

    private TableAggregationIterator.Source ascendingIteratorSource(final Tables tables) {
        return new TableAggregationIterator.Source() {
            @Override
            public CloseableIterator<Tuple> refresh(Key key, long snapshotId) {
                return ascendingTupleIterator(tables, key, snapshotId);
            }
        };
    }

    private CloseableIterator<Tuple> ascendingTupleIterator(Tables tables) {
        List<CloseableIterator<Tuple>> tableIterators = new ArrayList<CloseableIterator<Tuple>>();

        for (Table table : tables) {
            tableIterators.add(table.ascendingIterator(Long.MAX_VALUE));
        }

        return new LatestTupleIterator(Long.MAX_VALUE, new MergingIterator<Tuple>(false, tableIterators));
    }

    private CloseableIterator<Tuple> ascendingTupleIterator(Tables tables, Key key, long snapshotId) {
        List<CloseableIterator<Tuple>> tableIterators = new ArrayList<CloseableIterator<Tuple>>();

        for (Table table : tables) {
            tableIterators.add(table.ascendingIterator(key, snapshotId));
        }

        return new LatestTupleIterator(snapshotId, new MergingIterator<Tuple>(false, tableIterators));
    }

    private Tables testTables() {
        List<Table> tables = new ArrayList<Table>();

        for (int i = 0; i < 10; i++) {
            tables.add(testTable(i));
        }

        return new Tables(tables);
    }

    private MemoryTable testTable(long tableId) {
        MemoryTable testMemoryTable = new MemoryTable(tableId);
        List<Tuple> tuples = tupleGenerator.testRecords(100, 20);

        for (Tuple tuple : tuples) {
            testMemoryTable.put(tuple);
        }

        return testMemoryTable;
    }
}
