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

package com.jordanwilliams.heftydb.aggregate;

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.state.Tables;
import com.jordanwilliams.heftydb.table.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TableReader implements Iterable<Tuple> {

    private final Tables tables;

    public TableReader(Tables tables) {
        this.tables = tables;
    }

    public Tuple get(Key key) {
        Tuple closestTuple = null;

        tables.readLock();

        try {
            for (Table table : tables) {
                if (table.mightContain(key)) {
                    Tuple tableTuple = table.get(key);

                    if (tableTuple != null) {
                        if (closestTuple == null || tableTuple.key().snapshotId() > closestTuple.key().snapshotId()) {
                            closestTuple = tableTuple;
                        }
                    }
                }
            }
        } finally {
            tables.readUnlock();
        }

        return closestTuple;
    }

    public Iterator<Tuple> ascendingIterator(long snapshotId) {
        List<Iterator<Tuple>> tableIterators = new ArrayList<Iterator<Tuple>>();

        for (Table table : tables) {
            tableIterators.add(table.ascendingIterator(snapshotId));
        }

        return new LatestTupleIterator(snapshotId, new MergingIterator<Tuple>(tableIterators));
    }

    public Iterator<Tuple> descendingIterator(long snapshotId) {
        List<Iterator<Tuple>> tableIterators = new ArrayList<Iterator<Tuple>>();

        for (Table table : tables) {
            tableIterators.add(table.descendingIterator(snapshotId));
        }

        return new LatestTupleIterator(snapshotId, new MergingIterator<Tuple>(true, tableIterators));
    }

    public Iterator<Tuple> ascendingIterator(Key key, long snapshotId) {
        List<Iterator<Tuple>> tableIterators = new ArrayList<Iterator<Tuple>>();

        for (Table table : tables) {
            tableIterators.add(table.ascendingIterator(key, snapshotId));
        }

        return new LatestTupleIterator(snapshotId, new MergingIterator<Tuple>(tableIterators));
    }

    public Iterator<Tuple> descendingIterator(Key key, long snapshotId) {
        List<Iterator<Tuple>> tableIterators = new ArrayList<Iterator<Tuple>>();

        for (Table table : tables) {
            tableIterators.add(table.descendingIterator(key, snapshotId));
        }

        return new LatestTupleIterator(snapshotId, new MergingIterator<Tuple>(true, tableIterators));
    }

    public synchronized void close() throws IOException {
        tables.readLock();

        try {
            for (Table table : tables) {
                table.close();
            }
        } finally {
            tables.readUnlock();
        }
    }

    @Override
    public Iterator<Tuple> iterator() {
        return ascendingIterator(Long.MAX_VALUE);
    }
}
