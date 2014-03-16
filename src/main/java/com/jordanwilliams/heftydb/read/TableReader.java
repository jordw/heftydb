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

package com.jordanwilliams.heftydb.read;

import com.codahale.metrics.Histogram;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.metrics.CacheHitGauge;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.state.Tables;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.util.CloseableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TableReader implements Iterable<Tuple> {

    private final TableAggregationIterator.Source ascendingIteratorSource = new TableAggregationIterator.Source() {
        @Override
        public CloseableIterator<Tuple> refresh(Key key, long snapshotId) {
            tables.readLock();

            try {
                List<CloseableIterator<Tuple>> tableIterators = new ArrayList<CloseableIterator<Tuple>>();

                for (Table table : tables) {
                    tableIterators.add(key == null ? table.ascendingIterator(snapshotId) : table.ascendingIterator(key,
                            snapshotId));
                }

                return new LatestTupleIterator(snapshotId, new MergingIterator<Tuple>(tableIterators));
            } finally {
                tables.readUnlock();
            }
        }
    };

    private final TableAggregationIterator.Source descendingIteratorSource = new TableAggregationIterator.Source() {
        @Override
        public CloseableIterator<Tuple> refresh(Key key, long snapshotId) {
            tables.readLock();

            try {
                List<CloseableIterator<Tuple>> tableIterators = new ArrayList<CloseableIterator<Tuple>>();

                for (Table table : tables) {
                    tableIterators.add(key == null ? table.descendingIterator(snapshotId) : table.descendingIterator(key,
                            snapshotId));
                }

                return new LatestTupleIterator(snapshotId, new MergingIterator<Tuple>(true, tableIterators));
            } finally {
                tables.readUnlock();
            }
        }
    };

    private final Tables tables;
    private final Metrics metrics;

    private final CacheHitGauge bloomFilterFalsePositiveRate;
    private final Histogram tablesConsultedHistogram;
    private final CacheHitGauge recordNotFoundRate;

    public TableReader(Tables tables, Metrics metrics) {
        this.tables = tables;
        this.metrics = metrics;

        this.bloomFilterFalsePositiveRate = metrics.hitGauge("read.bloomFilterFalsePositiveRate");
        this.tablesConsultedHistogram = metrics.histogram("read.tablesConsulted");
        this.recordNotFoundRate = metrics.hitGauge("read.recordNotFoundRate");
    }

    public Tuple get(Key key) {
        Tuple closestTuple = null;

        tables.readLock();

        int tablesConsulted = 0;

        try {
            for (Table table : tables) {
                if (table.mightContain(key)) {
                    Tuple tableTuple = table.get(key);
                    tablesConsulted++;

                    bloomFilterFalsePositiveRate.sample(tableTuple == null);

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

        tablesConsultedHistogram.update(tablesConsulted);
        recordNotFoundRate.sample(closestTuple == null);

        return closestTuple;
    }

    public CloseableIterator<Tuple> ascendingIterator(long snapshotId) {
        tables.readLock();

        try {
            List<CloseableIterator<Tuple>> tableIterators = new ArrayList<CloseableIterator<Tuple>>();

            for (Table table : tables) {
                tableIterators.add(table.ascendingIterator(snapshotId));
            }

            TableAggregationIterator tableAggregationIterator = new TableAggregationIterator(new MergingIterator<Tuple>
                    (tableIterators), snapshotId, tables, ascendingIteratorSource);

            return new LatestTupleIterator(snapshotId, tableAggregationIterator);
        } finally {
            tables.readUnlock();
        }
    }

    public CloseableIterator<Tuple> descendingIterator(long snapshotId) {
        tables.readLock();

        try {
            List<CloseableIterator<Tuple>> tableIterators = new ArrayList<CloseableIterator<Tuple>>();

            for (Table table : tables) {
                tableIterators.add(table.descendingIterator(snapshotId));
            }

            TableAggregationIterator tableAggregationIterator = new TableAggregationIterator(new MergingIterator<Tuple>
                    (true, tableIterators), snapshotId, tables, descendingIteratorSource);

            return new LatestTupleIterator(snapshotId, tableAggregationIterator);
        } finally {
            tables.readUnlock();
        }
    }

    public CloseableIterator<Tuple> ascendingIterator(Key key, long snapshotId) {
        tables.readLock();

        try {
            List<CloseableIterator<Tuple>> tableIterators = new ArrayList<CloseableIterator<Tuple>>();

            for (Table table : tables) {
                tableIterators.add(table.ascendingIterator(key, snapshotId));
            }

            TableAggregationIterator tableAggregationIterator = new TableAggregationIterator(new MergingIterator<Tuple>
                    (tableIterators), snapshotId, tables, ascendingIteratorSource);

            return new LatestTupleIterator(snapshotId, tableAggregationIterator);
        } finally {
            tables.readUnlock();
        }
    }

    public CloseableIterator<Tuple> descendingIterator(Key key, long snapshotId) {
        tables.readLock();

        try {
            List<CloseableIterator<Tuple>> tableIterators = new ArrayList<CloseableIterator<Tuple>>();

            for (Table table : tables) {
                tableIterators.add(table.descendingIterator(key, snapshotId));
            }

            TableAggregationIterator tableAggregationIterator = new TableAggregationIterator(new MergingIterator<Tuple>
                    (true, tableIterators), snapshotId, tables, descendingIteratorSource);

            return new LatestTupleIterator(snapshotId, tableAggregationIterator);
        } finally {
            tables.readUnlock();
        }
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
