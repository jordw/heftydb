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

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.state.Tables;
import com.jordanwilliams.heftydb.util.CloseableIterator;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A CloseableIterator that merges streams of Tuples from multiple Tables. The Iterator is notified when the set of
 * tables in the database changes and refreshes its delegate iterator from a provided iterator source.
 */
public class TableAggregationIterator implements CloseableIterator<Tuple> {

    public interface Source {
        public CloseableIterator<Tuple> refresh(Key key, long snapshotId);
    }

    private final AtomicBoolean dirtySource = new AtomicBoolean();
    private final long snapshotId;
    private final Source source;
    private final Tables tables;
    private final Tables.ChangeHandler tableChangeHandler;

    private Key lastKey;
    private CloseableIterator<Tuple> delegate;

    public TableAggregationIterator(CloseableIterator<Tuple> initialSource, long snapshotId, Tables tables,
                                    Source source) {
        this.snapshotId = snapshotId;
        this.source = source;
        this.tables = tables;
        this.delegate = initialSource;
        this.tableChangeHandler = new Tables.ChangeHandler() {
            @Override
            public void changed() {
                dirtySource.set(true);
            }
        };

        tables.addChangeHandler(tableChangeHandler);
    }

    @Override
    public boolean hasNext() {
        tables.readLock();

        try {
            refreshSource();

            boolean hasNext = delegate.hasNext();

            if (!hasNext) {
                try {
                    close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            return delegate.hasNext();
        } finally {
            tables.readUnlock();
        }
    }

    @Override
    public Tuple next() {
        tables.readLock();

        try {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Tuple next = delegate.next();
            this.lastKey = next.key();
            return next;
        } finally {
            tables.readUnlock();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        tables.removeChangeHandler(tableChangeHandler);
    }

    private void refreshSource() {
        if (dirtySource.get()) {
            try {
                delegate.close();
                this.delegate = source.refresh(lastKey, snapshotId);

                //Advance past the previously seen key
                if (delegate.hasNext()) {
                    delegate.next();
                }

                dirtySource.set(false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
