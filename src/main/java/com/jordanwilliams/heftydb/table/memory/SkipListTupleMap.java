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

package com.jordanwilliams.heftydb.table.memory;

import com.jordanwilliams.heftydb.aggregate.LatestTupleIterator;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.util.CloseableIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SkipListTupleMap implements SortedTupleMap {

    private class TupleEntryIterator implements Iterator<Tuple> {

        private final Iterator<Map.Entry<Key, Value>> delegate;

        private TupleEntryIterator(Iterator<Map.Entry<Key, Value>> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Tuple next() {
            if (!delegate.hasNext()) {
                throw new NoSuchElementException();
            }

            Map.Entry<Key, Value> next = delegate.next();

            return new Tuple(next.getKey(), next.getValue());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private final ConcurrentNavigableMap<Key, Value> tuples = new ConcurrentSkipListMap<Key, Value>();

    @Override
    public void put(Key key, Value value) {
        tuples.put(key, value);
    }

    @Override
    public Tuple get(Key key) {
        Map.Entry<Key, Value> closestEntry = tuples.floorEntry(key);

        if (closestEntry == null) {
            return null;
        }

        return closestEntry.getKey().data().equals(key.data()) ? new Tuple(closestEntry.getKey(),
                closestEntry.getValue()) : null;
    }

    @Override
    public CloseableIterator<Tuple> ascendingIterator(long snapshotId) {
        return new LatestTupleIterator(snapshotId, new CloseableIterator.Wrapper<Tuple>(new TupleEntryIterator(tuples
                .entrySet().iterator())));
    }

    @Override
    public CloseableIterator<Tuple> descendingIterator(long snapshotId) {
        return new LatestTupleIterator(snapshotId, new CloseableIterator.Wrapper<Tuple>(new TupleEntryIterator(tuples
                .descendingMap().entrySet().iterator())));
    }

    @Override
    public CloseableIterator<Tuple> ascendingIterator(Key key, long snapshotId) {
        return new LatestTupleIterator(snapshotId, new CloseableIterator.Wrapper<Tuple>(new TupleEntryIterator(tuples
                .tailMap(key, true).entrySet().iterator())));
    }

    @Override
    public CloseableIterator<Tuple> descendingIterator(Key key, long snapshotId) {
        return new LatestTupleIterator(snapshotId, new CloseableIterator.Wrapper<Tuple>(new TupleEntryIterator(tuples
                .headMap(key, true).descendingMap().entrySet().iterator())));
    }

    @Override
    public Iterator<Tuple> iterator() {
        return new TupleEntryIterator(tuples.entrySet().iterator());
    }
}
