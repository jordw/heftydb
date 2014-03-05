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

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.read.LatestTupleIterator;
import com.jordanwilliams.heftydb.util.CloseableIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SkipListTupleMap implements SortedTupleMap {

    private final ConcurrentNavigableMap<Key, Tuple> tuples = new ConcurrentSkipListMap<Key, Tuple>();

    @Override
    public void put(Key key, Value value) {
        tuples.put(key, new Tuple(key, value));
    }

    @Override
    public Tuple get(Key key) {
        Map.Entry<Key, Tuple> closestEntry = tuples.floorEntry(key);

        if (closestEntry == null) {
            return null;
        }

        return closestEntry.getKey().data().equals(key.data()) ? closestEntry.getValue() : null;
    }

    @Override
    public CloseableIterator<Tuple> ascendingIterator(long snapshotId) {
        return new LatestTupleIterator(snapshotId, new CloseableIterator.Wrapper<Tuple>(tuples
                .values().iterator()));
    }

    @Override
    public CloseableIterator<Tuple> descendingIterator(long snapshotId) {
        return new LatestTupleIterator(snapshotId, new CloseableIterator.Wrapper<Tuple>(tuples
                .descendingMap().values().iterator()));
    }

    @Override
    public CloseableIterator<Tuple> ascendingIterator(Key key, long snapshotId) {
        return new LatestTupleIterator(snapshotId, new CloseableIterator.Wrapper<Tuple>(tuples
                .tailMap(key, true).values().iterator()));
    }

    @Override
    public CloseableIterator<Tuple> descendingIterator(Key key, long snapshotId) {
        return new LatestTupleIterator(snapshotId, new CloseableIterator.Wrapper<Tuple>(tuples
                .headMap(key, true).descendingMap().values().iterator()));
    }

    @Override
    public Iterator<Tuple> iterator() {
        return tuples.values().iterator();
    }
}
