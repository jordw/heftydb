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

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.util.CloseableIterator;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * An Iterator that filters a sorted stream of Tuples, and returns only a single Tuple for each unique Key in the
 * stream that is the latest version of that particular Tuple according to the snapshot id associated with each key.
 */
public class LatestTupleIterator implements CloseableIterator<Tuple> {

    private final CloseableIterator<Tuple> tupleIterator;
    private final Queue<Tuple> nextTuple = new LinkedList<Tuple>();
    private final long maxSnapshotId;
    private final SortedSet<Tuple> currentKeyTuples = new TreeSet<Tuple>();

    public LatestTupleIterator(long maxSnapshotId, CloseableIterator<Tuple> tupleIterator) {
        this.maxSnapshotId = maxSnapshotId;
        this.tupleIterator = tupleIterator;
    }

    @Override
    public boolean hasNext() {
        if (!nextTuple.isEmpty()) {
            return true;
        }

        Tuple tuple = fetchNextRecord();

        if (tuple == null) {
            return false;
        }

        nextTuple.add(tuple);

        return true;
    }

    @Override
    public Tuple next() {
        if (nextTuple.isEmpty()) {
            hasNext();
        }

        return nextTuple.poll();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private Tuple fetchNextRecord() {
        while (tupleIterator.hasNext()) {
            Tuple next = tupleIterator.next();

            if (next.key().snapshotId() > maxSnapshotId) {
                continue;
            }

            boolean nextKeyEqualCurrent = currentKeyTuples.isEmpty() || next.key().data().equals(currentKeyTuples
                    .last().key().data());

            if (nextKeyEqualCurrent) {
                currentKeyTuples.add(next);
                continue;
            }

            Tuple newest = currentKeyTuples.last();
            currentKeyTuples.clear();
            currentKeyTuples.add(next);
            return newest;
        }

        if (currentKeyTuples.isEmpty()) {
            return null;
        }

        Tuple newest = currentKeyTuples.last();
        currentKeyTuples.clear();
        return newest;
    }

    @Override
    public void close() throws IOException {
        tupleIterator.close();
    }
}
