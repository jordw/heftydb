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
 * An Iterator that filters a sorted stream of Tuples, and filters out all key versions older than a minimum snapshot
 * id, or passes along a key if there is only one version of it
 */
public class CompactionTupleIterator implements CloseableIterator<Tuple> {

    private final CloseableIterator<Tuple> tupleIterator;
    private final Queue<Tuple> nextTuples = new LinkedList<Tuple>();
    private final SortedSet<Tuple> currentKeyTuples = new TreeSet<Tuple>();
    private final long minSnapshotId;

    public CompactionTupleIterator(long minSnapshotId, CloseableIterator<Tuple> tupleIterator) {
        this.minSnapshotId = minSnapshotId;
        this.tupleIterator = tupleIterator;
    }

    @Override
    public boolean hasNext() {
        if (!nextTuples.isEmpty()) {
            return true;
        }

        if (!fetchNextTuples()){
            return false;
        }

        return true;
    }

    @Override
    public Tuple next() {
        if (nextTuples.isEmpty()) {
            hasNext();
        }

        return nextTuples.poll();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        tupleIterator.close();
    }

    private boolean fetchNextTuples() {
        while (tupleIterator.hasNext()) {
            Tuple next = tupleIterator.next();

            boolean nextKeyEqualCurrent = currentKeyTuples.isEmpty() || next.key().data().equals(currentKeyTuples
                    .last().key().data());

            if (nextKeyEqualCurrent) {
                currentKeyTuples.add(next);
                continue;
            }

            filterCurrentKeyTuples();
            currentKeyTuples.clear();
            currentKeyTuples.add(next);

            return true;
        }

        if (currentKeyTuples.isEmpty()) {
            return false;
        }

        filterCurrentKeyTuples();
        currentKeyTuples.clear();
        return true;
    }

    private void filterCurrentKeyTuples(){
        int count = 0;

        for (Tuple tuple : currentKeyTuples){
            if (tuple.key().snapshotId() >= minSnapshotId){
                nextTuples.add(tuple);
                count++;
            }
        }

        if (count == 0){
            nextTuples.add(currentKeyTuples.last());
        }
    }
}
