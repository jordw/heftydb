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

package com.jordanwilliams.heftydb.table;

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;

import java.util.Iterator;

public interface Table extends Iterable<Tuple>, Comparable<Table> {

    public long id();

    public boolean mightContain(Key key);

    public Tuple get(Key key);

    public Iterator<Tuple> ascendingIterator(long snapshotId);

    public Iterator<Tuple> descendingIterator(long snapshotId);

    public Iterator<Tuple> ascendingIterator(Key key, long snapshotId);

    public Iterator<Tuple> descendingIterator(Key key, long snapshotId);

    public long tupleCount();

    public long size();

    public int level();

    public long maxSnapshotId();

    public void close();

    public boolean isPersistent();
}
