/*
 * Copyright (c) 2013. Jordan Williams
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

import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;

import java.util.Iterator;

public interface Table extends Iterable<Record> {

    public enum IterationDirection {
        ASCENDING, DESCENDING
    }

    public long id();

    public boolean mightContain(Key key);

    public Record get(Key key);

    public Iterator<Record> ascendingIterator(long snapshotId);

    public Iterator<Record> descendingIterator(long snapshotId);

    public Iterator<Record> ascendingIterator(Key key, long snapshotId);

    public Iterator<Record> descendingIterator(Key key, long snapshotId);

    public long recordCount();

    public long size();

    public int level();

    public boolean isPersistent();
}
