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

package com.jordanwilliams.heftydb.table.file;

import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.table.Table;

import java.util.Iterator;

public class FileTable implements Table {

    @Override
    public long id() {
        return 0;
    }

    @Override
    public boolean mightContain(Key key) {
        return false;
    }

    @Override
    public Record get(Key key, long snapshotId) {
        return null;
    }

    @Override
    public Iterator<Record> iterator(IterationDirection direction, long snapshotId) {
        return null;
    }

    @Override
    public Iterator<Record> iteratorFrom(Key key, IterationDirection direction, long sn) {
        return null;
    }

    @Override
    public long recordCount() {
        return 0;
    }

    @Override
    public long sizeBytes() {
        return 0;
    }

    @Override
    public int level() {
        return 0;
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public Iterator<Record> iterator() {
        return null;
    }
}
