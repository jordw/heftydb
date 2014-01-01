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

import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.DataFiles;
import com.jordanwilliams.heftydb.table.Table;

import java.io.IOException;
import java.util.Iterator;

public class FileTable implements Table {

    private final long tableId;
    private final Index index;
    private final DataFile tableFile;

    private FileTable(long tableId, DataFiles dataFiles) throws IOException {
        this.tableId = tableId;
        this.index = Index.open(tableId, dataFiles);
        this.tableFile = MutableDataFile.open(dataFiles.tablePath(tableId));
    }

    @Override
    public long id() {
        return tableId;
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

    public static FileTable open(long tableId, DataFiles dataFiles) throws IOException {
        return new FileTable(tableId, dataFiles);
    }
}
