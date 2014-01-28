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

package com.jordanwilliams.heftydb.write;

import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.Paths;

import java.io.IOException;

public class MetaTableWriter {

    private final DataFile metaTableFile;
    private final long id;
    private final int level;

    private long maxSnapshotId;
    private long recordCount;
    private long size;

    private MetaTableWriter(long tableId, DataFile metaTableFile, int level) {
        this.metaTableFile = metaTableFile;
        this.level = level;
        this.id = tableId;
    }

    public void write(Record record) throws IOException {
        if (record.key().snapshotId() > maxSnapshotId) {
            maxSnapshotId = record.key().snapshotId();
        }

        recordCount++;
        size += record.size();
    }

    public void finish() throws IOException {
        metaTableFile.appendLong(id);
        metaTableFile.appendInt(level);
        metaTableFile.appendLong(maxSnapshotId);
        metaTableFile.appendLong(recordCount);
        metaTableFile.appendLong(size);
        metaTableFile.close();
    }

    public static MetaTableWriter open(long tableId, Paths paths, int level) throws IOException {
        DataFile metaTableFile = MutableDataFile.open(paths.metaPath(tableId));
        return new MetaTableWriter(tableId, metaTableFile, level);
    }
}
