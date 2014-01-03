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

package com.jordanwilliams.heftydb.table.file;

import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.util.Sizes;

import java.io.IOException;

public class MetaTable {

    private final long id;
    private final int level;
    private final long maxSnapshotId;
    private final long recordCount;
    private final long sizeBytes;

    private MetaTable(long tableId, Paths paths) throws IOException {
        DataFile metaTableFile = MutableDataFile.open(paths.metaPath(tableId));
        int filePosition = 0;

        this.id = metaTableFile.readLong(filePosition);
        filePosition += Sizes.LONG_SIZE;

        this.level = metaTableFile.readInt(filePosition);
        filePosition += Sizes.INT_SIZE;

        this.maxSnapshotId = metaTableFile.readLong(filePosition);
        filePosition += Sizes.LONG_SIZE;

        this.recordCount = metaTableFile.readLong(filePosition);
        filePosition += Sizes.LONG_SIZE;

        this.sizeBytes = metaTableFile.readLong(filePosition);
        metaTableFile.close();
    }

    public long sizeBytes() {
        return sizeBytes;
    }

    public long recordCount() {
        return recordCount;
    }

    public long maxSnapshotId() {
        return maxSnapshotId;
    }

    public int level() {
        return level;
    }

    public long id() {
        return id;
    }

    public static MetaTable open(long tableId, Paths paths) throws IOException {
        return new MetaTable(tableId, paths);
    }
}
