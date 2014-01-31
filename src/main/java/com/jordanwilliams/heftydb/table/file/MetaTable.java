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

import java.io.IOException;
import java.nio.ByteBuffer;

public class MetaTable {

    private static final int SIZE = 36;

    private final long id;
    private final int level;
    private final long maxSnapshotId;
    private final long recordCount;
    private final long size;

    public MetaTable(long id, int level, long maxSnapshotId, long recordCount, long size) {
        this.id = id;
        this.level = level;
        this.maxSnapshotId = maxSnapshotId;
        this.recordCount = recordCount;
        this.size = size;
    }

    public long size() {
        return size;
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
        DataFile metaTableFile = MutableDataFile.open(paths.metaPath(tableId));
        ByteBuffer metaTableBuffer = ByteBuffer.allocate(MetaTable.SIZE);
        metaTableFile.read(metaTableBuffer, 0);
        metaTableFile.close();
        metaTableBuffer.rewind();

        long id = metaTableBuffer.getLong();
        int level = metaTableBuffer.getInt();
        long maxSnapshotId = metaTableBuffer.getLong();
        long recordCount = metaTableBuffer.getLong();
        long size = metaTableBuffer.getLong();

        return new MetaTable(id, level, maxSnapshotId, recordCount, size);
    }
}
