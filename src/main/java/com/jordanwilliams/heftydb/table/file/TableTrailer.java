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
import com.jordanwilliams.heftydb.record.Record;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TableTrailer {

    public static final int SIZE = 28;

    public static class Builder {

        private final long tableId;
        private final int level;
        private long recordCount;
        private long maxSnapshotId;

        public Builder(long tableId, int level) {
            this.tableId = tableId;
            this.level = level;
        }

        public void put(Record record){
            maxSnapshotId = Math.max(record.key().snapshotId(), maxSnapshotId);
            recordCount++;
        }

        public TableTrailer build(){
            return new TableTrailer(serialize());
        }

        private ByteBuffer serialize(){
            ByteBuffer trailerBuffer = ByteBuffer.allocate(SIZE);
            trailerBuffer.putLong(tableId);
            trailerBuffer.putInt(level);
            trailerBuffer.putLong(recordCount);
            trailerBuffer.putLong(maxSnapshotId);
            trailerBuffer.rewind();
            return trailerBuffer;
        }
    }

    private final ByteBuffer buffer;
    private final long tableId;
    private final int level;
    private final long recordCount;
    private final long maxSnapshotId;

    public TableTrailer(ByteBuffer buffer){
        this.tableId = buffer.getLong();
        this.level = buffer.getInt();
        this.recordCount = buffer.getLong();
        this.maxSnapshotId = buffer.getLong();
        buffer.rewind();
        this.buffer = buffer;
    }

    public long tableId() {
        return tableId;
    }

    public long maxSnapshotId() {
        return maxSnapshotId;
    }

    public long recordCount() {
        return recordCount;
    }

    public int level() {
        return level;
    }

    public ByteBuffer buffer(){
        return buffer;
    }

    public static TableTrailer read(DataFile tableFile) throws IOException {
        ByteBuffer trailerBuffer = ByteBuffer.allocate(SIZE);
        tableFile.read(trailerBuffer, tableFile.size() - SIZE);
        trailerBuffer.rewind();
        return new TableTrailer(trailerBuffer);
    }
}
