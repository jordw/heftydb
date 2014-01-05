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

package com.jordanwilliams.heftydb.write;

import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.file.IndexBlock;
import com.jordanwilliams.heftydb.table.file.RecordBlock;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FileTableWriter {

    private final long tableId;
    private final int maxRecordBlockSizeBytes;
    private final int level;
    private final Paths paths;
    private final IndexWriter indexWriter;
    private final FilterWriter filterWriter;
    private final MetaTableWriter metaWriter;
    private final DataFile tableDataFile;

    private RecordBlock.Builder RecordBlockBuilder;

    private FileTableWriter(long tableId, Paths paths, long approxRecordCount, int maxRecordBlockSizeBytes, int level) throws IOException {
        this.tableId = tableId;
        this.paths = paths;
        this.level = level;
        this.indexWriter = IndexWriter.open(tableId, paths, maxRecordBlockSizeBytes);
        this.filterWriter = FilterWriter.open(tableId, paths, approxRecordCount);
        this.RecordBlockBuilder = new RecordBlock.Builder();
        this.maxRecordBlockSizeBytes = maxRecordBlockSizeBytes;
        this.metaWriter = MetaTableWriter.open(tableId, paths, level);
        this.tableDataFile = MutableDataFile.open(paths.tablePath(tableId));
    }

    public void write(Record record) throws IOException {
        if (RecordBlockBuilder.sizeBytes() >= maxRecordBlockSizeBytes) {
            writeRecordBlock();
        }

        RecordBlockBuilder.addRecord(record);
        filterWriter.write(record);
        metaWriter.write(record);
    }

    public void finish() throws IOException {
        writeRecordBlock();
        tableDataFile.appendInt(level);
        filterWriter.finish();
        indexWriter.finish();
        metaWriter.finish();
        tableDataFile.close();
    }

    private void writeRecordBlock() throws IOException {
        RecordBlock RecordBlock = RecordBlockBuilder.build();
        ByteBuffer RecordBlockBuffer = RecordBlock.memory().toDirectBuffer();
        long RecordBlockOffset = tableDataFile.appendInt(RecordBlockBuffer.capacity());
        tableDataFile.append(RecordBlockBuffer);
        indexWriter.write(new IndexBlock.Record(RecordBlock.startRecord().key(), RecordBlockOffset));
        RecordBlock.releaseMemory();
        RecordBlockBuilder = new RecordBlock.Builder();
    }

    public static FileTableWriter open(long tableId, Paths paths, long approxRecordCount, int maxRecordBlockSizeBytes, int level) throws IOException {
        return new FileTableWriter(tableId, paths, approxRecordCount, maxRecordBlockSizeBytes, level);
    }
}
