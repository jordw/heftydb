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
import com.jordanwilliams.heftydb.table.file.DataBlock;
import com.jordanwilliams.heftydb.table.file.IndexBlock;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FileTableWriter {

    private final long tableId;
    private final int maxDataBlockSizeBytes;
    private final int level;
    private final Paths paths;
    private final IndexWriter indexWriter;
    private final FilterWriter filterWriter;
    private final DataFile tableDataFile;

    private DataBlock.Builder dataBlockBuilder;

    private FileTableWriter(long tableId, Paths paths, long approxRecordCount, int maxDataBlockSizeBytes, int level) throws IOException {
        this.tableId = tableId;
        this.paths = paths;
        this.level = level;
        this.indexWriter = IndexWriter.open(tableId, paths, maxDataBlockSizeBytes);
        this.filterWriter = FilterWriter.open(tableId, paths, approxRecordCount);
        this.dataBlockBuilder = new DataBlock.Builder();
        this.maxDataBlockSizeBytes = maxDataBlockSizeBytes;
        this.tableDataFile = MutableDataFile.open(paths.tablePath(tableId));
    }

    public void write(Record record) throws IOException {
        if (dataBlockBuilder.sizeBytes() >= maxDataBlockSizeBytes) {
            writeDataBlock();
        }

        dataBlockBuilder.addRecord(record);
        filterWriter.write(record);
    }

    public void finish() throws IOException {
        writeDataBlock();
        tableDataFile.appendInt(level);
        filterWriter.finish();
        indexWriter.finish();
        tableDataFile.close();
    }

    private void writeDataBlock() throws IOException {
        DataBlock dataBlock = dataBlockBuilder.build();
        ByteBuffer dataBlockBuffer = dataBlock.memory().toDirectBuffer();
        long dataBlockOffset = tableDataFile.appendInt(dataBlockBuffer.capacity());
        tableDataFile.append(dataBlockBuffer);
        indexWriter.write(new IndexBlock.Record(dataBlock.startKey(), dataBlockOffset));
        dataBlock.releaseMemory();
        dataBlockBuilder = new DataBlock.Builder();
    }

    public static FileTableWriter open(long tableId, Paths paths, long approxRecordCount, int maxDataBlockSizeBytes, int level) throws IOException {
        return new FileTableWriter(tableId, paths, approxRecordCount, maxDataBlockSizeBytes, level);
    }
}
