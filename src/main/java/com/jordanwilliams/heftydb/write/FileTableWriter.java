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
import com.jordanwilliams.heftydb.state.State;
import com.jordanwilliams.heftydb.table.file.IndexRecord;
import com.jordanwilliams.heftydb.table.file.RecordBlock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileTableWriter {

    public static class Task implements Runnable {

        private final long tableId;
        private final int level;
        private final Iterator<Record> records;
        private final long recordCount;
        private final State state;

        public Task(long tableId, int level, State state, Iterator<Record> records, long recordCount) {
            this.tableId = tableId;
            this.level = level;
            this.state = state;
            this.records = records;
            this.recordCount = recordCount;
        }

        @Override
        public void run() {
            try {
                FileTableWriter tableWriter = FileTableWriter.open(tableId, state.files(), recordCount,
                        state.config().indexBlockSize(), state.config().fileTableBlockSize(), level);

                while (records.hasNext()){
                    tableWriter.write(records.next());
                }

                tableWriter.finish();
            } catch (IOException e){
                throw new RuntimeException(e);
            }
        }
    }

    private final long tableId;
    private final int maxRecordBlocksize;
    private final int level;
    private final List<Long> recordBlockOffsets = new ArrayList<Long>();
    private final Paths paths;
    private final IndexWriter indexWriter;
    private final FilterWriter filterWriter;
    private final MetaTableWriter metaWriter;
    private final DataFile tableDataFile;

    private RecordBlock.Builder recordBlockBuilder;

    private FileTableWriter(long tableId, Paths paths, long approxRecordCount, int maxIndexBlocksize, int maxRecordBlocksize, int level) throws IOException {
        this.tableId = tableId;
        this.paths = paths;
        this.level = level;
        this.indexWriter = IndexWriter.open(tableId, paths, maxRecordBlocksize);
        this.filterWriter = FilterWriter.open(tableId, paths, approxRecordCount);
        this.recordBlockBuilder = new RecordBlock.Builder();
        this.maxRecordBlocksize = maxRecordBlocksize;
        this.metaWriter = MetaTableWriter.open(tableId, paths, level);
        this.tableDataFile = MutableDataFile.open(paths.tablePath(tableId));
    }

    public void write(Record record) throws IOException {
        if (recordBlockBuilder.size() >= maxRecordBlocksize) {
            writeRecordBlock();
        }

        recordBlockBuilder.addRecord(record);
        filterWriter.write(record);
        metaWriter.write(record);
    }

    public void finish() throws IOException {
        writeRecordBlock();
        writeBlockOffsets();

        filterWriter.finish();
        indexWriter.finish();
        metaWriter.finish();
        tableDataFile.close();
    }

    private void writeRecordBlock() throws IOException {
        RecordBlock recordBlock = recordBlockBuilder.build();
        ByteBuffer recordBlockBuffer = recordBlock.memory().directBuffer();

        long recordBlockOffset = tableDataFile.appendInt(recordBlockBuffer.capacity());
        recordBlockOffsets.add(recordBlockOffset);
        recordBlockBuffer.rewind();
        tableDataFile.append(recordBlockBuffer);

        Record startRecord = recordBlock.startRecord();
        indexWriter.write(new IndexRecord(startRecord.key(), recordBlockOffset));
        recordBlock.memory().release();
        recordBlockBuilder = new RecordBlock.Builder();
    }

    private void writeBlockOffsets() throws IOException {
        for (long offset : recordBlockOffsets) {
            tableDataFile.appendLong(offset);
        }

        tableDataFile.appendInt(recordBlockOffsets.size());
    }

    public static FileTableWriter open(long tableId, Paths paths, long approxRecordCount, int maxIndexBlocksize, int maxRecordBlocksize, int level) throws IOException {
        return new FileTableWriter(tableId, paths, approxRecordCount, maxIndexBlocksize, maxRecordBlocksize, level);
    }
}
