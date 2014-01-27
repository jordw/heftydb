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
import com.jordanwilliams.heftydb.table.file.FileTable;
import com.jordanwilliams.heftydb.table.file.IndexRecord;
import com.jordanwilliams.heftydb.table.file.RecordBlock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileTableWriter {

    public static class Task implements Runnable {

        public interface Callback {

            public static Callback NO_OP = new Callback() {
                @Override
                public void finish() {

                }
            };

            public void finish();
        }

        private final long tableId;
        private final int level;
        private final Iterator<Record> records;
        private final long recordCount;
        private final State state;
        private final Callback callback;

        public Task(long tableId, int level, State state, Iterator<Record> records, long recordCount,
                    Callback callback) {
            this.tableId = tableId;
            this.level = level;
            this.state = state;
            this.records = records;
            this.recordCount = recordCount;
            this.callback = callback;
        }

        public Task(long tableId, int level, State state, Iterator<Record> records, long recordCount) {
            this.tableId = tableId;
            this.level = level;
            this.state = state;
            this.records = records;
            this.recordCount = recordCount;
            this.callback = Callback.NO_OP;
        }

        @Override
        public void run() {
            try {
                FileTableWriter tableWriter = FileTableWriter.open(tableId, state.files(), recordCount,
                        state.config().indexBlockSize(), state.config().fileTableBlockSize(), level);

                while (records.hasNext()) {
                    tableWriter.write(records.next());
                }

                tableWriter.finish();

                callback.finish();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final long tableId;
    private final int maxRecordBlocksize;
    private final int level;
    private final List<FileTable.RecordBlockDescriptor> recordBlockDescriptors = new ArrayList<FileTable
            .RecordBlockDescriptor>();
    private final Paths paths;
    private final IndexWriter indexWriter;
    private final FilterWriter filterWriter;
    private final MetaTableWriter metaWriter;
    private final DataFile tableDataFile;

    private RecordBlock.Builder recordBlockBuilder;

    private FileTableWriter(long tableId, Paths paths, long approxRecordCount, int maxIndexBlocksize,
                            int maxRecordBlocksize, int level) throws IOException {
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

        long recordBlockOffset = tableDataFile.append(recordBlockBuffer);

        recordBlockDescriptors.add(new FileTable.RecordBlockDescriptor(recordBlockOffset,
                recordBlockBuffer.capacity()));
        recordBlockBuffer.rewind();

        Record startRecord = recordBlock.startRecord();
        indexWriter.write(new IndexRecord(startRecord.key(), recordBlockOffset, recordBlockBuffer.capacity()));
        recordBlock.memory().release();
        recordBlockBuilder = new RecordBlock.Builder();
    }

    private void writeBlockOffsets() throws IOException {
        for (FileTable.RecordBlockDescriptor descriptor : recordBlockDescriptors) {
            tableDataFile.appendLong(descriptor.offset());
            tableDataFile.appendInt(descriptor.size());
        }

        tableDataFile.appendInt(recordBlockDescriptors.size());
    }

    public static FileTableWriter open(long tableId, Paths paths, long approxRecordCount, int maxIndexBlocksize, int maxRecordBlocksize, int level) throws IOException {
        return new FileTableWriter(tableId, paths, approxRecordCount, maxIndexBlocksize, maxRecordBlocksize, level);
    }
}
