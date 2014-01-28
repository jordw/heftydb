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
import com.jordanwilliams.heftydb.util.Sizes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

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
    private final int maxRecordBlockSize;
    private final int level;
    private final IndexWriter indexWriter;
    private final TableBloomFilterWriter filterWriter;
    private final MetaTableWriter metaWriter;
    private final DataFile tableDataFile;

    private RecordBlock.Builder recordBlockBuilder;

    private FileTableWriter(long tableId, IndexWriter indexWriter, TableBloomFilterWriter filterWriter,
                            MetaTableWriter metaWriter, DataFile tableDataFile, int maxRecordBlockSize,
                            int level) throws IOException {
        this.tableId = tableId;
        this.level = level;
        this.indexWriter = indexWriter;
        this.filterWriter = filterWriter;
        this.recordBlockBuilder = new RecordBlock.Builder();
        this.maxRecordBlockSize = maxRecordBlockSize;
        this.metaWriter = metaWriter;
        this.tableDataFile = tableDataFile;
    }

    public void write(Record record) throws IOException {
        if (recordBlockBuilder.size() >= maxRecordBlockSize) {
            writeRecordBlock(false);
        }

        recordBlockBuilder.addRecord(record);
        filterWriter.write(record);
        metaWriter.write(record);
    }

    public void finish() throws IOException {
        writeRecordBlock(true);
        filterWriter.finish();
        indexWriter.finish();
        metaWriter.finish();
        tableDataFile.close();
    }

    private void writeRecordBlock(boolean lastBlock) throws IOException {
        RecordBlock recordBlock = recordBlockBuilder.build();
        ByteBuffer recordBlockBuffer = recordBlock.memory().directBuffer();

        tableDataFile.appendInt(recordBlockBuffer.capacity());
        long recordBlockOffset = tableDataFile.append(recordBlockBuffer);
        recordBlockBuffer.rewind();

        if (lastBlock) {
            tableDataFile.appendLong(recordBlockOffset - Sizes.INT_SIZE);
        }

        Record startRecord = recordBlock.startRecord();
        indexWriter.write(new IndexRecord(startRecord.key(), recordBlockOffset, recordBlockBuffer.capacity()));
        recordBlock.memory().release();
        recordBlockBuilder = new RecordBlock.Builder();
    }

    public static FileTableWriter open(long tableId, Paths paths, long approxRecordCount, int maxIndexBlockSize,
                                       int maxRecordBlockSize, int level) throws IOException {
        IndexWriter indexWriter = IndexWriter.open(tableId, paths, maxIndexBlockSize);
        TableBloomFilterWriter filterWriter = TableBloomFilterWriter.open(tableId, paths, approxRecordCount);
        MetaTableWriter metaWriter = MetaTableWriter.open(tableId, paths, level);
        DataFile tableDataFile = MutableDataFile.open(paths.tablePath(tableId));

        return new FileTableWriter(tableId, indexWriter, filterWriter, metaWriter, tableDataFile, maxRecordBlockSize,
                level);
    }
}
