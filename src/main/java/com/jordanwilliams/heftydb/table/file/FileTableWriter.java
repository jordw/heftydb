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

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.index.IndexRecord;
import com.jordanwilliams.heftydb.index.IndexWriter;
import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.state.Config;
import com.jordanwilliams.heftydb.state.Paths;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;

public class FileTableWriter {

    public static class Task implements Runnable {

        public interface Callback {
            public void finish();
        }

        private final long tableId;
        private final int level;
        private final Iterator<Tuple> records;
        private final long recordCount;
        private final Paths paths;
        private final Config config;
        private final Callback callback;

        public Task(long tableId, int level, Paths paths, Config config, Iterator<Tuple> records, long recordCount,
                    Callback callback) {
            this.tableId = tableId;
            this.level = level;
            this.paths = paths;
            this.config = config;
            this.records = records;
            this.recordCount = recordCount;
            this.callback = callback;
        }

        public Task(long tableId, int level, Paths paths, Config config, Iterator<Tuple> records, long recordCount) {
            this.tableId = tableId;
            this.level = level;
            this.paths = paths;
            this.config = config;
            this.records = records;
            this.recordCount = recordCount;
            this.callback = null;
        }

        @Override
        public void run() {
            try {
                FileTableWriter tableWriter = FileTableWriter.open(tableId, paths, recordCount,
                        config.indexBlockSize(), config.fileTableBlockSize(), level);

                while (records.hasNext()) {
                    tableWriter.write(records.next());
                }

                tableWriter.finish();

                Files.move(paths.tempPath(tableId), paths.tablePath(tableId), StandardCopyOption.ATOMIC_MOVE);

                if (callback != null) {
                    callback.finish();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final int maxRecordBlockSize;
    private final IndexWriter indexWriter;
    private final TableBloomFilterWriter filterWriter;
    private final TableTrailer.Builder trailerBuilder;
    private final DataFile tableDataFile;

    private TupleBlock.Builder recordBlockBuilder;

    private FileTableWriter(long tableId, IndexWriter indexWriter, TableBloomFilterWriter filterWriter,
                            DataFile tableDataFile, int maxRecordBlockSize, int level) throws IOException {
        this.indexWriter = indexWriter;
        this.filterWriter = filterWriter;
        this.recordBlockBuilder = new TupleBlock.Builder();
        this.maxRecordBlockSize = maxRecordBlockSize;
        this.trailerBuilder = new TableTrailer.Builder(tableId, level);
        this.tableDataFile = tableDataFile;
    }

    public void write(Tuple tuple) throws IOException {
        if (recordBlockBuilder.size() >= maxRecordBlockSize) {
            writeRecordBlock();
        }

        recordBlockBuilder.addRecord(tuple);
        filterWriter.write(tuple.key());
        trailerBuilder.put(tuple);
    }

    public void finish() throws IOException {
        writeRecordBlock();
        writeTrailer();
        filterWriter.finish();
        indexWriter.finish();
        tableDataFile.close();
    }

    private void writeRecordBlock() throws IOException {
        TupleBlock tupleBlock = recordBlockBuilder.build();
        ByteBuffer recordBlockBuffer = tupleBlock.memory().directBuffer();

        tableDataFile.appendInt(recordBlockBuffer.capacity());
        long recordBlockOffset = tableDataFile.append(recordBlockBuffer);
        recordBlockBuffer.rewind();
        tableDataFile.appendInt(recordBlockBuffer.capacity());

        Tuple startTuple = tupleBlock.first();
        indexWriter.write(new IndexRecord(startTuple.key(), recordBlockOffset, recordBlockBuffer.capacity()));
        tupleBlock.memory().release();
        recordBlockBuilder = new TupleBlock.Builder();
    }

    private void writeTrailer() throws IOException {
        ByteBuffer trailerBuffer = trailerBuilder.build().buffer();
        tableDataFile.append(trailerBuffer);
    }

    public static FileTableWriter open(long tableId, Paths paths, long approxRecordCount, int maxIndexBlockSize,
                                       int maxRecordBlockSize, int level) throws IOException {
        IndexWriter indexWriter = IndexWriter.open(tableId, paths, maxIndexBlockSize);
        TableBloomFilterWriter filterWriter = TableBloomFilterWriter.open(tableId, paths, approxRecordCount);
        DataFile tableDataFile = MutableDataFile.open(paths.tempPath(tableId));

        return new FileTableWriter(tableId, indexWriter, filterWriter, tableDataFile, maxRecordBlockSize, level);
    }
}
