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
import com.jordanwilliams.heftydb.db.Config;
import com.jordanwilliams.heftydb.index.IndexRecord;
import com.jordanwilliams.heftydb.index.IndexWriter;
import com.jordanwilliams.heftydb.io.ChannelDataFile;
import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.state.Paths;
import org.isomorphism.util.TokenBucket;
import org.isomorphism.util.TokenBuckets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class FileTableWriter {

    public static class Task implements Runnable {

        public static class Builder {

            private long tableId;
            private int level = 1;
            private Iterator<Tuple> source;
            private long tupleCount;
            private Paths paths;
            private Config config;
            private Callback callback;
            private long rateLimitRate = Long.MAX_VALUE;

            public Builder tableId(long tableId) {
                this.tableId = tableId;
                return this;
            }

            public Builder level(int level) {
                this.level = level;
                return this;
            }

            public Builder source(Iterator<Tuple> source) {
                this.source = source;
                return this;
            }

            public Builder tupleCount(long tupleCount) {
                this.tupleCount = tupleCount;
                return this;
            }

            public Builder paths(Paths paths) {
                this.paths = paths;
                return this;
            }

            public Builder config(Config config) {
                this.config = config;
                return this;
            }

            public Builder callback(Callback callback) {
                this.callback = callback;
                return this;
            }

            public Builder maxWriteRate(long rateLimitRate) {
                this.rateLimitRate = rateLimitRate;
                return this;
            }

            public Task build() {
                return new Task(tableId, level, paths, config, source, tupleCount, callback, rateLimitRate);
            }
        }

        public interface Callback {
            public void finish();
        }

        private final long tableId;
        private final int level;
        private final Iterator<Tuple> tuples;
        private final long tupleCount;
        private final Paths paths;
        private final Config config;
        private final Callback callback;
        private final long rateLimitRate;

        public Task(long tableId, int level, Paths paths, Config config, Iterator<Tuple> tuples, long tupleCount,
                    Callback callback, long rateLimitRate) {
            this.tableId = tableId;
            this.level = level;
            this.paths = paths;
            this.config = config;
            this.tuples = tuples;
            this.tupleCount = tupleCount;
            this.callback = callback;
            this.rateLimitRate = rateLimitRate;
        }

        @Override
        public void run() {
            try {
                FileTableWriter tableWriter = FileTableWriter.open(tableId, paths, tupleCount,
                        config.indexBlockSize(), config.tableBlockSize(), level);

                TokenBucket rateBucket = TokenBuckets.newFixedIntervalRefill(rateLimitRate, rateLimitRate, 1,
                        TimeUnit.SECONDS);

                while (tuples.hasNext()) {
                    Tuple tuple = tuples.next();
                    tableWriter.write(tuple);
                    rateBucket.consume(tuple.size());
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
        DataFile tableDataFile = ChannelDataFile.open(paths.tempPath(tableId));

        return new FileTableWriter(tableId, indexWriter, filterWriter, tableDataFile, maxRecordBlockSize, level);
    }
}
