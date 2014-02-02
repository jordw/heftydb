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

package com.jordanwilliams.heftydb.write;

import com.jordanwilliams.heftydb.log.WriteLog;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.record.Snapshot;
import com.jordanwilliams.heftydb.record.Value;
import com.jordanwilliams.heftydb.state.State;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.table.file.FileTable;
import com.jordanwilliams.heftydb.table.memory.MemoryTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RecordWriter {

    private final State state;
    private final ExecutorService tableExecutor;

    private MemoryTable memoryTable;
    private WriteLog writeLog;

    public RecordWriter(State state) {
        this.state = state;
        this.tableExecutor = Executors.newFixedThreadPool(state.config().tableWriterThreads());
    }

    public Snapshot write(ByteBuffer key, ByteBuffer value) throws IOException {
        if (memoryTable == null || memoryTable.size() >= state.config().memoryTableSize()) {
            rotateMemoryTable();
        }

        long nextSnapshotId = state.snapshots().nextId();

        key.rewind();
        value.rewind();

        Key recordKey = new Key(key, nextSnapshotId);
        Value recordValue = new Value(value);
        Record record = new Record(recordKey, recordValue);

        writeLog.append(record);
        memoryTable.put(record);

        return new Snapshot(nextSnapshotId);
    }

    public void close() throws IOException {
        try {
            writeMemoryTable(memoryTable);
            tableExecutor.shutdown();
            tableExecutor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void rotateMemoryTable() throws IOException {
        if (memoryTable != null) {
            writeMemoryTable(memoryTable);
        }

        long nextTableId = state.tables().nextId();
        memoryTable = new MemoryTable(nextTableId);
        writeLog = WriteLog.open(nextTableId, state.paths());
        state.tables().add(memoryTable);
    }

    private void writeMemoryTable(final Table memoryTable){
        tableExecutor.submit(new FileTableWriter.Task(this.memoryTable.id(), 1, state.paths(), state.config(),
                this.memoryTable.ascendingIterator(Long.MAX_VALUE), this.memoryTable.recordCount(),
                new FileTableWriter.Task.Callback() {
                    @Override
                    public void finish() {
                        try {
                            state.tables().swap(FileTable.open(memoryTable.id(), state.paths(), state.caches()
                                    .recordBlockCache(), state.caches().indexBlockCache()), memoryTable);
                            Files.deleteIfExists(state.paths().logPath(memoryTable.id()));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }));
    }
}
