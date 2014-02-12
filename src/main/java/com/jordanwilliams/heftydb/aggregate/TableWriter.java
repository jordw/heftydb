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

package com.jordanwilliams.heftydb.aggregate;

import com.codahale.metrics.Timer;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.db.Config;
import com.jordanwilliams.heftydb.db.Snapshot;
import com.jordanwilliams.heftydb.log.WriteLog;
import com.jordanwilliams.heftydb.state.Caches;
import com.jordanwilliams.heftydb.state.Metrics;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.state.Snapshots;
import com.jordanwilliams.heftydb.state.Tables;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.table.file.FileTable;
import com.jordanwilliams.heftydb.table.file.FileTableWriter;
import com.jordanwilliams.heftydb.table.memory.MemoryTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TableWriter {

    private final Config config;
    private final Snapshots snapshots;
    private final ThreadPoolExecutor tableExecutor;
    private final Tables tables;
    private final Paths paths;
    private final Caches caches;
    private final Metrics metrics;

    private MemoryTable memoryTable;
    private WriteLog writeLog;

    public TableWriter(Config config, Paths paths, Tables tables, Snapshots snapshots, Caches caches,
                       Metrics metrics) {
        this.config = config;
        this.paths = paths;
        this.tables = tables;
        this.snapshots = snapshots;
        this.caches = caches;
        this.metrics = metrics;

        this.tableExecutor = new ThreadPoolExecutor(config.tableWriterThreads(), config.tableWriterThreads(),
                Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>(config.tableWriterThreads()),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public synchronized Snapshot write(ByteBuffer key, ByteBuffer value, boolean fsync) throws IOException {
        if (memoryTable == null || memoryTable.size() >= config.memoryTableSize()) {
            rotateMemoryTable();
        }

        long nextSnapshotId = snapshots.nextId();

        key.rewind();
        value.rewind();

        Key recordKey = new Key(key, nextSnapshotId);
        Value recordValue = new Value(value);
        Tuple tuple = new Tuple(recordKey, recordValue);

        writeLog.append(tuple, fsync);
        memoryTable.put(tuple);

        return new Snapshot(nextSnapshotId);
    }

    public void close() throws IOException {
        try {
            if (memoryTable != null) {
                writeLog.close();
                writeMemoryTable(memoryTable);
            }

            tableExecutor.shutdown();
            tableExecutor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void rotateMemoryTable() throws IOException {
        if (memoryTable != null) {
            writeLog.close();
            writeMemoryTable(memoryTable);
        }

        long nextTableId = tables.nextId();
        memoryTable = new MemoryTable(nextTableId);
        writeLog = WriteLog.open(nextTableId, paths);
        tables.add(memoryTable);
    }

    private void writeMemoryTable(final Table tableToWrite) {
        final FileTableWriter.Task task = new FileTableWriter.Task(tableToWrite.id(), 1, paths, config,
                tableToWrite.ascendingIterator(Long.MAX_VALUE), tableToWrite.tupleCount(),
                new FileTableWriter.Task.Callback() {
            @Override
            public void finish() {
                try {
                    tables.swap(FileTable.open(tableToWrite.id(), paths, caches.recordBlockCache(),
                            caches.indexBlockCache(), metrics), tableToWrite);
                    Files.deleteIfExists(paths.logPath(tableToWrite.id()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        tableExecutor.execute(new Runnable() {
            @Override
            public void run() {
                Timer.Context watch = metrics.timer("write.memoryTableSerialize").time();
                task.run();
                watch.stop();
            }
        });

        metrics.histogram("write.concurrentMemoryTableSerializers").update(tableExecutor.getActiveCount());
    }
}
