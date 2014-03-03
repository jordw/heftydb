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

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.data.Value;
import com.jordanwilliams.heftydb.db.Config;
import com.jordanwilliams.heftydb.db.Snapshot;
import com.jordanwilliams.heftydb.io.Throttle;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.state.Caches;
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
    private final Throttle writeThrottle;

    private MemoryTable memoryTable;
    private CommitLogWriter commitLogWriter;

    public TableWriter(Config config, Paths paths, Tables tables, Snapshots snapshots, Caches caches, Metrics metrics) {
        this.config = config;
        this.paths = paths;
        this.tables = tables;
        this.snapshots = snapshots;
        this.caches = caches;
        this.metrics = metrics;
        this.writeThrottle = new Throttle(config.maxWriteRate());

        this.tableExecutor = new ThreadPoolExecutor(config.tableWriterThreads(), config.tableWriterThreads(),
                Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>(config.tableWriterThreads()),
                new ThreadFactoryBuilder().setNameFormat("Table writer thread %d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public synchronized Snapshot write(ByteBuffer key, ByteBuffer value, boolean fsync) throws IOException {
        if (memoryTable == null || memoryTable.size() >= config.memoryTableSize()) {
            rotateMemoryTable();
        }

        int valueCapacity = value == null ? 0 : value.capacity();
        writeThrottle.consume(key.capacity() + valueCapacity);
        long nextSnapshotId = snapshots.nextId();

        key.rewind();

        if (value != null) {
            value.rewind();
        }

        Key recordKey = new Key(key, nextSnapshotId);
        Value recordValue = value == null ? Value.TOMBSTONE_VALUE : new Value(value);
        Tuple tuple = new Tuple(recordKey, recordValue);

        commitLogWriter.append(tuple, fsync);
        memoryTable.put(tuple);

        return new Snapshot(nextSnapshotId);
    }

    public void close() throws IOException {
        if (memoryTable != null) {
            commitLogWriter.close();
            writeMemoryTable(memoryTable);
        }

        tableExecutor.shutdownNow();
    }

    private void rotateMemoryTable() throws IOException {
        if (memoryTable != null) {
            commitLogWriter.close();
            writeMemoryTable(memoryTable);
        }

        long nextTableId = tables.nextId();
        memoryTable = new MemoryTable(nextTableId);
        commitLogWriter = CommitLogWriter.open(nextTableId, paths);
        tables.add(memoryTable);
    }

    private void writeMemoryTable(final Table tableToWrite) {
        final FileTableWriter.Task task = new FileTableWriter.Task.Builder().tableId(tableToWrite.id()).level(1)
                .paths(paths).config(config).source(tableToWrite.ascendingIterator(Long.MAX_VALUE)).tupleCount
                        (tableToWrite.tupleCount()).throttle(Throttle.MAX).callback(new FileTableWriter.Task.Callback
                        () {
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
        }).build();

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
