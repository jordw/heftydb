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

package com.jordanwilliams.heftydb.state;

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.index.IndexBlock;
import com.jordanwilliams.heftydb.log.WriteLog;
import com.jordanwilliams.heftydb.table.MutableTable;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.table.file.DataBlock;
import com.jordanwilliams.heftydb.table.file.FileTable;
import com.jordanwilliams.heftydb.table.file.FileTableWriter;
import com.jordanwilliams.heftydb.table.memory.MemoryTable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class StateInitializer {

    private final Config config;
    private final Paths paths;
    private final Caches caches;
    private long maxSnapshotId;

    public StateInitializer(Config config) {
        this.config = config;
        this.paths = new Paths(config.tableDirectory(), config.logDirectory());
        this.caches = new Caches(new DataBlock.Cache(config.tableCacheSize()),
                new IndexBlock.Cache(config.indexCacheSize()));
    }

    public State initialize() throws IOException {
        writeTablesFromLogs();
        List<Table> tables = loadTables();
        return new State(tables, config, paths, caches, maxSnapshotId);
    }

    private List<Table> loadTables() throws IOException {
        List<Table> tables = new ArrayList<Table>();
        Set<Path> metaTablePaths = paths.tableFilePaths();

        for (Path path : metaTablePaths) {
            long tableId = tableId(path);
            Table table = FileTable.open(tableId, paths, caches.recordBlockCache(), caches.indexBlockCache());
            maxSnapshotId = Math.max(table.maxSnapshotId(), maxSnapshotId);
            tables.add(table);
        }

        return tables;
    }

    private void writeTablesFromLogs() throws IOException {
        Set<Path> logPaths = paths.logFilePaths();

        for (Path path : logPaths) {
            long tableId = tableId(path);

            WriteLog log = WriteLog.open(tableId, paths);
            Table memoryTable = logTable(log);
            log.close();

            FileTableWriter.Task tableWriterTask = new FileTableWriter.Task(tableId, 1, paths, config,
                    memoryTable.ascendingIterator(Long.MAX_VALUE), memoryTable.recordCount());

            tableWriterTask.run();

            Files.deleteIfExists(paths.logPath(tableId));
        }
    }

    private Table logTable(WriteLog log) {
        MutableTable memoryTable = new MemoryTable(log.tableId());

        for (Tuple tuple : log) {
            memoryTable.put(tuple);
        }

        return memoryTable;
    }

    private static long tableId(Path path) {
        String fileName = path.getFileName().toString();
        String id = fileName.split("\\.")[0];
        return Long.parseLong(id);
    }
}
