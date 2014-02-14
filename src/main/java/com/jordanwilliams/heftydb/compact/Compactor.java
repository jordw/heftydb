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

package com.jordanwilliams.heftydb.compact;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.Futures;
import com.jordanwilliams.heftydb.aggregate.MergingIterator;
import com.jordanwilliams.heftydb.compact.planner.CompactionPlanner;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.db.Config;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.state.Caches;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.state.Tables;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.table.file.FileTable;
import com.jordanwilliams.heftydb.table.file.FileTableWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Compactor {

    private class Task implements Runnable {

        private final CompactionTask compactionTask;

        private Task(CompactionTask compactionTask) {
            this.compactionTask = compactionTask;
        }

        @Override
        public void run() {
            try {
                Timer.Context watch = metrics.timer("compactor.taskExecution").time();
                List<Iterator<Tuple>> tableIterators = new ArrayList<Iterator<Tuple>>();
                long tupleCount = 0;
                long nextTableId = tables.nextId();

                for (Table table : compactionTask.tables()) {
                    tableIterators.add(table.ascendingIterator(Long.MAX_VALUE));
                    tupleCount += table.tupleCount();
                }

                Iterator<Tuple> mergedIterator = new MergingIterator<Tuple>(tableIterators);

                FileTableWriter.Task writerTask = new FileTableWriter.Task.Builder().tableId(nextTableId).config
                        (config).paths(paths).level(compactionTask.level()).tupleCount(tupleCount).source
                        (mergedIterator).build();

                writerTask.run();

                tables.add(FileTable.open(nextTableId, paths, caches.recordBlockCache(), caches.indexBlockCache(),
                        metrics));

                removeObsoleteTables(compactionTask.tables());
                watch.stop();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void removeObsoleteTables(List<Table> toRemove) throws IOException {
            for (Table table : toRemove) {
                tables.remove(table);
                Files.deleteIfExists(paths.tablePath(table.id()));
                Files.deleteIfExists(paths.indexPath(table.id()));
                Files.deleteIfExists(paths.filterPath(table.id()));
            }
        }
    }

    private final Config config;
    private final Paths paths;
    private final Tables tables;
    private final Caches caches;
    private final ThreadPoolExecutor compactionExecutor;
    private final CompactionPlanner compactionPlanner;
    private final Metrics metrics;
    private final AtomicBoolean compactionRunning = new AtomicBoolean();


    public Compactor(Config config, Paths paths, Tables tables, Caches caches, CompactionStrategy compactionStrategy,
                     Metrics metrics) {
        this.config = config;
        this.paths = paths;
        this.tables = tables;
        this.caches = caches;
        this.metrics = metrics;
        this.compactionExecutor = new ThreadPoolExecutor(config.tableWriterThreads(),
                config.tableCompactionThreads(), Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>
                (config.tableCompactionThreads()), new ThreadPoolExecutor.CallerRunsPolicy());
        this.compactionPlanner = compactionStrategy.initialize(tables);

        tables.onChange(new Tables.ChangeHandler() {
            @Override
            public void trigger() {
                evaluateCompaction();
            }
        });
    }

    public synchronized void evaluateCompaction() {
        if (compactionPlanner.needsCompaction()) {
            scheduleCompaction();
        }
    }

    public synchronized Future<?> scheduleCompaction() {
        if (compactionRunning.get()) {
            return Futures.immediateFuture(null);
        }

        compactionRunning.set(true);

        FutureTask<?> task = new FutureTask<Object>(new Runnable() {
            @Override
            public void run() {
                CompactionPlan compactionPlan = compactionPlanner.planCompaction();
                List<Future<?>> taskFutures = new ArrayList<Future<?>>();

                for (CompactionTask task : compactionPlan) {
                    taskFutures.add(compactionExecutor.submit(new Task(task)));
                }

                metrics.histogram("compactor.concurrentTasks").update(compactionExecutor.getActiveCount());

                for (Future<?> taskFuture : taskFutures) {
                    try {
                        taskFuture.get();
                    } catch (Exception e) {
                        compactionRunning.set(false);
                        throw new RuntimeException(e);
                    }
                }

                compactionRunning.set(false);
            }
        }, null);

        compactionExecutor.execute(task);

        return task;
    }

    public void close() throws IOException {
        compactionExecutor.shutdownNow();
    }

    @Override
    public String toString() {
        return "Compactor{" +
                "compactionPlanner=" + compactionPlanner +
                '}';
    }
}
