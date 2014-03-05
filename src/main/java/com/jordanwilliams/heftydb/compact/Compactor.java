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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jordanwilliams.heftydb.compact.planner.CompactionPlanner;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.db.Config;
import com.jordanwilliams.heftydb.io.Throttle;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.read.MergingIterator;
import com.jordanwilliams.heftydb.state.Caches;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.state.Tables;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.table.file.FileTable;
import com.jordanwilliams.heftydb.table.file.FileTableWriter;
import com.jordanwilliams.heftydb.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Compactor {

    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);

    private class Task implements Runnable {

        private final CompactionTask compactionTask;
        private final Throttle throttle;

        private Task(CompactionTask compactionTask, Throttle throttle) {
            this.compactionTask = compactionTask;
            this.throttle = throttle;
        }

        @Override
        public void run() {
            try {
                Timer.Context watch = metrics.timer("compactor.taskExecution").time();
                List<CloseableIterator<Tuple>> tableIterators = new ArrayList<CloseableIterator<Tuple>>();
                long tupleCount = 0;
                long nextTableId = tables.nextId();

                for (Table table : compactionTask.tables()) {
                    tableIterators.add(table.ascendingIterator(Long.MAX_VALUE));
                    tupleCount += table.tupleCount();
                }

                Iterator<Tuple> mergedIterator = new MergingIterator<Tuple>(tableIterators);

                FileTableWriter.Task writerTask = new FileTableWriter.Task.Builder().tableId(nextTableId).config
                        (config).paths(paths).level(compactionTask.level()).tupleCount(tupleCount).source
                        (mergedIterator).throttle(throttle).build();

                writerTask.run();

                tables.add(FileTable.open(nextTableId, paths, caches.recordBlockCache(), caches.indexBlockCache(),
                        metrics));

                removeObsoleteTables(compactionTask.tables());

                watch.stop();
            } catch (ClosedChannelException e){
                logger.debug("Compaction terminated without finishing " + compactionId);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void removeObsoleteTables(List<Table> toRemove) throws IOException {
            tables.removeAll(toRemove);

            for (Table table : toRemove) {
                table.close();
                caches.indexBlockCache().invalidate(table.id());
                caches.recordBlockCache().invalidate(table.id());
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
    private final ThreadPoolExecutor compactionTaskExecutor;
    private final ExecutorService compactionExecutor;
    private final CompactionPlanner compactionPlanner;
    private final Metrics metrics;
    private final AtomicInteger compactionId = new AtomicInteger();
    private final LinkedList<Future<?>> pendingCompactions = new LinkedList<Future<?>>();

    public Compactor(Config config, Paths paths, Tables tables, Caches caches, CompactionStrategy compactionStrategy,
                     Metrics metrics) {
        this.config = config;
        this.paths = paths;
        this.tables = tables;
        this.caches = caches;
        this.metrics = metrics;
        this.compactionTaskExecutor = new ThreadPoolExecutor(config.tableWriterThreads(),
                config.tableCompactionThreads(), Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>
                (config.tableCompactionThreads()), new ThreadFactoryBuilder().setNameFormat("Compaction task thread " +
                "%d")
                .build(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        this.compactionExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat
                ("Compaction plan thread").build());
        this.compactionPlanner = compactionStrategy.initialize(tables);


        tables.addChangeHandler(new Tables.ChangeHandler() {
            @Override
            public void changed() {
                evaluateCompaction();
            }
        });
    }

    public synchronized void evaluateCompaction() {
        if (compactionPlanner.needsCompaction()) {
            scheduleCompaction(false);
        }
    }

    public synchronized Future<?> scheduleCompaction(final boolean force) {

        FutureTask<?> task = new FutureTask<Object>(new Runnable() {
            @Override
            public void run() {
                //Previous compaction may have obviated the need for another one
                if (!compactionPlanner.needsCompaction() && !force){
                    return;
                }

                int id = compactionId.incrementAndGet();
                logger.debug("Starting compaction " + id);

                CompactionPlan compactionPlan = compactionPlanner.planCompaction();

                if (compactionPlan == null){
                    logger.debug("No compaction tasks present " + id);
                    logger.debug("Finishing compaction " + id);
                    return;
                }

                List<Future<?>> taskFutures = new ArrayList<Future<?>>();
                Throttle compactionThrottle = force ? Throttle.MAX : new Throttle(config.maxCompactionRate());

                for (CompactionTask task : compactionPlan) {
                    logger.debug("Compaction " + id + "  task : " + task);
                    taskFutures.add(compactionTaskExecutor.submit(new Task(task, compactionThrottle)));
                }

                metrics.histogram("compactor.concurrentTasks").update(compactionTaskExecutor.getActiveCount());

                for (Future<?> taskFuture : taskFutures) {
                    try {
                        taskFuture.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                logger.debug("Finishing compaction " + id);
            }
        }, null);

        pendingCompactions.add(task);
        compactionExecutor.execute(task);

        return task;
    }

    public void close() throws IOException {
        compactionExecutor.shutdownNow();
        compactionTaskExecutor.shutdownNow();
    }

    @Override
    public String toString() {
        return "Compactor{" +
                "compactionPlanner=" + compactionPlanner +
                '}';
    }
}
