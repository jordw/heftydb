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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jordanwilliams.heftydb.compact.planner.CompactionPlanner;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.db.Config;
import com.jordanwilliams.heftydb.io.Throttle;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.read.CompactionTupleIterator;
import com.jordanwilliams.heftydb.read.MergingIterator;
import com.jordanwilliams.heftydb.state.Caches;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.state.Snapshots;
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
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Orchestrates the scheduling and execution of CompactionPlans provided by the contained CompactionPlanner.
 */
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
                    tableIterators.add(new CloseableIterator.Wrapper<Tuple>(table.iterator()));
                    tupleCount += table.tupleCount();
                }

                Iterator<Tuple> compactionIterator = new CompactionTupleIterator(snapshots.minimumRetainedId(),
                        new MergingIterator<Tuple>
                        (tableIterators));

                FileTableWriter.Task writerTask = new FileTableWriter.Task.Builder().tableId(nextTableId).config
                        (config).paths(paths).level(compactionTask.level()).tupleCount(tupleCount).source
                        (compactionIterator).throttle(throttle).build();

                writerTask.run();

                tables.add(FileTable.open(nextTableId, paths, caches.recordBlockCache(), caches.indexBlockCache(),
                        metrics));

                removeObsoleteTables(compactionTask.tables());

                watch.stop();
            } catch (ClosedChannelException e) {
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
    private final CompactionTables compactionTables;
    private final Caches caches;
    private final ThreadPoolExecutor compactionExecutor;
    private final ThreadPoolExecutor compactionTaskExecutor;
    private final ThreadPoolExecutor highPriorityCompactionTaskExecutor;
    private final CompactionPlanner compactionPlanner;
    private final Metrics metrics;
    private final AtomicInteger compactionId = new AtomicInteger();
    private final Snapshots snapshots;

    public Compactor(Config config, Paths paths, Tables tables, Caches caches, CompactionStrategy compactionStrategy,
                     Metrics metrics, Snapshots snapshots) {
        this.config = config;
        this.paths = paths;
        this.tables = tables;
        this.compactionTables = new CompactionTables(tables);
        this.caches = caches;
        this.metrics = metrics;
        this.snapshots = snapshots;

        this.compactionExecutor = new ThreadPoolExecutor(config.tableCompactionThreads(), config.tableCompactionThreads(),
                Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>(config.tableCompactionThreads()),
                new ThreadFactoryBuilder().setNameFormat("Compaction thread %d").build(), new ThreadPoolExecutor.CallerRunsPolicy());

        int compactionTaskThreads = Math.max(config.tableCompactionThreads() / 2, 1);

        this.compactionTaskExecutor = new ThreadPoolExecutor(compactionTaskThreads, compactionTaskThreads, Long.MAX_VALUE,
                TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>(config.tableCompactionThreads()),
                new ThreadFactoryBuilder().setNameFormat("Compaction task thread %d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy());

        this.highPriorityCompactionTaskExecutor = new ThreadPoolExecutor(compactionTaskThreads, compactionTaskThreads,
                Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>(config.tableCompactionThreads()),
                new ThreadFactoryBuilder().setNameFormat("High priority " +
                "compaction task thread %d").build(), new ThreadPoolExecutor.CallerRunsPolicy());

        this.compactionPlanner = compactionStrategy.initialize(compactionTables);

        tables.addChangeHandler(new Tables.ChangeHandler() {
            @Override
            public void changed() {
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
        final int id = compactionId.incrementAndGet();
        logger.debug("Starting compaction " + id);

        CompactionPlan compactionPlan = compactionPlanner.planCompaction();

        if (compactionPlan == null) {
            logger.debug("No compaction tasks present " + id);
            logger.debug("Finishing compaction " + id);
            return Futures.immediateFuture(null);
        }

        final List<Future<?>> taskFutures = new ArrayList<Future<?>>();
        Throttle compactionThrottle = new Throttle(config.maxCompactionRate());

        for (CompactionTask task : compactionPlan) {
            logger.debug("Compaction " + id + "  task : " + task);

            for (Table table : task.tables()){
                compactionTables.markAsCompacted(table);
            }

            ThreadPoolExecutor taskExecutor = task.priority().equals(CompactionTask.Priority.HIGH) ?
                    highPriorityCompactionTaskExecutor : compactionTaskExecutor;

            taskFutures.add(taskExecutor.submit(new Task(task, compactionThrottle)));
        }

        metrics.histogram("compactor.concurrentTasks").update(highPriorityCompactionTaskExecutor.getActiveCount() +
                compactionTaskExecutor.getActiveCount());

        return compactionExecutor.submit(new Runnable() {
            @Override
            public void run() {
                for (Future<?> taskFuture : taskFutures){
                    Futures.getUnchecked(taskFuture);
                }

                logger.debug("Finishing compaction " + id);
            }
        });
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
