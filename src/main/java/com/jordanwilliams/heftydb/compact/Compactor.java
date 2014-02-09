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

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.read.MergingIterator;
import com.jordanwilliams.heftydb.state.State;
import com.jordanwilliams.heftydb.state.Tables;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.table.file.FileTable;
import com.jordanwilliams.heftydb.table.file.FileTableWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
                List<Iterator<Tuple>> iterators = new ArrayList<Iterator<Tuple>>();
                long tupleCount = 0;
                long nextTableId = state.tables().nextId();

                for (Table table : compactionTask.tables()){
                    iterators.add(table.ascendingIterator(Long.MAX_VALUE));
                    tupleCount += table.tupleCount();
                }

                Iterator<Tuple> mergedIterator = new MergingIterator<Tuple>(iterators);

                FileTableWriter.Task writerTask = new FileTableWriter.Task.Builder().tableId(nextTableId).config(state.config
                        ()).paths(state.paths()).level(compactionTask.level()).tupleCount(tupleCount).source(mergedIterator)
                        .build();

                writerTask.run();

                state.tables().add(FileTable.open(nextTableId, state.paths(), state.caches().recordBlockCache(),
                        state.caches().indexBlockCache()));

                removeObsoleteTables(compactionTask.tables());
            } catch (IOException e){
                throw new RuntimeException(e);
            }
        }

        private void removeObsoleteTables(List<Table> toRemove) throws IOException {
            for (Table table : toRemove) {
                state.tables().remove(table);
                Files.deleteIfExists(state.paths().tablePath(table.id()));
                Files.deleteIfExists(state.paths().indexPath(table.id()));
                Files.deleteIfExists(state.paths().filterPath(table.id()));
            }
        }
    }

    private final State state;
    private final ExecutorService compactionExecutor;
    private final CompactionPlanner compactionPlanner;
    private final AtomicBoolean compactionRunning = new AtomicBoolean();


    public Compactor(State state, CompactionPlanner compactionPlanner) {
        this.state = state;
        this.compactionExecutor = Executors.newFixedThreadPool(state.config().tableCompactionThreads());
        this.compactionPlanner = compactionPlanner;

        state.tables().onChange(new Tables.ChangeHandler() {
            @Override
            public void trigger() {
                evaluateCompaction();
            }
        });
    }

    public synchronized void evaluateCompaction(){
        if (compactionPlanner.needsCompaction()){
           scheduleCompaction();
        }
    }

    public synchronized void scheduleCompaction(){
        if (compactionRunning.get()){
            return;
        }

        compactionRunning.set(true);

        compactionExecutor.execute(new Runnable() {
            @Override
            public void run() {
                CompactionPlan compactionPlan = compactionPlanner.planCompaction();
                List<Future<?>> taskFutures = new ArrayList<Future<?>>();

                for (CompactionTask task : compactionPlan){
                    taskFutures.add(compactionExecutor.submit(new Task(task)));
                }

                for (Future<?> taskFuture : taskFutures){
                    try {
                        taskFuture.get();
                    } catch (Exception e){
                        compactionRunning.set(false);
                        throw new RuntimeException(e);
                    }
                }

                compactionRunning.set(false);
            }
        });
    }

    @Override
    public String toString() {
        return "Compactor{" +
                "compactionPlanner=" + compactionPlanner +
                '}';
    }
}
