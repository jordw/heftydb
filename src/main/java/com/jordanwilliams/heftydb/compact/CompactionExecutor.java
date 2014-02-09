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
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.table.file.FileTable;
import com.jordanwilliams.heftydb.table.file.FileTableWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CompactionExecutor {

    private class Task implements Callable<Void> {

        private final CompactionTask compactionTask;

        private Task(CompactionTask compactionTask) {
            this.compactionTask = compactionTask;
        }

        @Override
        public Void call() {
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

                writerTask.call();

                state.tables().add(FileTable.open(nextTableId, state.paths(), state.caches().recordBlockCache(),
                        state.caches().indexBlockCache()));

                removeObsoleteTables(compactionTask.tables());
            } catch (IOException e){
                throw new RuntimeException(e);
            }

            return null;
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

    public CompactionExecutor(State state) {
        this.state = state;
        this.compactionExecutor = Executors.newFixedThreadPool(state.config().tableCompactionThreads());
    }

    public Future<?> schedule(final CompactionPlan compactionPlan){
        final CompletionService<Void> completionService = new ExecutorCompletionService<Void>(compactionExecutor);

        for (CompactionTask compactionTask : compactionPlan){
            completionService.submit(new Task(compactionTask));
        }

        return compactionExecutor.submit(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < compactionPlan.tasks().size(); i++){
                    try {
                        completionService.take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }
}
