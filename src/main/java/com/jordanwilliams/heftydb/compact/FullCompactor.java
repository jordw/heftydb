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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class FullCompactor implements Compactor {

    private final State state;

    public FullCompactor(State state) {
        this.state = state;
    }

    @Override
    public void compact() throws IOException {
        Set<Table> toRemove = new HashSet<Table>();
        List<Iterator<Tuple>> iterators = new ArrayList<Iterator<Tuple>>();
        long recordCount = 0;

        for (Table table : state.tables().persistent()){
            toRemove.add(table);
            iterators.add(table.ascendingIterator(Long.MAX_VALUE));
            recordCount += table.recordCount();
        }

        Iterator<Tuple> mergedIterator = new MergingIterator<Tuple>(iterators);
        long nextTableId = state.tables().nextId();

        FileTableWriter.Task writerTask = new FileTableWriter.Task(nextTableId, 2, state.paths(),
                state.config(), mergedIterator, recordCount);

        writerTask.run();

        state.tables().add(FileTable.open(nextTableId, state.paths(), state.caches().recordBlockCache(),
                state.caches().indexBlockCache()));

        for (Table table : toRemove){
            state.tables().remove(table);
            Files.deleteIfExists(state.paths().tablePath(table.id()));
            Files.deleteIfExists(state.paths().indexPath(table.id()));
            Files.deleteIfExists(state.paths().filterPath(table.id()));
        }
    }
}
