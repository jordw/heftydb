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

import com.jordanwilliams.heftydb.state.State;
import com.jordanwilliams.heftydb.state.Tables;

import java.util.concurrent.Future;

public class Compactor {

    private final CompactionExecutor compactionExecutor;
    private final CompactionPlanner compactionPlanner;
    private Future<?> compactionFuture;

    public Compactor(State state, CompactionPlanner compactionPlanner) {
        this.compactionExecutor = new CompactionExecutor(state);
        this.compactionPlanner = compactionPlanner;

        state.tables().onChange(new Tables.ChangeHandler() {
            @Override
            public void trigger() {
                evaluateCompaction();
            }
        });
    }

    public synchronized void evaluateCompaction(){
        if (compactionPlanner.shouldCompact()){
           scheduleCompaction();
        }
    }

    public synchronized void scheduleCompaction(){
       if (compactionFuture != null && !compactionFuture.isDone()){
           return;
       }

       //compactionFuture = compactionExecutor.schedule(compactionPlanner.planCompaction());
    }

    @Override
    public String toString() {
        return "Compactor{" +
                "compactionPlanner=" + compactionPlanner +
                '}';
    }
}
