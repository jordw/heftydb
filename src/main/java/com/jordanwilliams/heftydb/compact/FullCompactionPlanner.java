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

import com.jordanwilliams.heftydb.state.Tables;
import com.jordanwilliams.heftydb.table.Table;

public class FullCompactionPlanner implements CompactionPlanner {

    private final Tables tables;
    private int level = 1;

    public FullCompactionPlanner(Tables tables) {
        this.tables = tables;
    }

    @Override
    public CompactionPlan planCompaction() {
        CompactionTask.Builder taskBuilder = new CompactionTask.Builder(level++);

        tables.readLock();

        try {
            for (Table table : tables) {
                if (table.isPersistent()) {
                    taskBuilder.add(table);
                }
            }
        } finally {
            tables.readUnlock();
        }

        return new CompactionPlan(taskBuilder.build());
    }

    @Override
    public boolean needsCompaction() {
        int count = 0;

        for (Table table : tables) {
            if (table.isPersistent()) {
                count++;
            }
        }

        return count > 10;
    }
}
