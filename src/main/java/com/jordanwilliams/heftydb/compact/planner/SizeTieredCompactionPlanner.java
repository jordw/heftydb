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

package com.jordanwilliams.heftydb.compact.planner;

import com.jordanwilliams.heftydb.compact.CompactionPlan;
import com.jordanwilliams.heftydb.compact.CompactionTask;
import com.jordanwilliams.heftydb.state.Tables;
import com.jordanwilliams.heftydb.table.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class SizeTieredCompactionPlanner implements CompactionPlanner {

    private static final int MAX_LEVEL_TABLES = 4;

    private final Tables tables;

    public SizeTieredCompactionPlanner(Tables tables) {
        this.tables = tables;
    }

    @Override
    public CompactionPlan planCompaction() {
        SortedMap<Integer, List<Table>> leveledTables = leveledTables();
        List<CompactionTask> compactionTasks = new ArrayList<CompactionTask>();

        for (Map.Entry<Integer, List<Table>> entry : leveledTables.entrySet()){
            if (entry.getValue().size() >= MAX_LEVEL_TABLES){
                compactionTasks.add(new CompactionTask(entry.getValue(), entry.getKey() + 1));
            }
        }

        return new CompactionPlan(compactionTasks);
    }

    @Override
    public boolean needsCompaction() {
        SortedMap<Integer, List<Table>> leveledTables = leveledTables();

        for (Map.Entry<Integer, List<Table>> entry : leveledTables.entrySet()){
            if (entry.getValue().size() >= MAX_LEVEL_TABLES){
                return true;
            }
        }

        return false;
    }

    private SortedMap<Integer, List<Table>> leveledTables(){
        SortedMap<Integer, List<Table>> tableMap = new TreeMap<Integer, List<Table>>();

        tables.readLock();

        try {
            for (Table table : tables) {
                if (table.isPersistent()) {
                    List<Table> levelTables = tableMap.get(table.level());

                    if (levelTables == null){
                        levelTables = new ArrayList<Table>();
                        tableMap.put(table.level(), levelTables);
                    }

                    levelTables.add(table);
                }
            }
        } finally {
            tables.readUnlock();
        }

        return tableMap;
    }
}
