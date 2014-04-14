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
import com.jordanwilliams.heftydb.compact.CompactionTables;
import com.jordanwilliams.heftydb.compact.CompactionTask;
import com.jordanwilliams.heftydb.table.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Compacts tables into groups with overlapping key ranges once there are 10 tables at a particular size.
 */
public class SizeTieredCompactionPlanner implements CompactionPlanner {

    private static final int MAX_LEVEL_TABLES = 5;

    private final CompactionTables tables;

    public SizeTieredCompactionPlanner(CompactionTables tables) {
        this.tables = tables;
    }

    @Override
    public CompactionPlan planCompaction() {
        SortedMap<Integer, List<Table>> leveledTables = leveledTables();
        List<CompactionTask> compactionTasks = new ArrayList<CompactionTask>();

        for (Map.Entry<Integer, List<Table>> entry : leveledTables.entrySet()) {
            if (entry.getValue().size() >= MAX_LEVEL_TABLES) {
                int level = entry.getKey();
                compactionTasks.add(new CompactionTask(entry.getValue(), level + 1, level < 3 ? CompactionTask
                        .Priority.HIGH : CompactionTask.Priority.NORMAL));
            }
        }

        return new CompactionPlan(compactionTasks);
    }

    @Override
    public boolean needsCompaction() {
        SortedMap<Integer, List<Table>> leveledTables = leveledTables();

        for (Map.Entry<Integer, List<Table>> entry : leveledTables.entrySet()) {
            if (entry.getValue().size() >= MAX_LEVEL_TABLES) {
                return true;
            }
        }

        return false;
    }

    private SortedMap<Integer, List<Table>> leveledTables() {
        SortedMap<Integer, List<Table>> tableMap = new TreeMap<Integer, List<Table>>();

        List<Table> eligibleTables = tables.eligibleTables();

        for (Table table : eligibleTables) {
            List<Table> levelTables = tableMap.get(table.level());

            if (levelTables == null) {
                levelTables = new ArrayList<Table>();
                tableMap.put(table.level(), levelTables);
            }

            levelTables.add(table);
        }

        return tableMap;
    }
}
