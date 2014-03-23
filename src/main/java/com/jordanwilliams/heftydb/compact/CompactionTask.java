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

import com.jordanwilliams.heftydb.table.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a single CompactionTask.
 */
public class CompactionTask {

    public enum Priority {
        HIGH, NORMAL
    }

    public static class Builder {

        private final List<Table> tables = new ArrayList<Table>();
        private final int level;
        private final Priority priority;

        public Builder(int level, Priority priority) {
            this.level = level;
            this.priority = priority;
        }

        public void add(Table table) {
            tables.add(table);
        }

        public CompactionTask build() {
            return new CompactionTask(tables, level, priority);
        }
    }

    private final List<Table> tables;
    private final int level;
    private final Priority priority;

    public CompactionTask(List<Table> tables, int level, Priority priority) {
        this.tables = tables;
        this.level = level;
        this.priority = priority;
    }

    public List<Table> tables() {
        return tables;
    }

    public int level() {
        return level;
    }

    public Priority priority() {
        return priority;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompactionTask that = (CompactionTask) o;

        if (level != that.level) return false;
        if (priority != that.priority) return false;
        if (tables != null ? !tables.equals(that.tables) : that.tables != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tables != null ? tables.hashCode() : 0;
        result = 31 * result + level;
        result = 31 * result + (priority != null ? priority.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CompactionTask{" +
                "tables=" + tables +
                ", level=" + level +
                ", priority=" + priority +
                '}';
    }
}
