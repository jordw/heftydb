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

public class CompactionTask {

    public static class Builder {

        private final List<Table> tables = new ArrayList<Table>();
        private final int level;

        public Builder(int level) {
            this.level = level;
        }

        public void add(Table table) {
            tables.add(table);
        }

        public CompactionTask build() {
            return new CompactionTask(tables, level);
        }
    }

    private final List<Table> tables;
    private final int level;

    public CompactionTask(List<Table> tables, int level) {
        this.tables = tables;
        this.level = level;
    }

    public List<Table> tables() {
        return tables;
    }

    public int level() {
        return level;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompactionTask that = (CompactionTask) o;

        if (tables != null ? !tables.equals(that.tables) : that.tables != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return tables != null ? tables.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "CompactionTask{" +
                "tables=" + tables +
                '}';
    }
}
