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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A collection of CompactionTasks that can execute in parallel.
 */
public class CompactionPlan implements Iterable<CompactionTask> {

    private final List<CompactionTask> tasks;

    public CompactionPlan(List<CompactionTask> tasks) {
        this.tasks = tasks;
    }

    public CompactionPlan(CompactionTask... tasks) {
        this(Arrays.asList(tasks));
    }

    public List<CompactionTask> tasks() {
        return tasks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompactionPlan that = (CompactionPlan) o;

        if (tasks != null ? !tasks.equals(that.tasks) : that.tasks != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return tasks != null ? tasks.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "CompactionPlan{" +
                "tasks=" + tasks +
                '}';
    }

    @Override
    public Iterator<CompactionTask> iterator() {
        return tasks.iterator();
    }
}
