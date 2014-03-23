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

package com.jordanwilliams.heftydb.db;

/**
 * Represents a database snapshot.
 */
public class Snapshot implements Comparable<Snapshot> {

    public static final Snapshot MAX = new Snapshot(Long.MAX_VALUE);
    public static final Snapshot MIN = new Snapshot(0);

    private final long id;

    public Snapshot(long id) {
        this.id = id;
    }

    public long id() {
        return id;
    }

    @Override
    public int compareTo(Snapshot o) {
        return Long.compare(id, o.id());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Snapshot snapshot = (Snapshot) o;

        if (id != snapshot.id) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    @Override
    public String toString() {
        return "Snapshot{" +
                "id=" + id +
                '}';
    }
}
