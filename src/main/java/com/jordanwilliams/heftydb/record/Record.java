/*
 * Copyright (c) 2013. Jordan Williams
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

package com.jordanwilliams.heftydb.record;

import net.jcip.annotations.Immutable;

@Immutable
public class Record implements Comparable<Record> {

    private final Key key;
    private final Value value;
    private final long snapshotId;

    public Record(Key key, Value value, long snapshotId) {
        this.key = key;
        this.value = value;
        this.snapshotId = snapshotId;
    }

    public Key key() {
        return key;
    }

    public Value value() {
        return value;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public int size() {
        return key.size() + value().size();
    }

    @Override
    public int compareTo(Record o) {
        int compared = key.compareTo(o.key);

        if (compared != 0) {
            return compared;
        }

        if (snapshotId == o.snapshotId) {
            return 0;
        }

        return snapshotId > o.snapshotId ? 1 : -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Record record = (Record) o;

        if (snapshotId != record.snapshotId) {
            return false;
        }
        if (key != null ? !key.equals(record.key) : record.key != null) {
            return false;
        }
        if (value != null ? !value.equals(record.value) : record.value != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (int) (snapshotId ^ (snapshotId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Record{" +
                "key=" + key +
                ", value=" + value +
                ", snapshotId=" + snapshotId +
                '}';
    }
}
