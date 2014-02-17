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

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.util.ByteBuffers;
import com.jordanwilliams.heftydb.util.CloseableIterator;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Record implements Comparable<Record> {

    public static class TupleIterator implements CloseableIterator<Record> {

        private final CloseableIterator<Tuple> tupleIterator;

        public TupleIterator(CloseableIterator<Tuple> tupleIterator) {
            this.tupleIterator = tupleIterator;
        }

        @Override
        public boolean hasNext() {
            return tupleIterator.hasNext();
        }

        @Override
        public Record next() {
            return new Record(tupleIterator.next());
        }

        @Override
        public void remove() {
            tupleIterator.remove();
        }

        @Override
        public void close() throws IOException {
            tupleIterator.close();
        }
    }

    private final ByteBuffer key;
    private final ByteBuffer value;
    private final Snapshot snapshot;

    public Record(ByteBuffer key, ByteBuffer value, Snapshot snapshot) {
        this.key = key;
        this.value = value;
        this.snapshot = snapshot;
    }

    public Record(Tuple tuple) {
        this(tuple.key().data(), tuple.value().data(), new Snapshot(tuple.key().snapshotId()));
    }

    public ByteBuffer key() {
        return key;
    }

    public ByteBuffer value() {
        return value;
    }

    public Snapshot snapshot() {
        return snapshot;
    }

    @Override
    public int compareTo(Record o) {
        int compare = key.compareTo(o.key);

        if (compare != 0) {
            return 0;
        }

        return Long.compare(snapshot.id(), o.snapshot.id());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Record record = (Record) o;

        if (key != null ? !key.equals(record.key) : record.key != null) return false;
        if (snapshot != null ? !snapshot.equals(record.snapshot) : record.snapshot != null) return false;
        if (value != null ? !value.equals(record.value) : record.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (snapshot != null ? snapshot.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Record{" +
                "key=" + ByteBuffers.toString(key) +
                ", value=" + ByteBuffers.toString(value) +
                ", snapshot=" + snapshot +
                '}';
    }
}
