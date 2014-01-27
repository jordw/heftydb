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

import com.jordanwilliams.heftydb.read.RecordReader;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.record.Snapshot;
import com.jordanwilliams.heftydb.record.Value;
import com.jordanwilliams.heftydb.state.State;
import com.jordanwilliams.heftydb.write.RecordWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class HeftyDB {

    private final State state;
    private final RecordWriter recordWriter;
    private final RecordReader recordReader;

    public HeftyDB(State state) {
        this.state = state;
        this.recordWriter = new RecordWriter(state);
        this.recordReader = new RecordReader(state);
    }

    public Snapshot put(ByteBuffer key, ByteBuffer value) throws IOException  {
        long nextSnapshotId = state.snapshots().nextId();

        Key recordKey = new Key(key, nextSnapshotId);
        Value recordValue = new Value(value);
        Record record = new Record(recordKey, recordValue);

        recordWriter.write(record);

        return new Snapshot(nextSnapshotId);
    }

    public Record get(ByteBuffer key) throws IOException  {
        return null;
    }

    public Record get(ByteBuffer key, Snapshot snapshot) throws IOException  {
        return null;
    }

    public Iterator<Record> ascendingIterator(Snapshot snapshot) throws IOException  {
        return null;
    }

    public Iterator<Record> ascendingIterator(ByteBuffer key, Snapshot snapshot) throws IOException  {
        return null;
    }

    public Iterator<Record> descendingIterator(Snapshot snapshot) throws IOException  {
        return null;
    }

    public Iterator<Record> descendingIterator(ByteBuffer key, Snapshot snapshot) throws IOException {
        return null;
    }

    public void close() throws IOException {

    }
}
