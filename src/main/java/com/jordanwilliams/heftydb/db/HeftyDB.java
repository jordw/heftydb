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
import com.jordanwilliams.heftydb.state.Config;
import com.jordanwilliams.heftydb.state.State;
import com.jordanwilliams.heftydb.state.StateInitializer;
import com.jordanwilliams.heftydb.write.RecordWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class HeftyDB {

    private final RecordWriter recordWriter;
    private final RecordReader recordReader;

    public HeftyDB(State state) {
        this.recordWriter = new RecordWriter(state);
        this.recordReader = new RecordReader(state);
    }

    public Snapshot put(ByteBuffer key, ByteBuffer value) throws IOException {
        return recordWriter.write(key, value);
    }

    public Record get(ByteBuffer key) throws IOException {
        return recordReader.get(new Key(key, Long.MAX_VALUE));
    }

    public Record get(ByteBuffer key, Snapshot snapshot) throws IOException {
        return recordReader.get(new Key(key, snapshot.id()));
    }

    public Iterator<Record> ascendingIterator(Snapshot snapshot) throws IOException {
        return recordReader.ascendingIterator(snapshot.id());
    }

    public Iterator<Record> ascendingIterator(ByteBuffer key, Snapshot snapshot) throws IOException {
        return recordReader.ascendingIterator(new Key(key, snapshot.id()), snapshot.id());
    }

    public Iterator<Record> descendingIterator(Snapshot snapshot) throws IOException {
        return recordReader.descendingIterator(snapshot.id());
    }

    public Iterator<Record> descendingIterator(ByteBuffer key, Snapshot snapshot) throws IOException {
        return recordReader.descendingIterator(new Key(key, snapshot.id()), snapshot.id());
    }

    public void close() throws IOException {
        recordWriter.close();
        recordReader.close();
    }

    public static HeftyDB open(Config config) throws IOException {
        State state = new StateInitializer(config).initialize();
        return new HeftyDB(state);
    }
}
