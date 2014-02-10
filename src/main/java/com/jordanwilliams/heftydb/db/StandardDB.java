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

import com.jordanwilliams.heftydb.aggregate.TableReader;
import com.jordanwilliams.heftydb.aggregate.TableWriter;
import com.jordanwilliams.heftydb.compact.Compactor;
import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.state.Caches;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.state.Snapshots;
import com.jordanwilliams.heftydb.state.Tables;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

class StandardDB implements DB {

    private final TableWriter tableWriter;
    private final TableReader tableReader;
    private final Compactor compactor;
    private final Snapshots snapshots;

    StandardDB(Config config, Paths paths, Tables tables, Snapshots snapshots, Caches caches) {
        this.snapshots = snapshots;
        this.tableWriter = new TableWriter(config, paths, tables, snapshots, caches);
        this.tableReader = new TableReader(tables);
        this.compactor = new Compactor(config, paths, tables, caches, config.compactionStrategy());
    }

    @Override
    public Snapshot put(ByteBuffer key, ByteBuffer value) throws IOException {
        return tableWriter.write(key, value, false);
    }

    @Override
    public Snapshot put(ByteBuffer key, ByteBuffer value, boolean fsync) throws IOException {
        return tableWriter.write(key, value, fsync);
    }

    @Override
    public Record get(ByteBuffer key) throws IOException {
        Tuple tuple = tableReader.get(new Key(key, snapshots.currentId()));
        return tuple == null ? null : new Record(tuple);
    }

    @Override
    public Record get(ByteBuffer key, Snapshot snapshot) throws IOException {
        Tuple tuple = tableReader.get(new Key(key, snapshot.id()));
        return tuple == null ? null : new Record(tuple);
    }

    @Override
    public Iterator<Record> ascendingIterator(Snapshot snapshot) throws IOException {
        return new Record.TupleIterator(tableReader.ascendingIterator(snapshot.id()));
    }

    @Override
    public Iterator<Record> ascendingIterator(ByteBuffer key, Snapshot snapshot) throws IOException {
        return new Record.TupleIterator(tableReader.ascendingIterator(new Key(key, snapshot.id()), snapshot.id()));
    }

    @Override
    public Iterator<Record> descendingIterator(Snapshot snapshot) throws IOException {
        return new Record.TupleIterator(tableReader.descendingIterator(snapshot.id()));
    }

    @Override
    public Iterator<Record> descendingIterator(ByteBuffer key, Snapshot snapshot) throws IOException {
        return new Record.TupleIterator(tableReader.descendingIterator(new Key(key, snapshot.id()), snapshot.id()));
    }

    @Override
    public synchronized void close() throws IOException {
        compactor.close();
        tableWriter.close();
        tableReader.close();
    }

    @Override
    public synchronized void compact() throws IOException {
        compactor.scheduleCompaction();
    }
}
