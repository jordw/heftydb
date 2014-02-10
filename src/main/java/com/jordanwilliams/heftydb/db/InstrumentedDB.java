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

import com.codahale.metrics.Timer;
import com.jordanwilliams.heftydb.state.Metrics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class InstrumentedDB implements DB {

    private class InstrumentedScanIterator implements Iterator<Record> {

        private final Iterator<Record> delegate;

        private InstrumentedScanIterator(Iterator<Record> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            Timer.Context watch = scanTimer.time();
            boolean hasNext = delegate.hasNext();
            watch.stop();
            return hasNext;
        }

        @Override
        public Record next() {
            return delegate.next();
        }

        @Override
        public void remove() {
            delegate.remove();
        }
    }

    private final Timer readTimer = new Timer();
    private final Timer writeTimer = new Timer();
    private final Timer scanTimer = new Timer();

    private final Metrics metrics;
    private final DB delegate;

    public InstrumentedDB(Metrics metrics, DB delegate) {
        this.metrics = metrics;
        this.delegate = delegate;

        metrics.register("heftydb.write", writeTimer);
        metrics.register("heftydb.read", readTimer);
        metrics.register("heftydb.scan", scanTimer);
    }

    @Override
    public Snapshot put(ByteBuffer key, ByteBuffer value) throws IOException {
        Timer.Context watch = writeTimer.time();
        Snapshot snapshot = delegate.put(key, value);
        watch.stop();
        return snapshot;
    }

    @Override
    public Snapshot put(ByteBuffer key, ByteBuffer value, boolean fsync) throws IOException {
        Timer.Context watch = writeTimer.time();
        Snapshot snapshot = delegate.put(key, value, fsync);
        watch.stop();
        return snapshot;
    }

    @Override
    public Record get(ByteBuffer key) throws IOException {
        Timer.Context watch = readTimer.time();
        Record record = delegate.get(key);
        watch.stop();
        return record;
    }

    @Override
    public Record get(ByteBuffer key, Snapshot snapshot) throws IOException {
        Timer.Context watch = readTimer.time();
        Record record = delegate.get(key, snapshot);
        watch.stop();
        return record;
    }

    @Override
    public Iterator<Record> ascendingIterator(Snapshot snapshot) throws IOException {
        return new InstrumentedScanIterator(delegate.ascendingIterator(snapshot));
    }

    @Override
    public Iterator<Record> ascendingIterator(ByteBuffer key, Snapshot snapshot) throws IOException {
        return new InstrumentedScanIterator(delegate.ascendingIterator(key, snapshot));
    }

    @Override
    public Iterator<Record> descendingIterator(Snapshot snapshot) throws IOException {
        return new InstrumentedScanIterator(delegate.descendingIterator(snapshot));
    }

    @Override
    public Iterator<Record> descendingIterator(ByteBuffer key, Snapshot snapshot) throws IOException {
        return new InstrumentedScanIterator(delegate.descendingIterator(key, snapshot));
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void compact() throws IOException {
        delegate.compact();
    }
}
