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

import com.jordanwilliams.heftydb.state.Metrics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class InstrumentedDB implements DB {

    private final DB delegate;

    InstrumentedDB(DB delegate, Metrics metrics) {
        this.delegate = delegate;
    }

    @Override
    public Snapshot put(ByteBuffer key, ByteBuffer value) throws IOException {
        return null;
    }

    @Override
    public Snapshot put(ByteBuffer key, ByteBuffer value, boolean fsync) throws IOException {
        return null;
    }

    @Override
    public Record get(ByteBuffer key) throws IOException {
        return null;
    }

    @Override
    public Record get(ByteBuffer key, Snapshot snapshot) throws IOException {
        return null;
    }

    @Override
    public Iterator<Record> ascendingIterator(Snapshot snapshot) throws IOException {
        return null;
    }

    @Override
    public Iterator<Record> ascendingIterator(ByteBuffer key, Snapshot snapshot) throws IOException {
        return null;
    }

    @Override
    public Iterator<Record> descendingIterator(Snapshot snapshot) throws IOException {
        return null;
    }

    @Override
    public Iterator<Record> descendingIterator(ByteBuffer key, Snapshot snapshot) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void compact() throws IOException {

    }
}
