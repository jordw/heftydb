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

import com.jordanwilliams.heftydb.util.CloseableIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

/**
 * The public API for a database implementation.
 */
public interface DB {

    public Snapshot put(ByteBuffer key, ByteBuffer value) throws IOException;

    public Snapshot put(ByteBuffer key, ByteBuffer value, boolean fsync) throws IOException;

    public Record get(ByteBuffer key) throws IOException;

    public Record get(ByteBuffer key, Snapshot snapshot) throws IOException;

    public Snapshot delete(ByteBuffer key) throws IOException;

    public CloseableIterator<Record> ascendingIterator(Snapshot snapshot) throws IOException;

    public CloseableIterator<Record> ascendingIterator(ByteBuffer key, Snapshot snapshot) throws IOException;

    public CloseableIterator<Record> descendingIterator(Snapshot snapshot) throws IOException;

    public CloseableIterator<Record> descendingIterator(ByteBuffer key, Snapshot snapshot) throws IOException;

    public void close() throws IOException;

    public void logMetrics();

    public Future<?> compact() throws IOException;
}
