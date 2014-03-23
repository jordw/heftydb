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

package com.jordanwilliams.heftydb.write;

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.io.ImmutableChannelFile;
import com.jordanwilliams.heftydb.io.ImmutableFile;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.util.Sizes;
import com.jordanwilliams.heftydb.util.XORShiftRandom;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * Provides a read-only view on a CommitLog file. Commit logs are written with a predictable stream of pseudo random
 * numbers with each record to ensure consistency. A commit log file that contains corrupted records is truncated at
 * the first record that fails this consistency check.
 */
public class CommitLog implements Iterable<Tuple>, Closeable {

    private class LogIterator implements Iterator<Tuple> {

        private final XORShiftRandom pseudoRandom;
        private final Queue<Tuple> nextTuple = new LinkedList<Tuple>();
        private long fileOffset = Sizes.LONG_SIZE;

        public LogIterator() {
            this.pseudoRandom = new XORShiftRandom(CommitLog.this.seed);
        }

        @Override
        public boolean hasNext() {
            if (!nextTuple.isEmpty()) {
                return true;
            }

            Tuple next = nextRecord();

            if (next == null) {
                return false;
            }

            nextTuple.add(next);

            return true;
        }

        @Override
        public Tuple next() {
            if (nextTuple.isEmpty()) {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
            }

            return nextTuple.poll();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private Tuple nextRecord() {
            try {
                if (fileOffset >= logFile.size()) {
                    return null;
                }

                int recordSize = logFile.readInt(fileOffset);
                fileOffset += Sizes.INT_SIZE;

                ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize);
                logFile.read(recordBuffer, fileOffset);
                fileOffset += recordSize;
                int nextInt = logFile.readInt(fileOffset);
                fileOffset += Sizes.INT_SIZE;

                if (nextInt != pseudoRandom.nextInt()) {
                    return null;
                }

                recordBuffer.rewind();

                return Tuple.SERIALIZER.deserialize(recordBuffer);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final long tableId;
    private final ImmutableFile logFile;
    private final long seed;

    private CommitLog(long tableId, ImmutableFile logFile) throws IOException {
        this.tableId = tableId;
        this.logFile = logFile;
        this.seed = getSeed();
    }

    public long tableId() {
        return tableId;
    }

    @Override
    public void close() throws IOException {
        logFile.close();
    }

    @Override
    public Iterator<Tuple> iterator() {
        return new LogIterator();
    }

    private long getSeed() throws IOException {
        return logFile.readLong(0);
    }

    public static CommitLog open(long tableId, Paths paths) throws IOException {
        ImmutableFile logFile = ImmutableChannelFile.open(paths.logPath(tableId));
        return new CommitLog(tableId, logFile);
    }
}
