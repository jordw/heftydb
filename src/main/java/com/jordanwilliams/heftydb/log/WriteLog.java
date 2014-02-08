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

package com.jordanwilliams.heftydb.log;

import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.io.ChannelDataFile;
import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.util.Sizes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

public class WriteLog implements Iterable<Tuple>, AutoCloseable {

    private class LogIterator implements Iterator<Tuple> {

        private final Queue<Tuple> nextTuple = new LinkedList<Tuple>();
        private long fileOffset = 0;

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
                recordBuffer.rewind();

                return Tuple.SERIALIZER.deserialize(recordBuffer);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final long tableId;
    private final DataFile logFile;

    private WriteLog(long tableId, DataFile logFile) {
        this.tableId = tableId;
        this.logFile = logFile;
    }

    public void append(Tuple tuple, boolean fsync) throws IOException {
        Memory recordMemory = Memory.allocate(Tuple.SERIALIZER.size(tuple) + Sizes.INT_SIZE);

        try {
            recordMemory.directBuffer().putInt(recordMemory.size() - Sizes.INT_SIZE);
            Tuple.SERIALIZER.serialize(tuple, recordMemory.directBuffer());
            logFile.append(recordMemory.directBuffer());

            if (fsync){
                logFile.sync();
            }
        } finally {
            recordMemory.release();
        }
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

    public static WriteLog open(long tableId, Paths paths) throws IOException {
        DataFile logFile = ChannelDataFile.open(paths.logPath(tableId));
        return new WriteLog(tableId, logFile);
    }
}
