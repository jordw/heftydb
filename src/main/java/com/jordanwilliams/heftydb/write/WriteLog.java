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
import com.jordanwilliams.heftydb.io.ChannelDataFile;
import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.offheap.MemoryAllocator;
import com.jordanwilliams.heftydb.offheap.MemoryPointer;
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

public class WriteLog implements Iterable<Tuple>, Closeable {

    private static final int WRITE_BUFFER_SIZE = 4096;

    private static final ThreadLocal<MemoryPointer> writeBuffer = new ThreadLocal<MemoryPointer>() {
        @Override
        protected MemoryPointer initialValue() {
            return MemoryAllocator.allocate(WRITE_BUFFER_SIZE);
        }
    };

    private class LogIterator implements Iterator<Tuple> {

        private final XORShiftRandom pseudoRandom;
        private final Queue<Tuple> nextTuple = new LinkedList<Tuple>();
        private long fileOffset = Sizes.LONG_SIZE;

        public LogIterator() {
            this.pseudoRandom = new XORShiftRandom(WriteLog.this.seed);
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
    private final DataFile logFile;
    private final long seed;
    private final XORShiftRandom pseudoRandom;

    private WriteLog(long tableId, DataFile logFile) throws IOException {
        this.tableId = tableId;
        this.logFile = logFile;
        this.seed = getSeed();
        this.pseudoRandom = new XORShiftRandom(seed);
    }

    public void append(Tuple tuple, boolean fsync) throws IOException {
        MemoryPointer recordPointer = writeBuffer(Tuple.SERIALIZER.size(tuple) + Sizes.INT_SIZE + Sizes.INT_SIZE);

        recordPointer.directBuffer().putInt(recordPointer.size() - Sizes.INT_SIZE - Sizes.INT_SIZE);
        Tuple.SERIALIZER.serialize(tuple, recordPointer.directBuffer());
        recordPointer.directBuffer().putInt(recordPointer.size() - Sizes.INT_SIZE, pseudoRandom.nextInt());
        logFile.append(recordPointer.directBuffer());

        if (fsync) {
            logFile.sync();
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

    private long getSeed() throws IOException {
        long seed;

        if (logFile.size() == 0) {
            seed = System.nanoTime();
            logFile.appendLong(seed);
        } else {
            seed = logFile.readLong(0);
        }

        return seed;
    }

    private MemoryPointer writeBuffer(int size) {
        MemoryPointer buffer = writeBuffer.get();

        if (size > buffer.size()) {
            buffer = MemoryAllocator.allocate(size, 1024);
            writeBuffer.set(buffer);
        }

        buffer.directBuffer().rewind();
        buffer.directBuffer().limit(size);

        return buffer;
    }

    public static WriteLog open(long tableId, Paths paths) throws IOException {
        DataFile logFile = ChannelDataFile.open(paths.logPath(tableId));
        return new WriteLog(tableId, logFile);
    }
}
