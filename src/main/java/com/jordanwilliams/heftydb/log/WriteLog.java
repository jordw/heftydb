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

import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.util.Sizes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

public class WriteLog implements Iterable<Record> {

    private class LogIterator implements Iterator<Record> {

        private final Queue<Record> nextRecord = new LinkedList<Record>();
        private long fileOffset = 0;

        @Override
        public boolean hasNext() {
            if (!nextRecord.isEmpty()){
                return true;
            }

            Record next = nextRecord();

            if (next == null){
                return false;
            }

            nextRecord.add(next);

            return true;
        }

        @Override
        public Record next() {
            if (nextRecord.isEmpty()){
                if (!hasNext()){
                    throw new NoSuchElementException();
                }
            }

            return nextRecord.poll();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private Record nextRecord() {
            try {
                if (fileOffset >= logFile.size()){
                    return null;
                }

                int recordSize = logFile.readInt(fileOffset);
                fileOffset += Sizes.INT_SIZE;

                ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize);
                logFile.read(recordBuffer, fileOffset);
                fileOffset += recordSize;
                recordBuffer.rewind();

                return Record.SERIALIZER.deserialize(recordBuffer);
            } catch (IOException e){
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

    public void append(Record record) throws IOException {
        Memory recordMemory = Memory.allocate(Record.SERIALIZER.size(record));

        try {
            Record.SERIALIZER.serialize(record, recordMemory.directBuffer());
            logFile.appendInt(recordMemory.size());
            logFile.append(recordMemory.directBuffer());
        } finally {
            recordMemory.release();
        }
    }

    public long tableId(){
        return tableId;
    }

    public void close() throws IOException {
        logFile.close();
    }

    @Override
    public Iterator<Record> iterator() {
        return new LogIterator();
    }

    public static WriteLog open(long tableId, Paths paths) throws IOException {
        DataFile logFile = MutableDataFile.open(paths.logPath(tableId));
        return new WriteLog(tableId, logFile);
    }
}
