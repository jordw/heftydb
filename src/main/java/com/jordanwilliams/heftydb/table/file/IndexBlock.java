/*
 * Copyright (c) 2013. Jordan Williams
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

package com.jordanwilliams.heftydb.table.file;

import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.offheap.Offheap;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.record.Value;
import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class IndexBlock implements Iterable<IndexRecord>, Offheap {

    public static class Builder {

        private final RecordBlock.Builder recordBlockBuilder = new RecordBlock.Builder();

        private int sizeBytes;

        public void addRecord(IndexRecord indexRecord) {
            recordBlockBuilder.addRecord(toRecord(indexRecord));
            sizeBytes += indexRecord.sizeBytes();
        }

        public int sizeBytes() {
            return sizeBytes;
        }

        public IndexBlock build() {
            return new IndexBlock(recordBlockBuilder.build());
        }

        private Record toRecord(IndexRecord indexRecord){
            return new Record(indexRecord.startKey(), new Value(indexRecordContents(indexRecord)),
                    indexRecord.snapshotId());
        }

        private ByteBuffer indexRecordContents(IndexRecord indexRecord){
            ByteBuffer contentsBuffer = ByteBuffer.allocate(indexRecord.contentsSizeBytes());
            contentsBuffer.putLong(indexRecord.offset());
            contentsBuffer.put(indexRecord.isLeaf() ? (byte) 1 : (byte) 0);
            contentsBuffer.rewind();
            return contentsBuffer;
        }
    }

    private final RecordBlock recordBlock;

    public IndexBlock(RecordBlock recordBlock) {
        this.recordBlock = recordBlock;
    }

    public IndexRecord startRecord() {
        return deserializeRecord(0);
    }

    public IndexRecord get(Key key, long maxSnapshotId){
        return null;
    }

    @Override
    public Memory memory() {
        return recordBlock.memory();
    }

    @Override
    public long sizeBytes() {
        return recordBlock.sizeBytes();
    }

    @Override
    public void releaseMemory() {
        recordBlock.memory().release();
    }

    @Override
    public Iterator<IndexRecord> iterator() {
        return new Iterator<IndexRecord>() {

            int currentRecordIndex = 0;

            @Override
            public boolean hasNext() {
                return currentRecordIndex < recordBlock.recordCount();
            }

            @Override
            public IndexRecord next() {
                if (currentRecordIndex >= recordBlock.recordCount()) {
                    throw new NoSuchElementException();
                }

                IndexRecord next = deserializeRecord(currentRecordIndex);
                currentRecordIndex++;
                return next;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private IndexRecord deserializeRecord(int recordIndex){
        long memoryOffset = recordBlock.recordMemoryOffset(recordIndex);
        Memory memory = recordBlock.memory();

        //Key
        int keySize = memory.getInt(memoryOffset);
        ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
        memoryOffset += Sizes.INT_SIZE;
        memory.getBytes(memoryOffset, keyBuffer);
        memoryOffset += keySize;

        //Snapshot Id
        long snapshotId = memory.getLong(memoryOffset);
        memoryOffset += Sizes.LONG_SIZE;

        //Value Size
        memoryOffset += Sizes.INT_SIZE;

        //Offset
        long offset = memory.getLong(memoryOffset);
        memoryOffset += Sizes.LONG_SIZE;

        //Leaf flag
        boolean isLeaf = memory.getByte(memoryOffset) == (byte) 1;

        return new IndexRecord(new Key(keyBuffer), snapshotId, offset, isLeaf);
    }

    @Override
    public String toString() {
        List<IndexRecord> records = new ArrayList<IndexRecord>();
        for (IndexRecord record : this) {
            records.add(record);
        }

        return "IndexBlock{records=" + records + "}";
    }
}
