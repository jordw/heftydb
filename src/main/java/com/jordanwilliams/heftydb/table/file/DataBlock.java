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

public class DataBlock implements Iterable<Record>, Offheap {

    public static class Builder {

        private final List<Record> records = new ArrayList<Record>();
        private int sizeBytes;

        public void addRecord(Record record) {
            records.add(record);
            sizeBytes += record.size();
        }

        public int sizeBytes() {
            return sizeBytes;
        }

        public DataBlock build() {
            return new DataBlock(serializeRecords(records));
        }

        private static Memory serializeRecords(List<Record> records) {
            //Allocate memory
            int memorySize = 0;
            int[] recordOffsets = new int[records.size()];

            memorySize += Sizes.INT_SIZE; //Pointer count
            memorySize += Sizes.INT_SIZE * records.size(); //Pointers

            //Compute memory size
            for (int i = 0; i < records.size(); i++) {
                Record record = records.get(i);
                recordOffsets[i] = memorySize;
                memorySize += Sizes.INT_SIZE; //Key size
                memorySize += record.key().size(); //Key
                memorySize += Sizes.LONG_SIZE; //Snapshot Id
                memorySize += Sizes.INT_SIZE; //Value size
                memorySize += record.value().size(); //Value
            }

            Memory memory = Memory.allocate(memorySize);

            //Serialize the index records
            int memoryOffset = 0;

            //Pack pointers
            memory.putInt(memoryOffset, records.size());
            memoryOffset += Sizes.INT_SIZE;

            for (int i = 0; i < recordOffsets.length; i++) {
                memory.putInt(memoryOffset, recordOffsets[i]);
                memoryOffset += Sizes.INT_SIZE;
            }

            //Pack records
            for (Record record : records) {
                //Key size
                memory.putInt(memoryOffset, record.key().size());
                memoryOffset += Sizes.INT_SIZE;

                //Key
                memory.putBytes(memoryOffset, record.key().data());
                memoryOffset += record.key().size();

                //Snapshot Id
                memory.putLong(memoryOffset, record.snapshotId());
                memoryOffset += Sizes.LONG_SIZE;

                //Value size
                memory.putInt(memoryOffset, record.value().size());
                memoryOffset += Sizes.INT_SIZE;

                //Value
                if (record.value().size() != 0) {
                    memory.putBytes(memoryOffset, record.value().data());
                    memoryOffset += record.value().size();
                }
            }

            return memory;
        }
    }

    private final Memory memory;
    private final int recordCount;

    public DataBlock(Memory memory) {
        this.memory = memory;
        this.recordCount = memory.getInt(0);
    }

    public Record get(Key key, long maxSnapshotId) {
        int recordIndex = recordIndex(versionedKeyBuffer(key, maxSnapshotId));
        Record closest = deserializeRecord(recordIndex);

        key.data().rewind();
        int compare = key.compareTo(closest.key());
        closest.key().data().rewind();

        return compare == 0 ? closest : null;
    }

    public Key startKey(){
        Record deserializedRecord = deserializeRecord(0);
        return deserializedRecord.key();
    }

    @Override
    public Iterator<Record> iterator() {

        return new Iterator<Record>() {

            int currentRecordIndex = 0;

            @Override
            public boolean hasNext() {
                return currentRecordIndex < recordCount;
            }

            @Override
            public Record next() {
                if (currentRecordIndex >= recordCount){
                    throw new NoSuchElementException();
                }

                Record next = deserializeRecord(currentRecordIndex);
                currentRecordIndex++;
                return next;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private int recordIndex(ByteBuffer key) {
        int low = 0;
        int high = recordCount - 1;

        //Binary search
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int compare = compareKeys(key, mid);

            if (compare < 0) {
                low = mid + 1;
            } else if (compare > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }

        return low - 1;
    }

    private int compareKeys(ByteBuffer key, int compareKeyIndex) {
        int recordOffset = recordOffset(compareKeyIndex);
        int keySize = memory.getInt(recordOffset);
        int keyOffset = recordOffset + Sizes.INT_SIZE;
        key.rewind();
        return memory.compareAsBytes(key, keyOffset, keySize + Sizes.LONG_SIZE);
    }

    private int recordOffset(int pointerIndex) {
        return memory.getInt(pointerOffset(pointerIndex));
    }

    private Record deserializeRecord(int recordIndex) {
        int recordOffset = recordOffset(recordIndex);
        int keySize = memory.getInt(recordOffset);
        int keyOffset = recordOffset + Sizes.INT_SIZE;
        long memoryOffset = keyOffset;

        //Key
        ByteBuffer key = ByteBuffer.allocate(keySize);
        memory.getBytes(memoryOffset, key, 0, keySize);
        key.rewind();
        memoryOffset += keySize;

        //Snapshot Id
        long snapshotId = memory.getLong(memoryOffset);
        memoryOffset += Sizes.LONG_SIZE;

        //Value
        int valueSize = memory.getInt(memoryOffset);
        memoryOffset += Sizes.INT_SIZE;
        ByteBuffer value = null;

        if (valueSize != 0) {
            value = ByteBuffer.allocate(valueSize);
            memory.getBytes(memoryOffset, value, 0, valueSize);
            value.rewind();
        }

        return new Record(new Key(key), valueSize == 0 ? Value.TOMBSTONE_VALUE : new Value(value), snapshotId);
    }

    private static int pointerOffset(int pointerIndex) {
        int pointerOffset = Sizes.INT_SIZE;
        pointerOffset += pointerIndex * Sizes.INT_SIZE;
        return pointerOffset;
    }

    private static ByteBuffer versionedKeyBuffer(Key key, long snapshotId) {
        ByteBuffer versionedKey = ByteBuffer.allocate(key.size() + Sizes.LONG_SIZE);
        versionedKey.put(key.data());
        versionedKey.putLong(snapshotId);
        return versionedKey;
    }

    @Override
    public Memory memory() {
        return memory;
    }

    @Override
    public long sizeBytes() {
        return memory.size();
    }

    @Override
    public void releaseMemory() {
        memory.release();
    }

    @Override
    public String toString() {
        List<Record> records = new ArrayList<Record>();
        for (Record record : this){
            records.add(record);
        }

        return "DataBlock{records=" + records + "}";
    }
}
