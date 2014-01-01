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
import com.jordanwilliams.heftydb.util.Sizes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class IndexBlock implements Iterable<IndexBlock.Record>, Offheap {

    public static class Record {

        private final Key startKey;
        private final long offset;

        public Record(Key startKey, long offset) {
            this.startKey = startKey;
            this.offset = offset;
        }

        public Key startKey() {
            return startKey;
        }

        public long offset() {
            return offset;
        }

        public int sizeBytes() {
            return startKey.size() + Sizes.LONG_SIZE;
        }

        @Override
        public String toString() {
            return "Record{" +
                    "startKey=" + startKey +
                    ", offset=" + offset +
                    '}';
        }
    }

    public static class Builder {

        private final List<Record> indexRecords = new ArrayList<Record>();
        private final boolean isLeaf;

        private int sizeBytes;

        public Builder(boolean isLeaf) {
            this.isLeaf = isLeaf;
        }

        public void addRecord(Record indexRecord) {
            indexRecords.add(indexRecord);
            sizeBytes += indexRecord.sizeBytes();
        }

        public int sizeBytes() {
            return sizeBytes;
        }

        public IndexBlock build() {
            Memory contents = serializeRecords(indexRecords, isLeaf);
            return new IndexBlock(contents);
        }

        private static Memory serializeRecords(List<Record> indexRecords, boolean isLeaf) {
            //Allocate memory
            int memorySize = 0;
            int[] indexRecordOffsets = new int[indexRecords.size()];

            memorySize += Sizes.INT_SIZE; //Pointer count
            memorySize += Sizes.INT_SIZE * indexRecords.size(); //Pointers

            //Compute memory size
            for (int i = 0; i < indexRecords.size(); i++) {
                Record indexRecord = indexRecords.get(i);
                indexRecordOffsets[i] = memorySize;
                memorySize += Sizes.INT_SIZE; //Key size
                memorySize += indexRecord.startKey().size(); //Key
                memorySize += Sizes.LONG_SIZE; //Offset
            }

            memorySize += 1; //isLeaf flag

            Memory memory = Memory.allocate(memorySize);

            //Serialize the index records
            int memoryOffset = 0;

            //Pack pointers
            memory.putInt(memoryOffset, indexRecords.size());
            memoryOffset += Sizes.INT_SIZE;

            for (int i = 0; i < indexRecordOffsets.length; i++) {
                memory.putInt(memoryOffset, indexRecordOffsets[i]);
                memoryOffset += Sizes.INT_SIZE;
            }

            //Pack indexRecords
            for (Record indexRecord : indexRecords) {
                Key startKey = indexRecord.startKey();

                //Key size
                memory.putInt(memoryOffset, startKey.size());
                memoryOffset += Sizes.INT_SIZE;

                //Key
                memory.putBytes(memoryOffset, startKey.data());
                memoryOffset += startKey.size();

                //Offset
                memory.putLong(memoryOffset, indexRecord.offset());
                memoryOffset += Sizes.LONG_SIZE;
            }

            //Pack isLeaf flag
            memory.putByte(memoryOffset, isLeaf ? (byte) 1 : (byte) 0);

            return memory;
        }
    }

    private final Memory memory;
    private final int indexRecordCount;
    private final boolean isLeaf;

    public IndexBlock(Memory memory) {
        this.memory = memory;
        this.indexRecordCount = memory.getInt(0);
        this.isLeaf = memory.getByte(memory.size() - 1) == 1;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public Key startKey() {
        return deserializeRecord(0).startKey();
    }

    public List<Long> blockOffsets(Key key) {
        int startRecordIndex = startRecordIndex(key.data());
        return blockOffsets(key.data(), startRecordIndex);
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
    public Iterator<Record> iterator() {
        return new Iterator<Record>() {

            int currentRecordIndex = 0;

            @Override
            public boolean hasNext() {
                return currentRecordIndex < indexRecordCount;
            }

            @Override
            public Record next() {
                if (currentRecordIndex >= indexRecordCount) {
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

    @Override
    public String toString() {
        List<Record> records = new ArrayList<Record>();
        for (Record record : this) {
            records.add(record);
        }

        return "IndexBlock{records=" + records + "}";
    }

    private int startRecordIndex(ByteBuffer key) {
        int low = 0;
        int high = indexRecordCount - 1;
        int startIndex = -1;
        boolean found = false;

        //Binary search
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int compare = compareKeys(key, mid);

            if (compare < 0) {
                low = mid + 1;
            } else if (compare > 0) {
                high = mid - 1;
            } else {
                startIndex = mid;
                found = true;
                break;
            }
        }

        //Look for the lowest index with the same start key
        if (!found) {
            return low - 1;
        }

        int searchIndex = startIndex;
        while (searchIndex-- > 0) {
            if (compareKeys(key, searchIndex) != 0) {
                break;
            }
        }

        return searchIndex == 0 ? searchIndex : searchIndex + 1;
    }

    private List<Long> blockOffsets(ByteBuffer key, int startingKeyIndex) {
        List<Long> blockOffsets = new ArrayList<Long>();
        int keyIndex = startingKeyIndex;

        while (keyIndex < indexRecordCount) {
            int startKeyCompare = compareKeys(key, keyIndex);

            if (startKeyCompare <= 0) {
                if (keyIndex + 1 == indexRecordCount) {
                    blockOffsets.add(blockOffset(keyIndex));
                    break;
                }

                int endKeyCompare = compareKeys(key, keyIndex + 1);

                if (endKeyCompare > 0) {
                    blockOffsets.add(blockOffset(keyIndex));
                    break;
                }

                if (endKeyCompare <= 0) {
                    blockOffsets.add(blockOffset(keyIndex));
                }
            }

            keyIndex++;
        }

        return blockOffsets;
    }

    private int compareKeys(ByteBuffer key, int compareKeyIndex) {
        int recordOffset = recordOffset(compareKeyIndex);
        int keySize = memory.getInt(recordOffset);
        int keyOffset = recordOffset + Sizes.INT_SIZE;
        key.rewind();
        return memory.compareAsBytes(key, keyOffset, keySize);
    }

    private int recordOffset(int recordIndex) {
        return memory.getInt(pointerOffset(recordIndex));
    }

    private long blockOffset(int recordIndex) {
        int recordOffset = recordOffset(recordIndex);
        int keySize = memory.getInt(recordOffset);
        int blockOffset = recordOffset + Sizes.INT_SIZE + keySize;
        return memory.getLong(blockOffset);
    }

    private Record deserializeRecord(int recordIndex) {
        int recordOffset = recordOffset(recordIndex);

        //Start Key
        int keySize = memory.getInt(recordOffset);
        ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
        memory.getBytes(recordOffset + Sizes.INT_SIZE, keyBuffer);
        keyBuffer.rewind();

        //Offset
        long offset = memory.getLong(recordOffset + Sizes.INT_SIZE + keySize);

        return new Record(new Key(keyBuffer), offset);
    }

    private static int pointerOffset(int pointerIndex) {
        int pointerOffset = Sizes.INT_SIZE;
        pointerOffset += pointerIndex * Sizes.INT_SIZE;
        return pointerOffset;
    }
}
