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
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.util.Sizes;

import java.util.ArrayList;
import java.util.List;

public class IndexBlockBuilder {

    private static class Record {
        private final Key startKey;
        private final long offset;

        private Record(Key startKey, long offset) {
            this.startKey = startKey;
            this.offset = offset;
        }

        public Key startKey() {
            return startKey;
        }

        public long offset() {
            return offset;
        }

        @Override
        public String toString() {
            return "Record{" +
                    "startKey=" + startKey +
                    ", offset=" + offset +
                    '}';
        }
    }

    private final List<Record> indexRecords = new ArrayList<Record>();

    public void addRecord(Key startKey, long offset) {
        indexRecords.add(new Record(startKey, offset));
    }

    public IndexBlock build() {
        Memory contents = serializeRecords(indexRecords);
        return new IndexBlock(contents);
    }

    private static Memory serializeRecords(List<Record> indexRecords) {
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
            memory.putBytes(memoryOffset, startKey.key());
            memoryOffset += startKey.size();

            //Offset
            memory.putLong(memoryOffset, indexRecord.offset());
            memoryOffset += Sizes.LONG_SIZE;
        }

        return memory;
    }
}