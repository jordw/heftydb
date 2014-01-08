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
import com.jordanwilliams.heftydb.util.Sizes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class RecordBlock implements Iterable<Record>, Offheap {

    public static class Builder {

        private int sizeBytes;

        public void addRecord(Record record) {
            sizeBytes += record.size();
        }

        public int sizeBytes() {
            return sizeBytes;
        }

        public RecordBlock build() {
            return new RecordBlock(null);
        }
    }

    private final Memory memory;

    public RecordBlock(Memory memory) {
        this.memory = memory;
    }

    public Record get(Key key, long maxSnapshotId) {
        return null;
    }

    public int closestRecordIndex(Key key, long maxSnapshotId){
        return 0;
    }

    public int recordCount(){
        return 0;
    }

    public Record startRecord() {
        return deserializeRecord(0);
    }

    @Override
    public Iterator<Record> iterator() {

        return new Iterator<Record>() {

            int currentRecordIndex = 0;

            @Override
            public boolean hasNext() {
                return currentRecordIndex < 0;
            }

            @Override
            public Record next() {
                if (currentRecordIndex >= 0) {
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
    public Memory memory() {
        return memory;
    }

    @Override
    public String toString() {
        List<Record> records = new ArrayList<Record>();
        for (Record record : this) {
            records.add(record);
        }

        return "RecordBlock{records=" + records + "}";
    }

    private Record deserializeRecord(int recordIndex) {
        return null;
    }

    private static int pointerOffset(int pointerIndex) {
        int pointerOffset = Sizes.INT_SIZE;
        pointerOffset += pointerIndex * Sizes.INT_SIZE;
        return pointerOffset;
    }
}
