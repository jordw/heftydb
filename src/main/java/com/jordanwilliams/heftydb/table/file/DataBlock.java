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

import java.util.Iterator;

public class DataBlock implements Iterable<Record>, Offheap {

    private final Memory memory;
    private final int recordCount;

    public DataBlock(Memory memory) {
        this.memory = memory;
        this.recordCount = memory.getInt(0);
    }

    public Record get(Key key, long maxSnapshotId) {
        int record = recordIndex(key);
        return null;
    }

    @Override
    public Iterator<Record> iterator() {
        return null;
    }

    private int recordIndex(Key key) {
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

    private int compareKeys(Key key, int compareKeyIndex) {
        int recordOffset = recordOffset(compareKeyIndex);
        int keySize = memory.getInt(recordOffset);
        int keyOffset = recordOffset + Sizes.INT_SIZE;
        key.key().rewind();
        return memory.compareAsBytes(key.key(), keyOffset, keySize);
    }

    private int recordOffset(int pointerIndex) {
        return memory.getInt(pointerOffset(pointerIndex));
    }

    private static int pointerOffset(int pointerIndex) {
        int pointerOffset = Sizes.INT_SIZE;
        pointerOffset += pointerIndex * Sizes.INT_SIZE;
        return pointerOffset;
    }

    @Override
    public Memory memory() {
        return memory;
    }

    @Override
    public long sizeBytes() {
        return memory.size();
    }
}
