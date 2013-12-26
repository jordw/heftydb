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

public class IndexBlock {

    private final Memory memory;
    private final int indexRecordCount;

    public IndexBlock(Memory memory) {
        this.memory = memory;
        this.indexRecordCount = memory.getInt(0);
    }

    public List<Long> blockOffsets(Key key) {
        int startRecordIndex = startRecordIndex(key);
        return blockOffsets(key, startRecordIndex);
    }

    private int startRecordIndex(Key key){
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
        if (!found){
            return low - 1;
        }

        int searchIndex = startIndex;
        while (searchIndex > 0){
            searchIndex--;

            if (compareKeys(key, searchIndex) != 0){
                break;
            }
        }

        return searchIndex == 0 ? searchIndex : searchIndex + 1;
    }

    private List<Long> blockOffsets(Key key, int startingKeyIndex) {
        List<Long> blockOffsets = new ArrayList<Long>();
        int keyIndex = startingKeyIndex;

        while (keyIndex < indexRecordCount) {
            int startKeyCompare = compareKeys(key, keyIndex);

            if (startKeyCompare <= 0) {
                if (keyIndex + 1 == indexRecordCount){
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

    private int compareKeys(Key key, int compareKeyIndex) {
        int recordOffset = recordOffset(compareKeyIndex);
        int keySize = memory.getInt(recordOffset);
        int keyOffset = recordOffset + Sizes.INT_SIZE;
        key.key().rewind();
        return memory.compareAsBytes(key.key(), keyOffset, keySize);
    }

    private int recordOffset(int pointerIndex){
        return memory.getInt(pointerOffset(pointerIndex));
    }

    private long blockOffset(int pointerIndex){
        int recordOffset = recordOffset(pointerIndex);
        int keySize = memory.getInt(recordOffset);
        int blockOffset = recordOffset + Sizes.INT_SIZE + keySize;
        return memory.getLong(blockOffset);
    }

    private static int pointerOffset(int pointerIndex) {
        int pointerOffset = Sizes.INT_SIZE;
        pointerOffset += pointerIndex * Sizes.INT_SIZE;
        return pointerOffset;
    }
}
