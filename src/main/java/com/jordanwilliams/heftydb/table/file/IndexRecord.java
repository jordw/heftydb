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

package com.jordanwilliams.heftydb.table.file;

import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.util.Sizes;

public class IndexRecord {

    private final Key startKey;
    private final long blockOffset;
    private final int blockSize;
    private final boolean isLeaf;

    public IndexRecord(Key startKey, long blockOffset, int blockSize, boolean isLeaf) {
        this.startKey = startKey;
        this.blockOffset = blockOffset;
        this.blockSize = blockSize;
        this.isLeaf = isLeaf;
    }

    public IndexRecord(Key startKey, long blockOffset, int blockSize) {
        this(startKey, blockOffset, blockSize, true);
    }

    public Key startKey() {
        return startKey;
    }

    public long blockOffset() {
        return blockOffset;
    }

    public int blockSize() {
        return blockSize;
    }

    public int size() {
        return Sizes.INT_SIZE + //Key blockSize
               startKey.size() + //Key
               Sizes.LONG_SIZE + //Offset
               Sizes.INT_SIZE +
               1; //Leaf flag
    }

    public int contentsSize() {
        return Sizes.LONG_SIZE + //Offset
               Sizes.INT_SIZE +
               1; //Leaf flag
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    @Override
    public String toString() {
        return "IndexRecord{" +
                "startKey=" + startKey +
                ", blockOffset=" + blockOffset +
                ", blockSize=" + blockSize +
                ", isLeaf=" + isLeaf +
                '}';
    }
}
