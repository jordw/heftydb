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
    private final long offset;
    private final boolean isLeaf;

    public IndexRecord(Key startKey, long offset, boolean isLeaf) {
        this.startKey = startKey;
        this.offset = offset;
        this.isLeaf = isLeaf;
    }

    public IndexRecord(Key startKey, long offset) {
        this(startKey, offset, true);
    }

    public Key startKey() {
        return startKey;
    }

    public long offset() {
        return offset;
    }

    public int size() {
        return Sizes.INT_SIZE + //Key size
                startKey.size() + //Key
                Sizes.LONG_SIZE + //Offset
                1; //Leaf flag
    }

    public int contentsSizeBytes() {
        return Sizes.LONG_SIZE + //Offset
                1; //Leaf flag
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    @Override
    public String toString() {
        return "IndexRecord{" +
                "startKey=" + startKey +
                ", offset=" + offset +
                ", isLeaf=" + isLeaf +
                '}';
    }
}
