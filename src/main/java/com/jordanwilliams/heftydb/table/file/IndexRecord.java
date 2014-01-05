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

    public IndexRecord(Key startKey, long offset) {
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
