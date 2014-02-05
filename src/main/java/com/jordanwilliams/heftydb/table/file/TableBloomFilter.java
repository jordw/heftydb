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

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.offheap.BloomFilter;
import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.offheap.Offheap;
import com.jordanwilliams.heftydb.state.Paths;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TableBloomFilter implements Offheap {

    private final BloomFilter bloomFilter;

    private TableBloomFilter(BloomFilter bloomFilter) throws IOException {
        this.bloomFilter = bloomFilter;
    }

    public boolean mightContain(Key key) {
        return bloomFilter.mightContain(key);
    }

    public void close() {
        bloomFilter.memory().release();
    }

    @Override
    public Memory memory() {
        return bloomFilter.memory();
    }

    public static TableBloomFilter read(long tableId, Paths paths) throws IOException {
        DataFile filterFile = MutableDataFile.open(paths.filterPath(tableId));
        Memory filterMemory = Memory.allocate((int) filterFile.size());
        ByteBuffer filterBuffer = filterMemory.directBuffer();
        filterFile.read(filterBuffer, 0);
        filterFile.close();
        return new TableBloomFilter(new BloomFilter(filterMemory));
    }
}
