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
import com.jordanwilliams.heftydb.io.ImmutableChannelFile;
import com.jordanwilliams.heftydb.io.ImmutableFile;
import com.jordanwilliams.heftydb.offheap.BloomFilter;
import com.jordanwilliams.heftydb.offheap.MemoryAllocator;
import com.jordanwilliams.heftydb.offheap.MemoryPointer;
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
    public MemoryPointer memory() {
        return bloomFilter.memory();
    }

    public static TableBloomFilter read(long tableId, Paths paths) throws IOException {
        ImmutableFile filterFile = ImmutableChannelFile.open(paths.filterPath(tableId));
        MemoryPointer filterPointer = MemoryAllocator.allocate((int) filterFile.size());
        ByteBuffer filterBuffer = filterPointer.directBuffer();
        filterFile.read(filterBuffer, 0);
        filterFile.close();
        return new TableBloomFilter(new BloomFilter(filterPointer));
    }
}
