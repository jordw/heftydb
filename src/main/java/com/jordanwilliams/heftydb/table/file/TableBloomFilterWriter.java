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

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.offheap.BloomFilter;
import com.jordanwilliams.heftydb.state.Paths;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TableBloomFilterWriter {

    private static final double FALSE_POSITIVE_PROBABILITY = 0.01;

    private final BloomFilter.Builder filterBuilder;
    private final DataFile filterFile;

    private TableBloomFilterWriter(DataFile filterFile, long approxRecordCount) {
        this.filterBuilder = new BloomFilter.Builder(approxRecordCount, FALSE_POSITIVE_PROBABILITY);
        this.filterFile = filterFile;
    }

    public void write(Key key) throws IOException {
        filterBuilder.put(key);
    }

    public void finish() throws IOException {
        BloomFilter filter = filterBuilder.build();
        ByteBuffer filterBuffer = filter.memory().directBuffer();
        filterFile.append(filterBuffer);
        filterFile.close();
        filter.memory().release();
    }

    public static TableBloomFilterWriter open(long tableId, Paths paths, long approxRecordCount) throws IOException {
        DataFile filterFile = MutableDataFile.open(paths.filterPath(tableId));
        return new TableBloomFilterWriter(filterFile, approxRecordCount);
    }
}
