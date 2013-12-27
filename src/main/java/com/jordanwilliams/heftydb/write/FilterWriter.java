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

package com.jordanwilliams.heftydb.write;

import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.offheap.BloomFilter;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.DataFiles;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class FilterWriter {

    private final long tableId;
    private final DataFiles dataFiles;
    private final BloomFilter.Builder filterBuilder;
    private final DataFile filterFile;

    private FilterWriter(long tableId, DataFiles dataFiles, long approxRecordCount) throws IOException {
        this.tableId = tableId;
        this.dataFiles = dataFiles;
        this.filterBuilder = new BloomFilter.Builder(approxRecordCount, 0.03);
        this.filterFile = MutableDataFile.open(dataFiles.tempFilterPath(tableId));
    }

    public void addRecord(Record record) {
        filterBuilder.put(record.key().data().array());
    }

    public void close() throws IOException {
        BloomFilter filter = filterBuilder.build();
        ByteBuffer filterBuffer = filter.memory().toDirectBuffer();
        filterFile.append(filterBuffer);
        filterFile.close();
        Files.move(dataFiles.tempFilterPath(tableId), dataFiles.filterPath(tableId), StandardCopyOption.ATOMIC_MOVE);
    }

    public static FilterWriter open(long tableId, DataFiles dataFiles, long approxRecordCount) throws IOException {
        return new FilterWriter(tableId, dataFiles, approxRecordCount);
    }
}
